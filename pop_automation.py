# StarCasualty Pop Automation Program.
# from tarfile import data_filter
from cmath import exp
from typing import Dict, Any, List, Optional, Tuple

from attr import dataclass
import bot_config
from local_db import PopLocalDatabase, get_pop_db
from pop_sql import SQL_FIND_POP_BASIC, SQL_FIND_POP_LAST100DAYS, get_sql_dump_match_table, get_sql_find_popfields_testdb, SQL_FIND_POP_LAST_ONEDAY, get_sql_insert_into_match_table
from bot_logger import get_logger, get_console
import shutil, os
from gemini_with_pdf import define_json_schema, call_gemini_api_with_pdf, validate_json_output
from bot_config import get_config
from star_util import CONFIG_FILE, compare_dates, compare_strings, copy_file_into_localdir

from ms_sql_server_connector import connect_and_run_query


logger = get_logger()

@dataclass
class PopResult:
    """
    Result of processing a POP file with Gemini.
    """
    all_fields_present: bool
    json_output: dict
    policy_id: str
    named_insured: str
    effective_date: str
    expiration_date: str
    agent_code: int
    prior_carrier: str

@dataclass
class FindPopFieldsResult:
    """
    Result of Sql query to find the POP fields for a later match determination.
    """
    policy_id: str
    named_insured: str
    effective_date: str
    expiration_date: str
    agent_code: int
    prior_carrier: str

    def __str__(self):
        return f"""FindPopFieldsResult(policy_id={self.policy_id}, named_insured={self.named_insured},
         effective_date={self.effective_date}, expiration_date={self.expiration_date},
          agent_code={self.agent_code}, prior_carrier={self.prior_carrier})"""

@dataclass
class MatchField:
    """
    A field that needs to be matched.
    """
    field_name: str
    pop_document_value: str
    sqldb_value: str

@dataclass
class MatchResult:
    """
    Result of matching a POP file with a POP fields result.
    """
    policy_id: str
    all_fields_match: bool
    fields_that_dont_match: List[MatchField]  # (field_name, expected_value, actual_value)

    def to_xml(self) -> str:
        """Convert MatchResult to XML format."""
        xml_lines = []
        xml_lines.append("<MatchResult>")
        xml_lines.append(f"    <policy_id>{self.policy_id}</policy_id>")
        xml_lines.append(f"    <all_fields_match>{str(self.all_fields_match).lower()}</all_fields_match>")
        xml_lines.append("    <fields_that_dont_match>")
        
        for field in self.fields_that_dont_match:
            xml_lines.append("        <field>")
            xml_lines.append(f"            <field_name>{field.field_name}</field_name>")
            xml_lines.append(f"            <pop_document_value>{field.pop_document_value}</pop_document_value>")
            xml_lines.append(f"            <sqldb_value>{field.sqldb_value}</sqldb_value>")
            xml_lines.append("        </field>")
        
        xml_lines.append("    </fields_that_dont_match>")
        xml_lines.append("</MatchResult>")
        
        return "\n".join(xml_lines)


def should_process_file_check_local_db(file_id: str) -> bool:
    """
    Returns the current status of the file_id from local db, this is needed to see if we have
    already processed the file or begun processing the file. 
    """
    db = get_pop_db()
    record = db.get_record_by_file_id(file_id=file_id)
    if record is not None:
        print(record)
        return record[4] == "NOT_PROCESSED"  # Index 4 contains the status based on the DB schema
    return True

def update_local_db(file_id: str, date_created: str, filepath: str, status: str, match_result: str) -> bool: 
    db = get_pop_db()
    record = db.get_record_by_file_id(file_id=file_id)
    if record is not None:
        if not db.update_status(record[0], status): # Index 0 contains the processing id
            get_logger().error(f"\n Attempting to update status to local db failed: {record}")
            return False
        else:
            return True
    p_id = db.insert_record(file_id=file_id, original_date=date_created, filepath=filepath, status=status, match_result=match_result)
    get_logger().info(f"\n Inserted record into local db with processing id {p_id}")
    get_logger().info(f"\n Record details: file_id={file_id}, date_created={date_created}, filepath={filepath}, status={status}, match_result={match_result}")
    return True


def extract_pop_info(json_result:Dict[str, Any]) -> PopResult:
    """
    Extract information from a POP json result off a POP document (from gemini call).
    """
    # Initialize variables
    all_fields_present = True
    agent_code = None
    
    # Extract agent info if present
    if "insurance_agent_info" in json_result:
        agent_info = json_result["insurance_agent_info"]
        if "agent_number" in agent_info:
            try:
                agent_code = int(agent_info["agent_number"])
            except (ValueError, TypeError):
                agent_code = None
                all_fields_present = False
        else:
            all_fields_present = False
    else:
        all_fields_present = False

    # Extract policy info
    policy_info = json_result.get("policy_summary", {})
    if not policy_info:
        all_fields_present = False
    else:
        if "policy_number" in policy_info:
            policy_id = policy_info["policy_number"]
        else:
            all_fields_present = False
        if "policy_period" in policy_info:
            policy_period = policy_info["policy_period"]
            if "start_date" in policy_period:
                effective_date = policy_period["start_date"]
            else:
                all_fields_present = False
            if "end_date" in policy_period:
                expiration_date = policy_period["end_date"]
            else:
                all_fields_present = False
        else:
            all_fields_present = False
        if "underwritten_by" in policy_info:
            prior_carrier = policy_info["underwritten_by"]
        else:
            all_fields_present = False
  

    # Extract named insured info
    named_insured = json_result.get("named_insured", {}).get("name")
    if not named_insured:
        all_fields_present = False
    else:
        named_insured = named_insured.strip()

    
    # Create PopResult with extracted fields
    result = PopResult(
        all_fields_present=all_fields_present,
        json_output=json_result,
        policy_id=policy_id,
        named_insured=named_insured,
        effective_date=effective_date,
        expiration_date=expiration_date, 
        agent_code=agent_code,
        prior_carrier=None # Not provided in schema
    )

    get_logger().info(f"\n Extracted POP info: {result}")
    
    return result

def find_popfields_sqldb_query(policy_id: str):
    find_fields_query = get_sql_find_popfields_testdb(policyid=policy_id)
    get_logger().info(f"Find Pop Fields query: {find_fields_query}")
    rows = connect_and_run_query(sql_query=find_fields_query, config_file=CONFIG_FILE)
    pop_fields_results = []
    if rows is not None:
        get_logger().info(f"Find Pop Fields query returned {len(rows)} rows")
        for row in rows:
            get_logger().info(f"Row: {row}")
            match_result = FindPopFieldsResult(policy_id=row[0],
                 named_insured=row[1], effective_date=row[2],
                 expiration_date=row[3], agent_code=row[4],
                 prior_carrier=row[9])
            get_logger().info(f"Find Pop Fields result: {match_result}")
            pop_fields_results.append(match_result)
    else:
        get_logger().error(f"Find Pop Fields query returned no rows for policy_id {policy_id}")
        return None
    return pop_fields_results


# TODO: Fix this function. It is not working. Convert to an XML for the Remarks field. 
def insert_match_result_into_mssqldb(file_id:str, named_insured: str, expiration_date: str, agent_code: int, company_name: str, match_result: MatchResult):
    named_insured_match = True
    expiration_date_match = True
    agent_code_match = True
    company_name_match = True
    for field in match_result.fields_that_dont_match:
        if field.field_name == "named_insured":
            named_insured_match = False
        elif field.field_name == "expiration_date":
            expiration_date_match = False
        elif field.field_name == "agent_code":
            agent_code_match = False
        elif field.field_name == "company_name":
            company_name_match = False
    sql_query = get_sql_insert_into_match_table(policyid=match_result.policy_id,
        fileid=file_id, namedinsured=named_insured, expirationdate=expiration_date, agentcode=agent_code,
        companyname=company_name, namedinsuredmatch=named_insured_match,
        expirationdatematch=expiration_date_match, agentcodematch=agent_code_match,
        companynamematch=company_name_match, remarks=match_result.to_xml())
    get_logger().info(f"Insert Match Result query: {sql_query}")
    rows = connect_and_run_query(sql_query=sql_query, config_file=CONFIG_FILE)
    if rows is not None:
        get_logger().info(f"Insert Match Result query returned {len(rows)} rows")
    else:
        get_logger().error(f"Insert Match Result query returned no rows for policy_id {match_result.policy_id}")
    return rows


def dump_match_table():
    sql_query = get_sql_dump_match_table()
    rows = connect_and_run_query(sql_query=sql_query, config_file=CONFIG_FILE)
    if rows is not None:
        get_logger().info(f"Dump Match Table query returned {len(rows)} rows")
        for row in rows:
            get_logger().info(f"Row: {row}")
    else:
        get_logger().error(f"Dump Match Table query returned no rows")
    return rows


def compute_match(pop_document_result: PopResult, pop_sqldb_result: FindPopFieldsResult):
    all_fields_match = True
    fields_that_dont_match = []
    if pop_document_result.named_insured.lower() != pop_sqldb_result.named_insured.lower():
        all_fields_match = False
        fields_that_dont_match.append(MatchField(field_name="named_insured", pop_document_value=pop_document_result.named_insured, sqldb_value=pop_sqldb_result.named_insured))
    if not compare_dates(pop_document_result.effective_date, pop_sqldb_result.effective_date):
        all_fields_match = False
        fields_that_dont_match.append(MatchField(field_name="effective_date", pop_document_value=pop_document_result.effective_date, sqldb_value=pop_sqldb_result.effective_date))
    if not compare_dates(pop_document_result.expiration_date, pop_sqldb_result.expiration_date):
        all_fields_match = False
        fields_that_dont_match.append(MatchField(field_name="expiration_date", pop_document_value=pop_document_result.expiration_date, sqldb_value=pop_sqldb_result.expiration_date))
    if pop_document_result.agent_code != pop_sqldb_result.agent_code:
        all_fields_match = False
        fields_that_dont_match.append(MatchField(field_name="agent_code", pop_document_value=pop_document_result.agent_code, sqldb_value=pop_sqldb_result.agent_code))
    if not compare_strings(pop_document_result.prior_carrier, pop_sqldb_result.prior_carrier):
        all_fields_match = False
        fields_that_dont_match.append(MatchField(field_name="prior_carrier", pop_document_value=pop_document_result.prior_carrier, sqldb_value=pop_sqldb_result.prior_carrier))

    return MatchResult(policy_id=pop_sqldb_result.policy_id, all_fields_match=all_fields_match, fields_that_dont_match=fields_that_dont_match)


def process_document_with_gemini(filepath: str):
    """
    Process a POP file with Gemini.
    
    """
    schema = define_json_schema()

    # Call the Gemini API with the temporary PDF file
    with get_console().status("[bold green]Processing Proof of prior (POP) document ...", spinner="dots"):
        parsed_json = call_gemini_api_with_pdf(filepath, schema)

    if parsed_json is not None:
        if validate_json_output(parsed_json, schema):
            get_logger().info(f"\n Gemini API returned valid JSON for {filepath}")
        else:
            get_logger().error(f"\n Gemini API returned invalid JSON for {filepath}")
        pop_result = extract_pop_info(parsed_json)
        if pop_result.all_fields_present:
            get_logger().info(f"\n Extracted POP info: {pop_result}")
            return pop_result
        else:
            get_logger().error(f"\n Failed to extract all fields, partial result {pop_result}")
            return pop_result
    else:
        # TODO: Mark DB with processing error.
        get_logger().error(f"\n Gemini API returned no JSON for {filepath}")
        
        # TODO: Mark DB with processing error.
        return None



def process_incoming_pop_transaction(filepath: str, date_created: str, file_id: str, policy_id: str) -> bool:
    logger.info(f"\n Checking Incoming Pop request:  {filepath}, {date_created}, {file_id}, {policy_id}\n ")

    if should_process_file_check_local_db(file_id=file_id):
        local_subdir = get_config().get(bot_config.BotConfig.LOCAL_POP_FILEDIR_KEY, bot_config.BotConfig.LOCAL_POP_FILEDIR_DEFAULT)
        if copy_file_into_localdir(filepath=filepath, local_subdir=local_subdir) is None:
            get_logger().error("\n File copy failed. Marking DB with error.")
            # TODO: Mark DB with error. Process error ?
            update_local_db(file_id=file_id, date_created=date_created, filepath=filepath,
                status=PopLocalDatabase.STATUS_FAILED, match_result=PopLocalDatabase.MATCH_RESULT_NOT_MATCHED)
            return True
        else:
            document_result = process_document_with_gemini(filepath=filepath)
            get_logger().info(f"Document result: {document_result}")
            sqldb_results = find_popfields_sqldb_query(policy_id=policy_id)
            if sqldb_results is not None:
                get_logger().info(f"Sqldb query results: {sqldb_results}")
            else:
                get_logger().error(f"No Sqldb query results found for policy_id {policy_id}")
            sqldb_result = sqldb_results[0]
            match_result = compute_match(pop_document_result=document_result, pop_sqldb_result=sqldb_result)
            get_logger().info(f"Final Match result: {match_result}")
            # TODO: Store the match result in the MSSql DB.
            # TODO: Change local schema to include match result. TODO TODO TODO
            update_local_db(file_id=file_id, date_created=date_created, filepath=filepath, status=PopLocalDatabase.STATUS_PROCESSED,
                match_result=f"MATCHED: {match_result}")
            return True
    else:
        get_logger().info(f"\n Skipping fileid {file_id} since already processed.")
        return False
    

def run_pop_automation_loop():
    rows = connect_and_run_query(sql_query=SQL_FIND_POP_LAST100DAYS, config_file=CONFIG_FILE)

    #  Process results
    if rows is not None:
        if rows:
            print("\n--- Query Results for check_new_pop_entries() ---")
            for row in rows:
                get_logger().console_print(f"FilePath: {row[0]}, Date Created: {row[1]}, FileID: {row[2]}, PolicyID: {row[3]}\n")
                did_process = process_incoming_pop_transaction(filepath=row[0], date_created=row[1], file_id=row[2], policy_id=row[3])
                if did_process:
                    get_logger().info(f"Processed file {row[0]}")
                    print("We process only the first one.. exiting")
                    break
                else:
                    get_logger().info(f"Skipped file {row[0]}, will attemp the next one.")

            print("--------------------")
        else:
            logger.info("\nNo results found for the given query.")
            
    else:
        print("\nQuery execution failed. Check the error messages above.")

if __name__ == "__main__":
    run_pop_automation_loop()
