# StarCasualty Pop Automation Program.
# from tarfile import data_filter
from typing import Dict, Any, Optional

from attr import dataclass
import bot_config
from local_db import PopLocalDatabase, get_pop_db
import pyodbc
import sys # Used for exiting the script gracefully
from pop_sql import SQL_FIND_POP_BASIC, SQL_FIND_POP_LAST100DAYS
from bot_logger import get_logger, get_console
import shutil, os
from gemini_with_pdf import define_json_schema, call_gemini_api_with_pdf, validate_json_output

# --- File to hold configuration ---
# This script now reads connection details from a separate file.
# Create a file named "env.txt" in the same directory as this script.
#
# The file should have the following format.
# Use AUTHENTICATION=SQL for standard SQL login.
# Use AUTHENTICATION=WINDOWS for Windows integrated security.
#
# --- Example for SQL Authentication ---
# AUTHENTICATION=SQL
# SERVER=192.168.2.10
# DATABASE=isdata15testsql
# USERNAME=your_sql_username
# PASSWORD=your_sql_password
#
# --- Example for Windows Authentication ---
# AUTHENTICATION=WINDOWS
# SERVER=192.168.2.10
# DATABASE=isdata15testsql
#
CONFIG_FILE = 'env.txt'

logger = get_logger()

def find_sql_server_driver():
    """
    Automatically detects and returns the name of an installed SQL Server ODBC driver.
    """
    # Prioritized list of common driver names
    preferred_drivers = [
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "ODBC Driver 11 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server"
    ]
    
    installed_drivers = pyodbc.drivers()
    
    for driver in preferred_drivers:
        if driver in installed_drivers:
            print(f"Driver found: {driver}")
            return f"{{{driver}}}" # Return in the format pyodbc expects

    # If no preferred driver is found, print an error and exit
    print("\n--- DRIVER ERROR ---")
    print("Could not find a suitable SQL Server ODBC driver.")
    print("Please install the Microsoft ODBC Driver for SQL Server.")
    logger.error("Driver error: Counld not find a suitable SQL Server ODBC driver.")
    print("Download from: https://docs.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server")
    if installed_drivers:
        print("\nAvailable drivers on this system are:")
        for d in installed_drivers:
            print(f"- {d}")
    else:
        print("\nNo ODBC drivers were found on this system.")
    print("--------------------")
    return None

def read_config(filename):
    """
    Reads database configuration from a simple key-value file.
    """
    config = {}
    try:
        with open(filename, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    config[key.strip().upper()] = value.strip()
        return config
    except FileNotFoundError:
        print(f"Error: Configuration file '{filename}' not found.")
        logger.error(f"Error: Configuration file '{filename}' not found.")
        return None
    except Exception as e:
        print(f"Error reading configuration file: {e}")
        logger.error(f"Error reading configuration file: {e}")
        return None

def execute_sql_query(config, query, driver):
    """
    Connects to a SQL Server database, executes a query, and returns the results.
    """
    connection = None
    try:
        server = config['SERVER']
        database = config['DATABASE']
        auth_method = config.get('AUTHENTICATION', 'SQL').upper()

        if auth_method == 'WINDOWS':
            connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
            print(f"Connecting to {server} using Windows Authentication...")
        else:
            username = config['USERNAME']
            password = config['PASSWORD']
            connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};'
            print(f"Connecting to {server} using SQL Server Authentication...")
            

        print(f"Connection string: {connection_string}")
        connection = pyodbc.connect(connection_string, timeout=10)
        print("Connection successful.")

        cursor = connection.cursor()
        print("Executing query...")
        cursor.execute(query)
        print("Query executed.")

        return cursor.fetchall()

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"\n--- DATABASE ERROR ---")
        print(f"An error occurred while connecting or querying the database.")
        logger.error(f"DATABASE ERROR: An error occurred while connecting or querying the database.")
        print(f"SQLSTATE: {sqlstate}")
        print(f"Message: {ex}")
        print("----------------------")
        return None
    except KeyError as e:
        print(f"\n--- CONFIGURATION ERROR ---")
        print(f"Your '{CONFIG_FILE}' is missing a required setting: {e}")
        logger.error("CONFIG ERROR: Your '{CONFIG_FILE}' is missing a required setting: {e}")
        print("---------------------------")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        logger.error(f"Execute Sql Query: An unexpected error occurred: {e}")
        return None

    finally:
        if connection:
            connection.close()
            print("\nDatabase connection closed.")


def connect_and_run_query(sql_query: str, config_file: str):
     # Step 1: Automatically find the ODBC driver
    driver = find_sql_server_driver()
    if not driver:
        sys.exit(1) # Exit if no driver was found

    # Step 2: Read configuration from the env.txt file
    config = read_config(config_file)
    if not config:
        sys.exit(1)

    # Step 4: Execute the query
    rows = execute_sql_query(config, sql_query, driver)
    return rows



# def main():
#     """
#     Main routine to find driver, read config, define query, and process results.
#     """
#     rows = connect_and_run_query(sql_query=SQL_FIND_POP_BASIC, config_file=CONFIG_FILE)

#     # Process results
#     if rows is not None:
#         if rows:
#             print("\n--- Query Results ---")
#             for row in rows:
#                 print(f"FilePath: {row[0]}")
#             print("--------------------")
#         else:
#             print("\nNo results found for the given query.")
#     else:
#         print("\nQuery execution failed. Check the error messages above.")


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

def update_local_db(file_id: str, date_created: str, filepath: str, status: str) -> bool: 
    db = get_pop_db()
    record = db.get_record_by_file_id(file_id=file_id)
    if record is not None:
        if not db.update_status(record[0], status): # Index 0 contains the processing id
            get_logger().error(f"\n Attempting to update status to local db failed: {record}")
            return False
        else:
            return True
    p_id = db.insert_record(file_id=file_id, original_date=date_created, filepath=filepath, status=status)
    get_logger().info(f"\n Inserted record into local db with processing id {p_id}")
    return True


def copy_file_into_localdir(filepath, local_subdir):
    """
    Copy a file from source filepath to a local subdirectory.
    
    Args:
        filepath: Full source file path
        local_subdir: Local subdirectory to copy file to, relative to current dir
        
    Returns:
        str: Path to the copied local file, or None if copy failed
    """
    try:
        # Create the local subdirectory if it doesn't exist
        os.makedirs(local_subdir, exist_ok=True)
        
        # Extract just the filename from the full path
        filename = os.path.basename(filepath)
        
        # Construct destination path
        dest_path = os.path.join(local_subdir, filename)
        
        # Copy the file, overwriting if it exists
        if os.path.exists(dest_path):
            os.remove(dest_path)
        shutil.copy2(filepath, dest_path)
        
        get_logger().info(f"Copied {filepath} to {dest_path}")
        return dest_path
        
    except Exception as e:
        get_logger().error(f"Failed to copy file {filepath}: {str(e)}")
        return None

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
    

def extract_pop_info(json_result:Dict[str, Any]) -> PopResult:
    """
    Extract information from a POP file.
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


def process_pop_with_gemini(filepath: str):
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

def process_incoming_pop(filepath: str, date_created:str, file_id: str):
    logger.info(f"\n Checking Incoming Pop request:  {filepath}, {date_created}, {file_id}")

    if should_process_file_check_local_db(file_id=file_id):
        local_subdir = bot_config().get(bot_config.BotConfig.LOCAL_POP_FILEDIR_KEY, bot_config.BotConfig.LOCAL_POP_FILEDIR_DEFAULT)
        if copy_file_into_localdir(filepath=filepath, local_subdir=local_subdir) is None:
            get_logger().error("\n File copy failed.")
            # TODO: Mark DB with error. Process error ?
            update_local_db(file_id=file_id, date_created=date_created, filepath=filepath, status=PopLocalDatabase.STATUS_FAILED)
            return 
        else:
            pop_result = process_pop_with_gemini(filepath=filepath)
    else:
        get_logger().info(f"\n Skipping fileid {file_id} since already processed.")
    pass

def run_pop_automation_loop():
    rows = connect_and_run_query(sql_query=SQL_FIND_POP_LAST100DAYS, config_file=CONFIG_FILE)

    #  Process results
    if rows is not None:
        if rows:
            print("\n--- Query Results for check_new_pop_entries() ---")
            for row in rows:
                print(f"FilePath: {row[0]}, Date Created: {row[1]}, FileID: {row[2]}")
            print("--------------------")
        else:
            logger.info("\nNo results found for the given query.")
            
    else:
        print("\nQuery execution failed. Check the error messages above.")

if __name__ == "__main__":
    run_pop_automation_loop()
