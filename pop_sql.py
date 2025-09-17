# Sql strings for use with the MS Sql Server for StarCasualty.

SQL_FIND_POP_LAST100DAYS = """
SELECT 
    FilePath, DateCreated, ISFileID, PolicyID
FROM
    isdata15testsql..isfiles 
WHERE
    ISFileID in (
        SELECT 
            ISFileID 
        FROM 
            isdata15testsql..uwtasksdone 
        WHERE
            TaskComments LIKE 'Proof of Prior%' AND DateCreated > getdate()-100
    )
"""

SQL_FIND_POP_LAST_ONEDAY = """
SELECT 
    FilePath, DateCreated, ISFileID, PolicyID
FROM
    isdata15testsql..isfiles 
WHERE
    ISFileID in (
        SELECT 
            ISFileID 
        FROM 
            isdata15testsql..uwtasksdone 
        WHERE
            TaskComments LIKE 'Proof of Prior%' AND DateCreated > getdate()-1
    )
"""

SQL_FIND_POP_BASIC = """
    SELECT
        FilePath
    FROM
        isdata15testsql..isfiles
    WHERE
        ISFileID IN (
            SELECT
                ISFileID
            FROM
                isdata15testsql..uwtasksdone
            WHERE
                TaskComments LIKE 'Proof of Prior%'
                AND PolicyID = 482144
                AND DateCreated > GETDATE() - 1
        )
    
    
"""

def get_sql_insert_into_match_table(policyid, fileid, namedinsured, expirationdate, agentcode,
     companyname, namedinsuredmatch, expirationdatematch, agentcodematch, companynamematch, remarks):
    SQL_INSERT_INTO_MATCH_TABLE = f"""
INSERT INTO isdata15testsql..POPTaskMatch (
    PolicyID,
    FileID,
    NamedInsured,
    ExpirationDate,
    AgentCode,
    CompanyName,
    NamedInsuredMatch,
    ExpirationDateMatch,
    AgentCodeMatch,
    CompanyNameMatch,
    Remarks
) VALUES (
    '{policyid}',
    '{fileid}',
    '{namedinsured}',
    '{expirationdate}',
    '{agentcode}',
    '{companyname}',
    '{namedinsuredmatch}',
    '{expirationdatematch}',
    '{agentcodematch}',
    '{companynamematch}',
    '{remarks}'
) ON DUPLICATE KEY UPDATE

    NamedInsured = '{namedinsured}',
    ExpirationDate = '{expirationdate}',
    AgentCode = '{agentcode}',
    CompanyName = '{companyname}',
    NamedInsuredMatch = '{namedinsuredmatch}',
    ExpirationDateMatch = '{expirationdatematch}',
    AgentCodeMatch = '{agentcodematch}',
    CompanyNameMatch = '{companynamematch}',
    Remarks = '{remarks}';
    """
    return SQL_INSERT_INTO_MATCH_TABLE


def get_sql_dump_match_table():
    SQL_DUMP_MATCH_TABLE = """
    SELECT * FROM POPTaskMatch
    """
    return SQL_DUMP_MATCH_TABLE


def get_sql_find_popfields_testdb(policyid):
    SQL_FIND_POPFIELDS_TESTDB = f"""
select top 1 d.policyid, NamedInsured, d.EffectiveDate , d.ExpirationDate ,d.agentcode,DBAName,AgentName,ChoiceValue,ChoiceText, dbo.ReadWDDX_udf (decInfo,'PriorCarrier') as PriorCarrier 
from ISData15TestSQL..decpages d
inner join
ISData15TestSQL..Agents a on 
d.AgentCode =a.AgentCode 
inner join ISRating15testSQL..ManualChoices m on 
dbo.ReadWDDX_udf (decInfo,'PriorCarrier') =ChoiceValue
and m.ManualID =622
inner join ISData15TestSQL..UWMaster w
on
d.PolicyID = w.PolicyID 
where decpageid=1 
and d.policyid={policyid}
"""
    return SQL_FIND_POPFIELDS_TESTDB