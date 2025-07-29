# Sql strings for use with the MS Sql Server for StarCasualty.

SQL_FIND_POP_LAST100DAYS = """
SELECT 
    FilePath, DateCreated, ISFileID
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