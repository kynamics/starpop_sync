# star_sync


Implements Proof of prior automation for StarCasualty.

- Reads Pop document from Sql server using pyodbc
- maintains a local sqlite db of document status (processed, failed, in progress).
- uses GenAI APIs to extract json information.

Remaining work:
-  Add PolicyID everywhere (to sqlite etc)
-  Run match query, check match and write results back using SQL to Sql Server.
-  Run automation loop with console terminal on separate thread. 

Error:

some fields can be None during the match 

TODO:
- Agent code matching logic
- Test loop by uploading document to test account. 
- Allow for human approval in the local sql db table. Add human approval trigger.
- cfml page to read and show table with fastapi backend possibility.