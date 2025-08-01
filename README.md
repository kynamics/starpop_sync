# star_sync


Implements Proof of prior automation for StarCasualty.

- Reads Pop document from Sql server using pyodbc
- maintains a local sqlite db of document status (processed, failed, in progress).
- uses GenAI APIs to extract json information.

Remaining work:
- Write results back to sql table.
