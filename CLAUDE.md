# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python-based automation tool for syncing StarCasualty "Proof of Prior" (POP) records between Microsoft machine and macbook. The system queries a SQL Server database to find and track insurance files related to proof of prior coverage tasks.

## Architecture

The project consists of two main Python modules:

- **pop_automation.py** - Main automation script with database connection handling, ODBC driver detection, and query execution
- **pop_sql.py** - SQL query definitions for finding POP records

### Key Components

- **Database Connection**: Uses pyodbc with automatic ODBC driver detection for SQL Server connectivity
- **Configuration**: Reads database credentials from `env.txt` file (not tracked in git)
- **Query Execution**: Two main queries - basic POP search and recent entries (last 100 days)

### Configuration Format

The system expects an `env.txt` file with database connection details:
```
AUTHENTICATION=SQL
SERVER=192.168.2.10
DATABASE=isdata15testsql
USERNAME=your_sql_username
PASSWORD=your_sql_password
```

## Common Commands

This is a Python script project without package.json or build tools. Run directly with:

```bash
# Run the main automation script
python pop_automation.py

# Check for new POP entries (last 100 days)
python -c "from pop_automation import check_new_pop_entries; check_new_pop_entries()"
```

## Development Notes

- Database queries target the StarCasualty insurance system tables (isfiles, uwtasksdone)
- The system searches for tasks with comments matching "Proof of Prior%" pattern
- Supports both SQL Server authentication and Windows integrated authentication
- Includes comprehensive error handling for database connections and ODBC driver issues