# StarCasualty Pop Automation Program.
import pyodbc
import sys # Used for exiting the script gracefully
from pop_sql import SQL_FIND_POP_BASIC, SQL_FIND_POP_LAST100DAYS

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
        return None
    except Exception as e:
        print(f"Error reading configuration file: {e}")
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
        print(f"SQLSTATE: {sqlstate}")
        print(f"Message: {ex}")
        print("----------------------")
        return None
    except KeyError as e:
        print(f"\n--- CONFIGURATION ERROR ---")
        print(f"Your '{CONFIG_FILE}' is missing a required setting: {e}")
        print("---------------------------")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
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



def main():
    """
    Main routine to find driver, read config, define query, and process results.
    """
    rows = connect_and_run_query(sql_query=SQL_FIND_POP_BASIC, config_file=CONFIG_FILE)

    # Step 5: Process results
    if rows is not None:
        if rows:
            print("\n--- Query Results ---")
            for row in rows:
                print(f"FilePath: {row[0]}")
            print("--------------------")
        else:
            print("\nNo results found for the given query.")
    else:
        print("\nQuery execution failed. Check the error messages above.")

def check_new_pop_entries():
    rows = connect_and_run_query(sql_query=SQL_FIND_POP_LAST100DAYS, config_file=CONFIG_FILE)

    # Step 5: Process results
    if rows is not None:
        if rows:
            print("\n--- Query Results for check_new_pop_entries() ---")
            for row in rows:
                print(f"FilePath: {row[0]}, Date Created: {row[1]}, FileID: {row[2]}")
            print("--------------------")
        else:
            print("\nNo results found for the given query.")
    else:
        print("\nQuery execution failed. Check the error messages above.")

if __name__ == "__main__":
    check_new_pop_entries()
