# StarCasualty Pop Automation Program.
import pyodbc
import sys # Used for exiting the script gracefully

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

def read_config(filename):
    """
    Reads database configuration from a simple key-value file.

    Args:
        filename (str): The path to the configuration file.

    Returns:
        dict: A dictionary containing the configuration, or None if file is not found.
    """
    config = {}
    try:
        with open(filename, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'): # Ignore empty lines and comments
                    key, value = line.split('=', 1)
                    config[key.strip().upper()] = value.strip() # Keys are case-insensitive
        return config
    except FileNotFoundError:
        print(f"Error: Configuration file '{filename}' not found.")
        print("Please create it with the required settings.")
        return None
    except Exception as e:
        print(f"Error reading configuration file: {e}")
        return None


def execute_sql_query(config, query):
    """
    Connects to a SQL Server database, executes a query, and returns the results.

    Args:
        config (dict): A dictionary with connection details.
        query (str): The SQL query to execute.

    Returns:
        list: A list of rows fetched from the database, or None if an error occurs.
    """
    connection = None
    try:
        driver = "{SQL Server}" # Change if you have a different driver
        server = config['SERVER']
        database = config['DATABASE']
        auth_method = config.get('AUTHENTICATION', 'SQL').upper() # Default to SQL Auth

        if auth_method == 'WINDOWS':
            # Windows Authentication uses a trusted connection
            connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
            print(f"Connecting to {server} using Windows Authentication...")
        else:
            # SQL Server Authentication requires username and password
            username = config['USERNAME']
            password = config['PASSWORD']
            connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};'
            print(f"Connecting to {server} using SQL Server Authentication...")

        connection = pyodbc.connect(connection_string)
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
        print("Please check the file and try again.")
        print("---------------------------")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

    finally:
        if connection:
            connection.close()
            print("\nDatabase connection closed.")

def main():
    """
    Main routine to read config, define the query, and process the results.
    """
    config = read_config(CONFIG_FILE)
    if not config:
        sys.exit(1)

    # The query to be executed.
    sql_query = """
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

    # Call the function to execute the query
    rows = execute_sql_query(config, sql_query)

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


# --- Script Entry Point ---
if __name__ == "__main__":
    main()
