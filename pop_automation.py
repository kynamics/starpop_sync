# StarCasualty Pop Automation Program.
import pyodbc
import sys # Used for exiting the script gracefully

# --- File to hold configuration ---
# This script now reads connection details from a separate file.
# Create a file named "env.txt" in the same directory as this script.
# The file should have the following format (replace with your actual details):
#
# SERVER=192.168.2.10
# DATABASE=isdata15testsql
# USERNAME=your_username
# PASSWORD=your_password
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
                    config[key.strip()] = value.strip()
        return config
    except FileNotFoundError:
        print(f"Error: Configuration file '{filename}' not found.")
        print("Please create it with the required server, database, username, and password.")
        return None
    except Exception as e:
        print(f"Error reading configuration file: {e}")
        return None


def execute_sql_query(server, database, username, password, query):
    """
    Connects to a SQL Server database, executes a query, and returns the results.

    Args:
        server (str): The address of the SQL Server.
        database (str): The name of the database to connect to.
        username (str): The username for authentication.
        password (str): The password for authentication.
        query (str): The SQL query to execute.

    Returns:
        list: A list of rows fetched from the database, or None if an error occurs.
    """
    connection = None  # Initialize connection to None
    try:
        # Construct the connection string.
        connection_string = (
            f'DRIVER={{SQL Server}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password};'
        )

        # Establish the connection to the database.
        print(f"Connecting to database on {server}...")
        connection = pyodbc.connect(connection_string)
        print("Connection successful.")

        cursor = connection.cursor()
        print("Executing query...")
        cursor.execute(query)
        print("Query executed.")

        return cursor.fetchall()

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Error connecting to or querying the database.")
        print(f"SQLSTATE: {sqlstate}")
        print(f"Message: {ex}")
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
    # Read configuration from the env.txt file
    config = read_config(CONFIG_FILE)
    if not config:
        sys.exit(1) # Exit if config file is not found or is invalid

    # Check for all required keys
    required_keys = ['SERVER', 'DATABASE', 'USERNAME', 'PASSWORD']
    if not all(key in config for key in required_keys):
        print(f"Error: The config file '{CONFIG_FILE}' is missing one or more required keys.")
        print(f"Please ensure it contains: {', '.join(required_keys)}")
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

    # Call the function to execute the query using credentials from the file
    rows = execute_sql_query(
        config['SERVER'],
        config['DATABASE'],
        config['USERNAME'],
        config['PASSWORD'],
        sql_query
    )

    # Process and display the results.
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
