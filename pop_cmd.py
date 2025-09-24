"""
Rich console interface for POP automation with slash commands.
"""
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.prompt import Prompt
from rich import print as rprint
from typing import List, Optional
import sys

from bot_logger import get_logger
from local_db import get_pop_db, PopLocalDatabase
from pop_sql import SQL_FIND_POP_LAST100DAYS
from star_util import CONFIG_FILE, truncate_filepath
from ms_sql_server_connector import connect_and_run_query, fetch_match_table_rows

logger = get_logger()
console = Console()

class PopConsole:
    """Rich console interface for POP automation."""
    
    def __init__(self):
        self.console = Console()
        self.running = True
        
    def show_welcome(self):
        """Display welcome message and available commands."""
        welcome_text = Text("StarCasualty POP Automation Console", style="bold blue")
        self.console.print(Panel(welcome_text, title="Welcome", border_style="green"))
        
        help_text = """
Available  commands:
list_local_db    - List entries from local database
list_mstask_db  - List entries from MS SQL Task Table database  
list_match_db    - List entries from MS SQL Match Table database
delete_local_db  - Delete a record from local database (requires processing_id)
search_local_db  - Search local database for keyword matches (requires keyword)
search_mstask_db  - Search MS SQL Task Table database for keyword matches (requires keyword)
search_match_db   - Search MS SQL Match Table database for keyword matches (requires keyword)
help             - Show this help message
exit             - Exit the console
        """
        self.console.print(Panel(help_text.strip(), title="Commands", border_style="yellow"))
        
    def show_help(self):
        """Display help information."""
        help_text = """
Commands:
list_local_db    - Display all entries from the local POP database
list_mstask_db  - Display recent POP entries from MS SQL Task Table database
list_match_db    - Display all entries from the MS SQL Match Table database
delete_local_db  - Delete a record from local database (requires processing_id)
search_local_db  - Search local database for keyword matches (requires keyword)
search_mstask_db  - Search MS SQL database for keyword matches (requires keyword)
search_match_db   - Search MS SQL Match Table database for keyword matches (requires keyword)
help             - Show this help message
exit             - Exit the console

You can also type any other text to see it echoed back.
        """
        self.console.print(Panel(help_text.strip(), title="Help", border_style="cyan"))
        
    def list_local_db(self):
        """List entries from local database."""
        try:
            self.console.print("\n[bold green]Fetching local database entries...[/bold green]")
            
            # Get local database entries
            db = get_pop_db()
            entries = db.get_all_records()
            
            if not entries:
                self.console.print("[yellow]No entries found in local database.[/yellow]")
                return
                
            # Create table for display
            table = Table(title="Local Database Entries")
            table.add_column("Processing ID", style="cyan")
            table.add_column("File ID", style="cyan")
            table.add_column("Date Created", style="magenta")
            table.add_column("File Path", style="green")
            table.add_column("Status", style="yellow")
            table.add_column("Match Result", style="green")
            
            for entry in entries:
                processing_id = entry[0]
                file_id = entry[1]
                date_created = str(entry[2])
                filepath = entry[3]
                # Format filepath for multi-line display if it's too long
                filepath = truncate_filepath(filepath)
                status = entry[4]
                match_result = entry[5]
                table.add_row(
                    str(processing_id),
                    file_id,
                    date_created,
                    filepath,
                    status,
                    match_result
                )
                
            self.console.print(table)
            
        except Exception as e:
            self.console.print(f"[red]Error fetching local database entries: {e}[/red]")
            logger.error(f"Error in list_local_db: {e}")
            
    def list_mstask_db(self):
        """List entries from MS SQL database."""
        try:
            self.console.print("\n[bold green]Fetching MS SQL database entries...[/bold green]")
            
            # Query MS SQL database
            rows = connect_and_run_query(sql_query=SQL_FIND_POP_LAST100DAYS, config_file=CONFIG_FILE)
            
            if not rows:
                self.console.print("[yellow]No entries found in MS SQL database.[/yellow]")
                return
                
            # Create table for display
            table = Table(title="MS SQL Database Entries (Last 100 Days)")
            table.add_column("File Path", style="cyan")
            table.add_column("Date Created", style="magenta")
            table.add_column("File ID", style="green")
            table.add_column("Policy ID", style="yellow")
            
            for row in rows:
                table.add_row(
                    str(row[0]),  # FilePath
                    str(row[1]),  # Date Created
                    str(row[2]),  # FileID
                    str(row[3])  if len(row) > 3 else "N/A"  # PolicyID
                )
                
            self.console.print(table)
            
        except Exception as e:
            self.console.print(f"[red]Error fetching MS SQL database entries: {e}[/red]")
            logger.error(f"Error in list_mstask_db: {e}")
            
    def list_match_db(self):
        """List entries from MS SQL Match Table database."""
        try:
            self.console.print("\n[bold green]Fetching MS SQL Match Table entries...[/bold green]")
            
            # Fetch match table rows
            rows = fetch_match_table_rows()
            
            if not rows:
                self.console.print("[yellow]No entries found in MS SQL Match Table database.[/yellow]")
                return
                
            # Create table for display
            table = Table(title="MS SQL Match Table Entries")
            table.add_column("Column 1", style="cyan")
            table.add_column("Column 2", style="magenta")
            table.add_column("Column 3", style="green")
            table.add_column("Column 4", style="yellow")
            table.add_column("Column 5", style="blue")
            
            # Display first few rows to understand structure
            for i, row in enumerate(rows[:10]):  # Limit to first 10 rows for display
                # Convert all values to strings and handle None values
                row_values = [str(val) if val is not None else "N/A" for val in row]
                
                # Pad with empty strings if row has fewer columns than expected
                while len(row_values) < 5:
                    row_values.append("")
                
                # Truncate if row has more columns than expected
                row_values = row_values[:5]
                
                table.add_row(*row_values)
            
            self.console.print(table)
            
            if len(rows) > 10:
                self.console.print(f"[yellow]Showing first 10 of {len(rows)} total entries[/yellow]")
            else:
                self.console.print(f"[green]Displayed {len(rows)} entries[/green]")
            
        except Exception as e:
            self.console.print(f"[red]Error fetching MS SQL Match Table entries: {e}[/red]")
            logger.error(f"Error in list_match_db: {e}")
            
    def delete_local_db(self, processing_id: str):
        """Delete a record from local database by processing ID."""
        try:
            if not processing_id:
                self.console.print("[red]Error: Processing ID is required for delete_local_db command.[/red]")
                self.console.print("[yellow]Usage: delete_local_db <processing_id>[/yellow]")
                return
                
            self.console.print(f"\n[bold green]Deleting record with processing ID: {processing_id}...[/bold green]")
            
            # Get database instance and delete record
            db = get_pop_db()
            success = db.delete_record(processing_id)
            
            if success:
                self.console.print(f"[green]Successfully deleted record with processing ID: {processing_id}[/green]")
            else:
                self.console.print(f"[yellow]No record found with processing ID: {processing_id}[/yellow]")
                
        except Exception as e:
            self.console.print(f"[red]Error deleting record: {e}[/red]")
            logger.error(f"Error in delete_local_db: {e}")
            
    def search_local_db(self, keyword: str):
        """Search local database for keyword matches across all fields."""
        try:
            if not keyword:
                self.console.print("[red]Error: search_local_db requires a keyword parameter.[/red]")
                self.console.print("[yellow]Usage: search_local_db <keyword>[/yellow]")
                return
                
            self.console.print(f"\n[bold green]Searching for keyword: '{keyword}'...[/bold green]")
            
            # Get all records from local database
            db = get_pop_db()
            entries = db.get_all_records()
            
            if not entries:
                self.console.print("[yellow]No entries found in local database.[/yellow]")
                return
                
            # Search for keyword matches
            matches = []
            keyword_lower = keyword.lower()
            
            for entry in entries:
                # Check all fields for keyword match (case-insensitive)
                processing_id = str(entry[0])
                file_id = str(entry[1])
                date_created = str(entry[2])
                filepath = str(entry[3])
                status = str(entry[4])
                match_result = str(entry[5])
                
                # Check if keyword matches any field
                if (keyword_lower in processing_id.lower() or
                    keyword_lower in file_id.lower() or
                    keyword_lower in date_created.lower() or
                    keyword_lower in filepath.lower() or
                    keyword_lower in status.lower() or
                    keyword_lower in match_result.lower()):
                    matches.append(entry)
            
            if not matches:
                self.console.print(f"[yellow]No matches found for keyword: '{keyword}'[/yellow]")
                return
                
            # Create table for display
            table = Table(title=f"Search Results for '{keyword}' ({len(matches)} matches)")
            table.add_column("Processing ID", style="cyan")
            table.add_column("File ID", style="cyan")
            table.add_column("Date Created", style="magenta")
            table.add_column("File Path", style="green")
            table.add_column("Status", style="yellow")
            table.add_column("Match Result", style="green")
            
            for entry in matches:
                processing_id = str(entry[0])
                file_id = str(entry[1])
                date_created = str(entry[2])
                filepath = truncate_filepath(str(entry[3]))
                status = str(entry[4])
                match_result = str(entry[5])
                
                table.add_row(
                    processing_id,
                    file_id,
                    date_created,
                    filepath,
                    status,
                    match_result
                )
                
            self.console.print(table)
            self.console.print(f"[green]Found {len(matches)} matching records[/green]")
            
        except Exception as e:
            self.console.print(f"[red]Error searching local database: {e}[/red]")
            logger.error(f"Error in search_local_db: {e}")
            
    def search_mstask_db(self, keyword: str):
        """Search MS SQL database for keyword matches across all fields."""
        try:
            if not keyword:
                self.console.print("[red]Error: search_mstask_db requires a keyword parameter.[/red]")
                self.console.print("[yellow]Usage: search_mstask_db <keyword>[/yellow]")
                return
                
            self.console.print(f"\n[bold green]Searching MS SQL database for keyword: '{keyword}'...[/bold green]")
            
            # Query MS SQL database
            rows = connect_and_run_query(sql_query=SQL_FIND_POP_LAST100DAYS, config_file=CONFIG_FILE)
            
            if not rows:
                self.console.print("[yellow]No entries found in MS SQL database.[/yellow]")
                return
                
            # Search for keyword matches
            matches = []
            keyword_lower = keyword.lower()
            
            for row in rows:
                # Check all fields for keyword match (case-insensitive)
                filepath = str(row[0])
                date_created = str(row[1])
                file_id = str(row[2])
                policy_id = str(row[3]) if len(row) > 3 else "N/A"
                
                # Check if keyword matches any field
                if (keyword_lower in filepath.lower() or
                    keyword_lower in date_created.lower() or
                    keyword_lower in file_id.lower() or
                    keyword_lower in policy_id.lower()):
                    matches.append(row)
            
            if not matches:
                self.console.print(f"[yellow]No matches found for keyword: '{keyword}'[/yellow]")
                return
                
            # Create table for display
            table = Table(title=f"MS SQL Search Results for '{keyword}' ({len(matches)} matches)")
            table.add_column("File Path", style="cyan")
            table.add_column("Date Created", style="magenta")
            table.add_column("File ID", style="green")
            table.add_column("Policy ID", style="yellow")
            
            for row in matches:
                filepath = truncate_filepath(str(row[0]))
                date_created = str(row[1])
                file_id = str(row[2])
                policy_id = str(row[3]) if len(row) > 3 else "N/A"
                
                table.add_row(
                    filepath,
                    date_created,
                    file_id,
                    policy_id
                )
                
            self.console.print(table)
            self.console.print(f"[green]Found {len(matches)} matching records[/green]")
            
        except Exception as e:
            self.console.print(f"[red]Error searching MS SQL database: {e}[/red]")
            logger.error(f"Error in search_mstask_db: {e}")
            
    def search_match_db(self, keyword: str):
        """Search MS SQL Match Table database for keyword matches across all fields."""
        try:
            if not keyword:
                self.console.print("[red]Error: search_match_db requires a keyword parameter.[/red]")
                self.console.print("[yellow]Usage: search_match_db <keyword>[/yellow]")
                return
                
            self.console.print(f"\n[bold green]Searching MS SQL Match Table for keyword: '{keyword}'...[/bold green]")
            
            # Fetch match table rows
            rows = fetch_match_table_rows()
            
            if not rows:
                self.console.print("[yellow]No entries found in MS SQL Match Table database.[/yellow]")
                return
                
            # Search for keyword matches
            matches = []
            keyword_lower = keyword.lower()
            
            for row in rows:
                # Check all fields for keyword match (case-insensitive)
                row_str_values = [str(val) if val is not None else "" for val in row]
                
                # Check if keyword matches any field
                if any(keyword_lower in str_val.lower() for str_val in row_str_values):
                    matches.append(row)
            
            if not matches:
                self.console.print(f"[yellow]No matches found for keyword: '{keyword}'[/yellow]")
                return
                
            # Create table for display
            table = Table(title=f"MS SQL Match Table Search Results for '{keyword}' ({len(matches)} matches)")
            table.add_column("Column 1", style="cyan")
            table.add_column("Column 2", style="magenta")
            table.add_column("Column 3", style="green")
            table.add_column("Column 4", style="yellow")
            table.add_column("Column 5", style="blue")
            
            for row in matches:
                # Convert all values to strings and handle None values
                row_values = [str(val) if val is not None else "N/A" for val in row]
                
                # Pad with empty strings if row has fewer columns than expected
                while len(row_values) < 5:
                    row_values.append("")
                
                # Truncate if row has more columns than expected
                row_values = row_values[:5]
                
                table.add_row(*row_values)
                
            self.console.print(table)
            self.console.print(f"[green]Found {len(matches)} matching records[/green]")
            
        except Exception as e:
            self.console.print(f"[red]Error searching MS SQL Match Table: {e}[/red]")
            logger.error(f"Error in search_match_db: {e}")
            
    def process_command(self, command: str) -> bool:
        """Process a  command. Returns True if command was handled."""
        command = command.strip()
        command_lower = command.lower()
        
        if command_lower == "help":
            self.show_help()
            return True
        elif command_lower == "list_local_db":
            self.list_local_db()
            return True
        elif command_lower == "list_mstask_db":
            self.list_mstask_db()
            return True
        elif command_lower == "list_match_db":
            self.list_match_db()
            return True
        elif command_lower == "exit":
            self.console.print("[bold red]Goodbye![/bold red]")
            self.running = False
            return True
        elif command_lower.startswith("delete_local_db"):
            # Parse the processing_id parameter
            parts = command.split()
            if len(parts) < 2:
                self.console.print("[red]Error: delete_local_db requires a processing_id parameter.[/red]")
                self.console.print("[yellow]Usage: delete_local_db <processing_id>[/yellow]")
                return True
            processing_id = parts[1]
            self.delete_local_db(processing_id)
            return True
        elif command_lower.startswith("search_local_db"):
            # Parse the keyword parameter
            parts = command.split()
            if len(parts) < 2:
                self.console.print("[red]Error: search_local_db requires a keyword parameter.[/red]")
                self.console.print("[yellow]Usage: search_local_db <keyword>[/yellow]")
                return True
            keyword = parts[1]
            self.search_local_db(keyword)
            return True
        elif command_lower.startswith("search_mstask_db"):
            # Parse the keyword parameter
            parts = command.split()
            if len(parts) < 2:
                self.console.print("[red]Error: search_mstask_db requires a keyword parameter.[/red]")
                self.console.print("[yellow]Usage: search_mstask_db <keyword>[/yellow]")
                return True
            keyword = parts[1]
            self.search_mstask_db(keyword)
            return True
        elif command_lower.startswith("search_match_db"):
            # Parse the keyword parameter
            parts = command.split()
            if len(parts) < 2:
                self.console.print("[red]Error: search_match_db requires a keyword parameter.[/red]")
                self.console.print("[yellow]Usage: search_match_db <keyword>[/yellow]")
                return True
            keyword = parts[1]
            self.search_match_db(keyword)
            return True
        else:
            return False
            
    def run(self):
        """Main console loop."""
        self.show_welcome()
        
        while self.running:
            try:
                # Get user input
                user_input = Prompt.ask("\n[bold blue]POP Console>[/bold blue]")
                
                # # Process slash commands
                # if user_input.startswith("/"):
                if not self.process_command(user_input):
                    self.console.print(f"[red]Unknown command: {user_input}[/red]")
                    self.console.print("[yellow]Type /help for available commands.[/yellow]")
                # else:
                #     # Echo non-command input
                #     self.console.print(f"[green]You said: {user_input}[/green]")
                    
            except KeyboardInterrupt:
                self.console.print("\n[bold red]Goodbye![/bold red]")
                break
            except EOFError:
                self.console.print("\n[bold red]Goodbye![/bold red]")
                break
            except Exception as e:
                self.console.print(f"[red]Error: {e}[/red]")
                logger.error(f"Console error: {e}")

def run_pop_console():
    """Entry point for the POP console."""
    pop_console = PopConsole()
    pop_console.run()

if __name__ == "__main__":
    run_pop_console()
