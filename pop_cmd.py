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
from star_util import CONFIG_FILE
from ms_sql_server_connector import connect_and_run_query

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
list_mssql       - List entries from MS SQL database  
help             - Show this help message
exit             - Exit the console
        """
        self.console.print(Panel(help_text.strip(), title="Commands", border_style="yellow"))
        
    def show_help(self):
        """Display help information."""
        help_text = """
Slash Commands:
list_local_db    - Display all entries from the local POP database
list_mssql       - Display recent POP entries from MS SQL database
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
            
    def list_mssql(self):
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
                    row[0],  # FilePath
                    str(row[1]),  # Date Created
                    row[2],  # FileID
                    row[3] if len(row) > 3 else "N/A"  # PolicyID
                )
                
            self.console.print(table)
            
        except Exception as e:
            self.console.print(f"[red]Error fetching MS SQL database entries: {e}[/red]")
            logger.error(f"Error in list_mssql: {e}")
            
    def process_command(self, command: str) -> bool:
        """Process a  command. Returns True if command was handled."""
        command = command.strip().lower()
        
        if command == "help":
            self.show_help()
            return True
        elif command == "list_local_db":
            self.list_local_db()
            return True
        elif command == "list_mssql":
            self.list_mssql()
            return True
        elif command == "exit":
            self.console.print("[bold red]Goodbye![/bold red]")
            self.running = False
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
