import sqlite3
import uuid
from typing import List, Optional, Tuple
from datetime import datetime
import os

class PopLocalDatabase:
    def __init__(self, db_path: str = "pop_automation_db.sqlite"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize the SQLite database and create tables if they don't exist."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pop_local_state (
                    PopProcessingId TEXT PRIMARY KEY,
                    FileID TEXT NOT NULL,
                    originaldate TEXT NOT NULL,
                    filepath TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('NOT_PROCESSED', 'IN_PROGRESS', 'FAILED', 'PROCESSED'))
                )
            """)
            conn.commit()
    
    def insert_record(self, file_id: str, original_date: str, filepath: str, 
                     status: str = "NOT_PROCESSED") -> str:
        """Insert a new record and return the generated PopProcessingId."""
        pop_processing_id = str(uuid.uuid4())
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO pop_local_state (PopProcessingId, FileID, originaldate, filepath, status)
                VALUES (?, ?, ?, ?, ?)
            """, (pop_processing_id, file_id, original_date, filepath, status))
            conn.commit()
        
        return pop_processing_id
    
    def update_status(self, pop_processing_id: str, status: str) -> bool:
        """Update the status of a record by PopProcessingId."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE pop_local_state SET status = ? WHERE PopProcessingId = ?
            """, (status, pop_processing_id))
            conn.commit()
            return cursor.rowcount > 0
    
    def get_record_by_id(self, pop_processing_id: str) -> Optional[Tuple]:
        """Get a record by PopProcessingId."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT PopProcessingId, FileID, originaldate, filepath, status
                FROM pop_local_state WHERE PopProcessingId = ?
            """, (pop_processing_id,))
            return cursor.fetchone()
    
    def get_records_by_status(self, status: str) -> List[Tuple]:
        """Get all records with a specific status."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT PopProcessingId, FileID, originaldate, filepath, status
                FROM pop_local_state WHERE status = ?
            """, (status,))
            return cursor.fetchall()
    
    def get_record_by_file_id(self, file_id: str) -> Optional[Tuple]:
        """Get a record by FileID."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT PopProcessingId, FileID, originaldate, filepath, status
                FROM pop_local_state WHERE FileID = ?
            """, (file_id,))
            return cursor.fetchone()
    
    def get_all_records(self) -> List[Tuple]:
        """Get all records from the pop_local_state table."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT PopProcessingId, FileID, originaldate, filepath, status
                FROM pop_local_state ORDER BY originaldate DESC
            """)
            return cursor.fetchall()
    
    def delete_record(self, pop_processing_id: str) -> bool:
        """Delete a record by PopProcessingId."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM pop_local_state WHERE PopProcessingId = ?
            """, (pop_processing_id,))
            conn.commit()
            return cursor.rowcount > 0
    
    def count_records_by_status(self, status: str) -> int:
        """Count records with a specific status."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM pop_local_state WHERE status = ?
            """, (status,))
            return cursor.fetchone()[0]

def create_pop_database(db_path: str = "pop_automation_db.sqlite") -> PopLocalDatabase:
    """Factory function to create and initialize a PopLocalDatabase instance."""
    return PopLocalDatabase(db_path)

if __name__ == "__main__":
    db = create_pop_database()
    print("PopLocalDatabase initialized successfully")
    print(f"Database file: {db.db_path}")