import sqlite3
from contextlib import contextmanager
from typing import Generator, Optional
import logging
import pandas as pd

@contextmanager
def database_connection(db_path: str) -> Generator[sqlite3.Connection, None, None]:
    """Context manager for database connections."""
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        yield conn
    except sqlite3.Error as e:
        logging.getLogger('housing_pipeline').error(f"Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()

def get_sqlite_type(dtype):
    """Map pandas dtypes to SQLite types"""
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    else:
        return "TEXT"

def create_table_from_dataframe(conn, table_name: str, df: Optional[pd.DataFrame] = None):
    """Create a table based on DataFrame schema or with default schema if DataFrame not provided"""
    if df is not None:
        # Get column definitions from DataFrame
        column_definitions = [
            f"{col.lower()} {get_sqlite_type(df[col].dtype)}"
            for col in df.columns
            if col.lower() != 'batch_id'  # Exclude batch_id if it exists
        ]
        
        # Add standard columns at the beginning
        base_columns = [
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "batch_id INTEGER NOT NULL",
            "timestamp DATETIME DEFAULT CURRENT_TIMESTAMP"
        ]
        
        # Combine all columns
        all_columns = base_columns + column_definitions
        
        # Create table
        create_statement = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(all_columns)}
            )
        """
    else:
        # Default schema for raw_data and predictions tables
        if table_name == 'raw_data':
            create_statement = """
                CREATE TABLE IF NOT EXISTS raw_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL,
                    inserted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    longitude FLOAT,
                    latitude FLOAT,
                    median_age INTEGER,
                    rooms INTEGER,
                    bedrooms INTEGER,
                    population INTEGER,
                    households INTEGER,
                    median_income FLOAT,
                    median_house_value FLOAT,
                    ocean_proximity TEXT,
                    agency BOOLEAN
                )
            """
        elif table_name == 'predictions':
            create_statement = """
                CREATE TABLE IF NOT EXISTS predictions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    input_data_id INTEGER,
                    predicted_price FLOAT,
                    FOREIGN KEY(input_data_id) REFERENCES transformed_data(id)
                )
            """
    
    conn.execute(create_statement)

def initialize_database(db_path: str, transformed_df: Optional[pd.DataFrame] = None) -> None:
    """
    Initialize database with required tables.
    
    Args:
        db_path: Path to the SQLite database
        transformed_df: Optional DataFrame with the structure of transformed data
                       If provided, will create transformed_data table based on its schema
    """
    with database_connection(db_path) as conn:
        # Create raw_data table with default schema
        create_table_from_dataframe(conn, 'raw_data')
        
        # Create transformed_data table based on provided DataFrame or skip if not provided
        if transformed_df is not None:
            create_table_from_dataframe(conn, 'transformed_data', transformed_df)
        
        # Create predictions table with default schema
        create_table_from_dataframe(conn, 'predictions')
        
        conn.commit()