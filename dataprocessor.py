import pandas as pd
import numpy as np
from typing import Tuple, Optional
import json
from config import CONFIG
from database import database_connection
import logging

class DataProcessor:
    """Class to handle data processing and transformation."""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger('housing_pipeline')
        self.transformed_schema_initialized = False

    def validate_data(self, df: pd.DataFrame) -> None:
        """Validate input data structure and content."""
        # Check required columns
        missing_cols = set(map(str.lower, CONFIG['required_columns'])) - set(df.columns.str.lower())
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Check nulls in columns
        for col in df.columns:
            if df[col].isnull().any():
                self.logger.warning(f"Found null values in column: {col}")


    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform input data to the required format."""
        self.validate_data(df)
        
        # Create a copy to avoid modifying original data
        transformed = df.copy()
        transformed = transformed.dropna()
        transformed = transformed[~transformed.eq("Null").any(axis=1)]
        # Encode categorical variables
        transformed = pd.get_dummies(transformed)
        
        return transformed
    
    def initialize_transformed_schema(self, df: pd.DataFrame):
        """Initialize the transformed_data table schema if not already done"""
        if not self.transformed_schema_initialized:
            database_connection.initialize_database(self.config['db_path'], df)
            self.transformed_schema_initialized = True

    def save_to_db(
        self,
        df: pd.DataFrame,
        table_name: str,
        batch_id: Optional[int] = None,
        predicted_price_col: Optional[str] = None,
        input_data_id_col: Optional[str] = None
        ) -> None:
        """Save DataFrame to the appropriate table in the database using bulk insert."""
        with database_connection(self.config['db_path']) as conn:
            current_batch_id = batch_id if batch_id is not None else self.batch_id
            
            if table_name == 'raw_data':
                df_to_save = df.copy()
                df_to_save['batch_id'] = current_batch_id
                df_to_save.to_sql('raw_data', conn, if_exists='append', index=False)
                
            elif table_name == 'transformed_data':
                # Initialize transformed_data table schema if needed
                self.initialize_transformed_schema(df)
                
                df_to_save = df.copy()
                df_to_save['batch_id'] = current_batch_id
                df_to_save.to_sql('transformed_data', conn, if_exists='append', index=False)
                
            elif table_name == 'predictions':
                if predicted_price_col is None or input_data_id_col is None:
                    raise ValueError(
                        "For the 'predictions' table, 'predicted_price_col' and 'input_data_id_col' must be provided."
                    )
                
                predictions_df = pd.DataFrame({
                    'batch_id': [current_batch_id] * len(df),
                    'input_data_id': df[input_data_id_col],
                    'predicted_price': df[predicted_price_col]
                })
                
                predictions_df.to_sql('predictions', conn, if_exists='append', index=False)
            
            else:
                raise ValueError(f"Unsupported table name: {table_name}")
            
            conn.commit()