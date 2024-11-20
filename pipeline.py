import pandas as pd
from typing import Optional
import logging
from datetime import datetime
from config import CONFIG
from dataprocessor import DataProcessor
from utils import setup_logging
from main import load_model, predict
from database import database_connection

class Pipeline:
    """Main pipeline class to orchestrate the ETL process."""
    
    def __init__(self):
        self.logger = setup_logging(CONFIG['log_file'])
        self.processor = DataProcessor(self.logger)
        self.batch_id = int(datetime.now().timestamp())

    def load_data(self) -> pd.DataFrame:
        """Load input data from CSV file."""
        try:
            self.logger.info(f"Loading data from {CONFIG['input_file']}")
            df = pd.read_csv(CONFIG['input_file'])
            df.columns = df.columns.str.lower()
            self.logger.info(f"Loaded {len(df)} records")
            return df
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
            raise

    def process_data(self) -> None:
        """Execute the complete data processing pipeline."""
        try:
            # Load data
            raw_data = self.load_data()
            
            # Save raw data with batch_id
            self.processor.save_to_db(
                raw_data,
                'raw_data'
            )
            
            # Transform data
            transformed_data = self.processor.transform_data(raw_data)
            
            # Save transformed data with batch_id
            self.processor.save_to_db(
                transformed_data,
                'transformed_data'
            )
            
            # Load model and make predictions
            model = load_model(CONFIG['model_file'])
            features = transformed_data.drop(
                columns=['median_house_value'],
                errors='ignore'
            )
            predictions = predict(features, model)
            
            # Create predictions DataFrame
            predictions_df = pd.DataFrame({
                'input_data_id': range(1, len(predictions) + 1),  # Generate sequential IDs
                'predicted_price': predictions
            })
            
            # Save predictions using bulk insert
            self.processor.save_to_db(
                predictions_df,
                'predictions',
                predicted_price_col='predicted_price',
                input_data_id_col='input_data_id'
            )
            
            self.logger.info(f"Pipeline executed successfully for batch")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed for batch: {e}")
            raise
