from pathlib import Path
from typing import Dict, Any

CONFIG: Dict[str, Any] = {
    'input_file': 'housing.csv',
    'model_file': 'model.joblib',
    'db_path': 'housing_test.db',
    'log_file': 'pipeline.log',
    'required_columns': [
        'LONGITUDE', 'LATITUDE', 'MEDIAN_AGE', 'ROOMS',
        'BEDROOMS', 'POP', 'HOUSEHOLDS', 'MEDIAN_INCOME', 'MEDIAN_HOUSE_VALUE', 
        'OCEAN_PROXIMITY', 'AGENCY'
    ]
}