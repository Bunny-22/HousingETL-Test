-- Housing Price Prediction ETL Pipeline

This repo contain a pipeline for processsing housing data and generating price predictions using a pre-trained model.

- Features

- Data validation and transformation pipeline
- SQLite database storage for raw data, transformed data, and predictions
- Comprehensive logging
- Error handling and data validation
- Batch processing support
- Performance monitoring

- Installation

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv\Scripts\activate  # On Windows
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

- Usage

-- Running the Pipeline

To process data and generate predictions:

```bash
python run.py
```

This will:
1. Load data from `housing.csv`
2. Transform and validate the data
3. Save raw and transformed data to the database
4. Generate predictions using the model
5. Save predictions to the database

-- Directory Structure

```
├── config.py           # Configuration settings
├── database.py        # Database operations
├── data_processor.py  # Data transformation logic
├── pipeline.py        # Main pipeline implementation
├── utils.py           # Utility functions
├── run.py            # Entry point script
├── housing.csv       # Input data
├── model.joblib      # Pre-trained model
└── requirements.txt   # Python dependencies
```

-- Configuration

Edit `config.py` to modify:
- Input/output file paths
- Database settings
- Logging configuration
- Required data columns

-- Logging

Logs are written to both console and file (`pipeline.log`). The log includes:
- Data loading/transformation events
- Error messages
- Performance metrics
- Processing statistics

-- Database Schema

The SQLite database contains three tables:
1. `raw_data`: Original input data
2. `transformed_data`: Processed and transformed data
3. `predictions`: Model predictions with references to input data

-- Error Handling

The pipeline includes comprehensive error handling for:
- File I/O operations
- Data validation
- Database operations
- Model predictions
```

- Requirements

- Python 3.9.13
- See `requirements.txt` for package dependencies
