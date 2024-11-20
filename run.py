from pipeline import Pipeline
from database import initialize_database
from config import CONFIG

def main():
    """Main function to run the pipeline."""
    try:
        # Initialize database
        initialize_database(CONFIG['db_path'])
        
        # Run pipeline
        pipeline = Pipeline()
        pipeline.process_data()
        
    except Exception as e:
        print(f"Error running pipeline: {e}")
        raise

if __name__ == "__main__":
    main()