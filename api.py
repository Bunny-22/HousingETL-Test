from flask import Flask, request, jsonify
from flask_restful import Api, Resource
import pandas as pd
from datetime import datetime
import logging
from config import CONFIG
from pipeline import Pipeline
from dataprocessor import DataProcessor
from main import load_model, predict
from database import database_connection

app = Flask(__name__)
api = Api(app)

# Initializing logger
logger = logging.getLogger('housing_pipeline_api')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
))
logger.addHandler(handler)


class PredictSingle(Resource):
    def post(self):
        """Endpoint for single house price prediction."""
        try:
            data = request.get_json()
            if not data:
                return {"error": "No data provided"}, 400
            
            # Validate input fields
            required_fields = CONFIG['required_columns']
            missing_fields = [field for field in required_fields if field not in data]
            if missing_fields:
                return {
                    "error": f"Missing required fields: {missing_fields}"
                }, 400
            
            # Convert to DataFrame
            df = pd.DataFrame([data])
            
            # Process data
            processor = DataProcessor()
            transformed_df = processor.transform_data(df)
            
            # Load model and predict
            model = load_model(CONFIG['model_file'])
            prediction = predict(transformed_df, model)[0]
            
            # Save to database
            batch_id = int(datetime.now().timestamp())
            processor.save_to_db(df, 'raw_data')
            processor.save_to_db(transformed_df, 'transformed_data', batch_id)
            
            with database_connection(CONFIG['db_path']) as conn:
                conn.execute("""
                    INSERT INTO predictions 
                    (batch_id, predicted_price)
                    VALUES (?, ?)
                """, (batch_id, float(prediction)))
                conn.commit()
            
            return {
                "prediction": float(prediction),
                "batch_id": batch_id,
                "timestamp": datetime.now().isoformat()
            }, 200
            
        except Exception as e:
            logger.error(f"Prediction error: {str(e)}")
            return {"error": str(e)}, 500

class PredictBatch(Resource):
    def post(self):
        """Endpoint for batch predictions."""
        try:
            if 'file' not in request.files:
                return {"error": "No file provided"}, 400
            
            file = request.files['file']
            if file.filename == '':
                return {"error": "No file selected"}, 400
            
            if not file.filename.endswith('.csv'):
                return {"error": "Only CSV files are supported"}, 400
            
            # Read CSV
            df = pd.read_csv(file)
            
            # Run pipeline
            pipeline = Pipeline()
            batch_id = pipeline.batch_id
            pipeline.process_data()
            
            # Get predictions
            with database_connection(CONFIG['db_path']) as conn:
                predictions = pd.read_sql_query(
                    f"SELECT * FROM predictions WHERE batch_id = {batch_id}",
                    conn
                )
            
            return {
                "batch_id": batch_id,
                "num_predictions": len(predictions),
                "timestamp": datetime.now().isoformat()
            }, 200
            
        except Exception as e:
            logger.error(f"Batch prediction error: {str(e)}")
            return {"error": str(e)}, 500

# Add routes
api.add_resource(PredictSingle, '/predict')
api.add_resource(PredictBatch, '/predict/batch')

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)