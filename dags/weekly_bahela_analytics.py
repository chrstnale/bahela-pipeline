from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.smtp.operators.smtp import EmailOperator
import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestRegressor
import pickle
import os
from airflow.models import Variable
from airflow.configuration import conf

# Get environment variables
MONGODB_API_URL = os.getenv('MONGODB_API_URL')
CLOUDINARY_API_URL = os.getenv('CLOUDINARY_API_URL')
CLOUDINARY_API_KEY = os.getenv('CLOUDINARY_API_KEY')
CLOUDINARY_API_SECRET = os.getenv('CLOUDINARY_API_SECRET')
EMAIL_RECIPIENTS = os.getenv('EMAIL_RECIPIENTS', 'christianale85@gmail.com').split(',')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'email': ['dreambooth.idn@gmail.com', 'christianale85@gmail.com'],
    'email_on_failure': False
}

@dag(
    default_args=default_args,
    schedule_interval='0 0 * * MON',  # Runs every Monday at midnight
    catchup=False,
    dag_id='weekly_bahela_analytics'
)
def weekly_bahela_analytics():
    
    @task()
    def extract_historical_data():
        # Calculate date ranges
        # Set today's date to a fixed date for testing
        today = datetime.strptime("2024-11-18", "%Y-%m-%d")

        # Adjust the historical data range to 2 weeks
        start_date = (today - timedelta(days=14)).strftime("%Y-%m-%d")  # 2 weeks of historical data
        end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")     # Up to the day before today

        print(f"Historical data range: {start_date} to {end_date}")
        
        # Fetch MongoDB historical data
        params = {"startDate": start_date, "endDate": end_date}
        mongodb_response = requests.get(MONGODB_API_URL, params=params)
        
        # Fetch Cloudinary data using pagination logic
        def fetch_cloudinary_data(start_date, end_date):
            all_data = []
            next_cursor = None
            while True:
                params = {
                    "expression": f"created_at:[{start_date} TO {end_date}]",
                    "max_results": 50,
                }
                if next_cursor:
                    params["next_cursor"] = next_cursor
                cloudinary_response = requests.get(
                    CLOUDINARY_API_URL,
                    auth=(CLOUDINARY_API_KEY, CLOUDINARY_API_SECRET),
                    params=params
                )
                if cloudinary_response.status_code == 200:
                    data = cloudinary_response.json()
                    all_data.extend(data.get("resources", []))
                    next_cursor = data.get("next_cursor")
                    if not next_cursor:
                        break
                else:
                    raise Exception(f"Failed to fetch Cloudinary data: {cloudinary_response.text}")
            return all_data

        cloudinary_data = fetch_cloudinary_data(start_date, end_date)
        
        # Save historical data
        historical_file_path = '/tmp/historical_data.json'
        with open(historical_file_path, 'w') as f:
            json.dump({
                'mongodb_data': mongodb_response.json(),
                'cloudinary_data': cloudinary_data,
                'date_range': {'start': start_date, 'end': end_date}
            }, f)
        
        return historical_file_path

    @task()
    def transform_historical_data(file_path: str):
        # Load historical data
        print(f"[INFO] Loading historical data from {file_path}")
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # MongoDB and Cloudinary data loading
        mongodb_df = pd.DataFrame(data['mongodb_data'].get("appStates", []))
        cloudinary_df = pd.DataFrame(data['cloudinary_data'])

        # Log MongoDB and Cloudinary data loading
        print(f"[INFO] MongoDB data loaded with {len(mongodb_df)} records.")
        print(f"[INFO] Cloudinary data loaded with {len(cloudinary_df)} records.")

        # Check for null values in MongoDB DataFrame
        print(f"[DEBUG] Null values in MongoDB DataFrame:\n{mongodb_df.isnull().sum()}")
        print(f"[DEBUG] MongoDB DataFrame types:\n{mongodb_df.dtypes}")
        
        # Check for null values in Cloudinary DataFrame
        print(f"[DEBUG] Null values in Cloudinary DataFrame:\n{cloudinary_df.isnull().sum()}")
        print(f"[DEBUG] Cloudinary DataFrame types:\n{cloudinary_df.dtypes}")

        # Filtering MongoDB data
        print("[INFO] Filtering MongoDB data for 'Result' page and paid payments.")
        filtered_mongodb = mongodb_df[
            (mongodb_df["page"] == "Result") & 
            (mongodb_df["payment"].apply(lambda x: x.get("paid", False)))
        ].copy()
        print(f"[INFO] Filtered MongoDB data contains {len(filtered_mongodb)} records.")

        # Extract relevant progress data
        filtered_mongodb = filtered_mongodb.assign(
            app_start_time=filtered_mongodb["progress"].apply(lambda x: x.get("appStartTime")),
            start_time=pd.to_datetime(filtered_mongodb["progress"].apply(lambda x: x.get("startTime")), errors='coerce'),
            end_time=pd.to_datetime(filtered_mongodb["progress"].apply(lambda x: x.get("endTime")), errors='coerce'),
            duration=lambda df: (df["end_time"] - df["start_time"]).dt.total_seconds(),
            lang=lambda df: df["options"].apply(lambda x: x.get("lang")),
            filter_fn=lambda df: df["options"].apply(lambda x: x.get("filterFn")),
            copies=lambda df: df["options"].apply(lambda x: x.get("copies")),
            use_retry=lambda df: df["options"].apply(lambda x: x.get("useRetryMode", False)),
            retry_count=lambda df: df["options"].apply(lambda x: x.get("retryCount", 0) if x.get("useRetryMode", False) else 0),
            amount=lambda df: df["payment"].apply(lambda x: x.get("amount")),
            order_id=lambda df: df["payment"].apply(lambda x: x.get("orderId")),
            discount=lambda df: df["payment"].apply(lambda x: x.get("discount", 0)),
            expiry_time=lambda df: df["payment"].apply(lambda x: x.get("expiryTime")),
            uploaded_images=filtered_mongodb["result"].apply(lambda x: x.get("uploadedImageUrls", []))
        )

        # Cloudinary data processing
        print("[INFO] Processing Cloudinary data.")
        cloudinary_df['session_id'] = cloudinary_df['public_id'].apply(lambda x: x[:6])
        cloudinary_df['public_id'] = cloudinary_df['public_id'].fillna('unknown')
        cloudinary_df['bytes'] = cloudinary_df['bytes'].fillna(0)

        # Aggregate Cloudinary data by `session_id`
        print("[INFO] Aggregating Cloudinary data.")
        cloudinary_aggregated = cloudinary_df.groupby('session_id').agg(
            total_images=('public_id', 'count'),
            total_bytes=('bytes', 'sum'),
            avg_bytes=('bytes', 'mean'),
            total_pixels=('pixels', 'sum'),
            avg_pixels=('pixels', 'mean')
        ).reset_index()
        print(f"[INFO] Cloudinary aggregation completed with {len(cloudinary_aggregated)} records.")

        # Join filtered MongoDB and aggregated Cloudinary data
        print("[INFO] Merging MongoDB and Cloudinary data.")
        final_data = filtered_mongodb.merge(
            cloudinary_aggregated,
            left_on='id', 
            right_on='session_id', 
            how='left'
        )

        # Fill NaN values in final_data
        print("[INFO] Filling missing values in merged data.")
        final_data = final_data.fillna({
            'total_images': 0,
            'total_bytes': 0,
            'avg_bytes': 0,
            'total_pixels': 0,
            'avg_pixels': 0
        })

        # Check final_data for nulls and data types
        print(f"[DEBUG] Null values in final DataFrame:\n{final_data.isnull().sum()}")
        print(f"[DEBUG] Final DataFrame types:\n{final_data.dtypes}")

        # Ensure uploaded_images is serialized to JSON
        print("[INFO] Serializing uploaded_images to JSON strings.")
        final_data["uploaded_images"] = final_data["uploaded_images"].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)

                # Rename and select the final columns as per BigQuery schema
        final_data = final_data[[
            "_id", "id", "app_start_time", "start_time", "end_time", "duration", "lang", "copies",
            "filter_fn", "amount", "order_id", "discount", "expiry_time", "use_retry", "retry_count", 
            "uploaded_images", "total_images", "total_bytes", "avg_bytes", "total_pixels", "avg_pixels"
        ]].rename(columns={
            "_id": "id", 
            "id": "session_id", 
            "amount": "transaction_amount",
            "retry_count": "retry_attempts"
        })

        # Ensure the data types are correct before proceeding
        final_data['duration'] = final_data['duration'].astype(float)
        final_data['total_images'] = final_data['total_images'].astype(float)
        final_data['total_bytes'] = final_data['total_bytes'].astype(float)
        final_data['avg_bytes'] = final_data['avg_bytes'].astype(float)
        final_data['total_pixels'] = final_data['total_pixels'].astype(float)
        final_data['avg_pixels'] = final_data['avg_pixels'].astype(float)

        # Print summary information to debug
        print("Final Data", final_data.describe())
        print("Info Final Data", final_data.head())

        print("[INFO] Creating daily aggregated data for prediction model.")
        
        # Convert timestamps to dates and ensure they're in UTC
        final_data['date'] = final_data['start_time'].dt.date
        
        # Create daily aggregated metrics
        daily_metrics = final_data.groupby('date').agg({
            'transaction_amount': ['sum', 'count', 'mean'],
            'duration': ['sum', 'mean', 'count'],
            'copies': ['sum', 'mean'],
            'total_images': ['sum', 'mean'],
            'total_bytes': ['sum', 'mean'],
            'total_pixels': ['sum', 'mean'],
            'discount': ['sum', 'mean']
        }).reset_index()

        # Flatten column names
        daily_metrics.columns = [
            f"{col[0]}_{col[1]}" if col[1] else col[0] 
            for col in daily_metrics.columns
        ]

        # Rename some columns for clarity
        daily_metrics = daily_metrics.rename(columns={
            'transaction_amount_sum': 'daily_revenue',
            'transaction_amount_count': 'total_transactions',
            'transaction_amount_mean': 'avg_transaction_amount',
            'duration_sum': 'total_duration',
            'duration_mean': 'avg_duration',
            'duration_count': 'total_sessions',
            'copies_sum': 'total_copies',
            'copies_mean': 'avg_copies',
            'total_images_sum': 'daily_images',
            'total_images_mean': 'avg_images_per_session',
            'total_bytes_sum': 'daily_bytes',
            'total_bytes_mean': 'avg_bytes_per_session',
            'total_pixels_sum': 'daily_pixels',
            'total_pixels_mean': 'avg_pixels_per_session',
            'discount_sum': 'total_discounts',
            'discount_mean': 'avg_discount'
        })

        # Add time-based features
        daily_metrics['date'] = pd.to_datetime(daily_metrics['date'])
        daily_metrics['day_of_week'] = daily_metrics['date'].dt.dayofweek
        daily_metrics['month'] = daily_metrics['date'].dt.month
        daily_metrics['day'] = daily_metrics['date'].dt.day
        daily_metrics['is_weekend'] = daily_metrics['day_of_week'].isin([5, 6]).astype(int)

        # Calculate rolling averages for key metrics (7-day window)
        for column in ['daily_revenue', 'total_transactions', 'total_duration', 'daily_images']:
            daily_metrics[f'{column}_7day_avg'] = daily_metrics[column].rolling(window=7, min_periods=1).mean()

        # Log the final structure
        print(f"[INFO] Daily metrics shape: {daily_metrics.shape}")
        print(f"[DEBUG] Daily metrics data types:\n{daily_metrics.dtypes}")
        print(f"[DEBUG] Sample of daily metrics:\n{daily_metrics.head()}")
        print(f"[DEBUG] Null values in daily metrics:\n{daily_metrics.isnull().sum()}")

        # Save transformed data
        transformed_path = '/tmp/transformed_historical_data.json'
        print(f"[INFO] Saving daily metrics to {transformed_path}")
        daily_metrics.to_json(transformed_path, orient='records', date_format='iso')

        return transformed_path
    
    @task()
    def train_prediction_model(transformed_path: str):
        print(f"[INFO] Loading transformed data from: {transformed_path}")
        
        try:
            daily_data = pd.read_json(transformed_path)
            print(f"[INFO] Data loaded successfully with {len(daily_data)} records.")
        except Exception as e:
            print(f"[ERROR] Failed to load data: {e}")
            raise

        # Define features and targets
        features = [
            'day_of_week', 'month', 'day', 'is_weekend',
            'daily_revenue_7day_avg', 'total_transactions_7day_avg',
            'total_duration_7day_avg', 'daily_images_7day_avg'
        ]
        
        targets = [
            'daily_revenue', 'total_transactions', 'total_duration',
            'daily_images', 'avg_transaction_amount'
        ]

        # Validate features and targets
        missing_columns = [col for col in features + targets if col not in daily_data.columns]
        if missing_columns:
            raise KeyError(f"[ERROR] Missing columns: {missing_columns}")

        print(f"[INFO] Using features: {features}")
        print(f"[INFO] Predicting targets: {targets}")

        # Train models
        models = {}
        feature_importance = {}
        
        for target in targets:
            print(f"[INFO] Training model for: {target}")
            try:
                X = daily_data[features]
                y = daily_data[target]

                # Handle any remaining NaN values
                X = X.fillna(X.mean())
                y = y.fillna(y.mean())

                model = RandomForestRegressor(
                    n_estimators=100,
                    random_state=42,
                    n_jobs=-1  # Use all available cores
                )
                model.fit(X, y)
                
                # Store model and feature importance
                models[target] = model
                feature_importance[target] = dict(zip(features, model.feature_importances_))
                
                print(f"[INFO] Top features for {target}:")
                for feat, imp in sorted(feature_importance[target].items(), key=lambda x: x[1], reverse=True)[:3]:
                    print(f"  - {feat}: {imp:.4f}")
                    
            except Exception as e:
                print(f"[ERROR] Failed to train model for {target}: {e}")
                raise

        # Save models and feature importance
        model_path = '/tmp/prediction_models.pkl'
        print(f"[INFO] Saving models to: {model_path}")
        
        try:
            with open(model_path, 'wb') as f:
                pickle.dump({
                    'models': models,
                    'feature_importance': feature_importance,
                    'features': features,
                    'targets': targets
                }, f)
            print("[INFO] Models saved successfully")
        except Exception as e:
            print(f"[ERROR] Failed to save models: {e}")
            raise

        return model_path
        
    @task()
    def generate_predictions(model_path: str):
        print("[INFO] Loading prediction models")
        try:
            with open(model_path, 'rb') as f:
                model_data = pickle.load(f)
                models = model_data['models']
                features = model_data['features']
                targets = model_data['targets']
            print("[INFO] Models loaded successfully")
        except Exception as e:
            print(f"[ERROR] Failed to load models: {e}")
            raise

        # Generate dates for next week
        today = datetime.utcnow()
        next_week_dates = [(today + timedelta(days=i)) for i in range(7)]
        
        # Create prediction DataFrame with all required features
        pred_data = pd.DataFrame({
            'date': next_week_dates,
            'day_of_week': [d.weekday() for d in next_week_dates],
            'month': [d.month for d in next_week_dates],
            'day': [d.day for d in next_week_dates],
            'is_weekend': [1 if d.weekday() in [5, 6] else 0 for d in next_week_dates]
        })

        # Initialize rolling average features with zeros (as we don't have recent data)
        for avg_feature in [f for f in features if '7day_avg' in f]:
            pred_data[avg_feature] = 0  # You might want to use historical averages instead

        # Ensure all required features are present
        missing_features = [f for f in features if f not in pred_data.columns]
        if missing_features:
            raise KeyError(f"[ERROR] Missing features for prediction: {missing_features}")

        print("[DEBUG] Prediction features:")
        print(pred_data.head())
        print("\n[DEBUG] Feature dtypes:")
        print(pred_data.dtypes)

        # Generate predictions for each target
        predictions = {}
        for target in targets:
            print(f"[INFO] Generating predictions for {target}")
            try:
                X_pred = pred_data[features]
                predictions[target] = models[target].predict(X_pred)
                print(f"[DEBUG] {target} predictions shape: {predictions[target].shape}")
            except Exception as e:
                print(f"[ERROR] Failed to generate predictions for {target}: {e}")
                raise

        # Create predictions DataFrame
        predictions_df = pd.DataFrame({
            'date': next_week_dates,
            **{f'predicted_{target}': predictions[target] for target in targets}
        })

        # Format predictions
        predictions_df['date'] = pd.to_datetime(predictions_df['date'])
        
        print("[INFO] Prediction summary:")
        print(predictions_df.describe())
        print("\n[DEBUG] Predictions head:")
        print(predictions_df.head())

        # Save predictions
        predictions_path = '/tmp/next_week_predictions.json'
        predictions_df.to_json(predictions_path, orient='records', date_format='iso')
        print(f"[INFO] Predictions saved to {predictions_path}")
        
        return predictions_path

    @task()
    def prepare_weekly_report(transformed_path: str, predictions_path: str):
        print("[INFO] Preparing weekly report")
        
        try:
            # Load historical data and predictions
            historical_data = pd.read_json(transformed_path)
            predictions_df = pd.read_json(predictions_path)
            
            # Convert dates to datetime
            historical_data['date'] = pd.to_datetime(historical_data['date'])
            predictions_df['date'] = pd.to_datetime(predictions_df['date'])
            
            # Get last week's data
            last_week_data = historical_data.sort_values('date').tail(7)
            
            # Format currency values
            currency_cols = ['daily_revenue', 'avg_transaction_amount', 'total_discounts']
            
            # Function to format numbers
            def format_number(x):
                if pd.isna(x):
                    return 'N/A'
                if isinstance(x, (int, float)):
                    if any(col in str(x) for col in currency_cols):
                        return f"Rp {x:,.2f}"
                    return f"{x:,.2f}"
                return str(x)

            # Create email content
            email_content = """
            <h2>Weekly Bahela Analytics Report</h2>
            
            <h3>Last Week's Performance Summary</h3>
            <table>
                <tr>
                    <th>Metric</th>
                    <th>Total</th>
                    <th>Daily Average</th>
                </tr>
            """
            
            # Add summary metrics
            metrics = {
                'Revenue': ('daily_revenue', 'Rp'),
                'Transactions': ('total_transactions', ''),
                'Images Generated': ('daily_images', ''),
                'Session Duration': ('total_duration', ' seconds'),
                'Discounts Given': ('total_discounts', 'Rp')
            }
            
            for metric_name, (col, unit) in metrics.items():
                if col in last_week_data.columns:
                    total = last_week_data[col].sum()
                    avg = last_week_data[col].mean()
                    email_content += f"""
                    <tr>
                        <td>{metric_name}</td>
                        <td>{unit}{format_number(total)}</td>
                        <td>{unit}{format_number(avg)}</td>
                    </tr>
                    """

            email_content += """
            </table>
            
            <h3>Next Week's Predictions</h3>
            <table>
                <tr>
                    <th>Date</th>
                    <th>Predicted Revenue</th>
                    <th>Predicted Transactions</th>
                    <th>Predicted Images</th>
                </tr>
            """
            
            # Add predictions
            for _, row in predictions_df.iterrows():
                email_content += f"""
                <tr>
                    <td>{row['date'].strftime('%Y-%m-%d')}</td>
                    <td>Rp {format_number(row['predicted_daily_revenue'])}</td>
                    <td>{format_number(row['predicted_total_transactions'])}</td>
                    <td>{format_number(row['predicted_daily_images'])}</td>
                </tr>
                """
            
            email_content += "</table>"
            
            print("[INFO] Weekly report prepared successfully")
            return email_content
            
        except Exception as e:
            print(f"[ERROR] Failed to prepare weekly report: {e}")
            raise

    # Define task flow
    historical_data = extract_historical_data()
    transformed_data = transform_historical_data(historical_data)
    prediction_model = train_prediction_model(transformed_data)
    next_week_predictions = generate_predictions(prediction_model)
    weekly_report = prepare_weekly_report(transformed_data, next_week_predictions)

    # Create email task
    email_task = EmailOperator(
        task_id='send_weekly_report',
        to=EMAIL_RECIPIENTS,
        subject='Bahela Weekly Analytics Report',
        html_content=weekly_report,
        files=[next_week_predictions],
        conn_id='smtp_default'
    )

    # Set dependencies
    historical_data >> transformed_data >> prediction_model >> next_week_predictions >> weekly_report >> email_task

# Instantiate the DAG
weekly_analytics_dag = weekly_bahela_analytics()