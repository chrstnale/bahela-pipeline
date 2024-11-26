from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import requests
import json
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(10),
    'retries': 0,
}

@dag(default_args=default_args, schedule_interval='@daily', catchup=False)
def bahela_etl_pipeline():
   
    @task()
    def extract_api_data():
        # Calculate yesterday's date
        yesterday = (datetime.utcnow() - timedelta(days=10)).strftime("%Y-%m-%d")
        
        # Fetch MongoDB data
        mongodb_url = "https://bahela-admin.vercel.app/api/sessions"
        params = {"startDate": yesterday, "endDate": yesterday}
        mongodb_response = requests.get(mongodb_url, params=params)
        if mongodb_response.status_code != 200:
            raise Exception(f"MongoDB API request failed: {mongodb_response.text}")

        # Fetch Cloudinary data using pagination logic
        def fetch_cloudinary_data(start_date, end_date):
            all_data = []
            next_cursor = None
            while True:
                # Parameters for filtering by creation date
                params = {
                    "expression": f"created_at:[{start_date} TO {end_date}]",
                    "max_results": 50,
                }
                if next_cursor:
                    params["next_cursor"] = next_cursor
                response = requests.get(
                    "https://api.cloudinary.com/v1_1/dqyf4lrlo/resources/search",
                    auth=("214285446753493", "_Lwj_-7ml1KqQAe4Oadpaxoqhf4"),
                    params=params,
                )
                if response.status_code == 200:
                    data = response.json()
                    all_data.extend(data.get("resources", []))
                    next_cursor = data.get("next_cursor")
                    if not next_cursor:  # Break if no more pages
                        break
                else:
                    raise Exception(f"Failed to fetch Cloudinary data: {response.text}")
            return all_data

        # Fetch Cloudinary data for yesterday's date
        cloudinary_data = fetch_cloudinary_data(yesterday, yesterday)

        # Save both responses to a local file
        local_file_path = '/tmp/api_data.json'
        with open(local_file_path, 'w') as f:
            json.dump({
                'mongodb_data': mongodb_response.json(),
                'cloudinary_data': cloudinary_data,
                'date': yesterday
            }, f)

        # Log data for debugging
        print("MongoDB API Data:", mongodb_response.json())  # MongoDB data log
        print("Cloudinary API Data:", cloudinary_data)       # Cloudinary data log
            
        return local_file_path

    
    @task()
    def transform_data(file_path: str):
        with open(file_path, 'r') as f:
            data = json.load(f)

        # MongoDB and Cloudinary data loading
        mongodb_df = pd.DataFrame(data['mongodb_data'].get("appStates", []))
        cloudinary_df = pd.DataFrame(data['cloudinary_data'])
        
        # Filtering MongoDB data
        filtered_mongodb = mongodb_df[
            (mongodb_df["page"] == "Result") &
            (mongodb_df["payment"].apply(lambda x: x.get("paid", False)))
        ].copy() 
        
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

        # Trim session_id (first 5 characters from Cloudinary `public_id`)
        cloudinary_df['session_id'] = cloudinary_df['public_id'].apply(lambda x: x[:6])

        # Aggregate Cloudinary data by `session_id`
        cloudinary_aggregated = cloudinary_df.groupby('session_id').agg(
            total_images=('public_id', 'count'),
            total_bytes=('bytes', 'sum'),
            avg_bytes=('bytes', 'mean'),
            total_pixels=('pixels', 'sum'),
            avg_pixels=('pixels', 'mean')
        ).reset_index()  # Reset index to make session_id a column

        print(cloudinary_aggregated.head())

        # Direct join between filtered_mongodb and cloudinary_aggregated
        final_data = filtered_mongodb.merge(
            cloudinary_aggregated,
            left_on='id',  # MongoDB session ID
            right_on='session_id',  # Cloudinary session ID
            how='left'  # Keep all MongoDB records, even if no Cloudinary match
        )

        # Fill NaN values from the join with appropriate defaults
        final_data = final_data.fillna({
            'total_images': 0,
            'total_bytes': 0,
            'avg_bytes': 0,
            'total_pixels': 0,
            'avg_pixels': 0
        })

        # Ensure uploaded_images is properly serialized to JSON string (BigQuery expects STRING for this)
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

        # Save data
        joined_file_path = '/tmp/joined_data.json'
        final_data.to_json(joined_file_path, orient='records', lines=True, date_format='iso')

        # Create and save daily summary (if needed)
        daily_summary = create_daily_summary(final_data)
        summary_file_path = '/tmp/daily_summary.json'
        daily_summary.to_json(summary_file_path, orient='records', lines=True, date_format='iso')

        return joined_file_path

    @task()
    def create_daily_summary(df):
        """Create daily summary from the joined data"""
        df['date'] = df['start_time'].dt.date
        
        summary = df.groupby('date').agg(
            transaction_amount=('transaction_amount', 'sum'),
            total_discount=('discount', 'sum'),
            total_transactions=('transaction_amount', 'count'),
            total_duration=('duration', 'sum'),
            avg_duration=('duration', 'mean'),
            total_copies=('copies', 'sum'),
            avg_copies=('copies', 'mean'),
            total_bytes=('total_bytes', 'sum'),
            avg_bytes=('avg_bytes', 'mean'),
            total_pixels=('total_pixels', 'sum'),
            avg_pixels=('avg_pixels', 'mean')
        ).reset_index()
        
        return summary

    @task()
    def load_to_bigquery(file_path: str):
        project_id = "moonlit-outlet-442401-d7"
        dataset_id = "etl_bahela"
        table_id = "joined_data_1"

        client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        # Define schema based on `joined_data` log
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=[
                bigquery.SchemaField("id", "STRING"),
                bigquery.SchemaField("session_id", "STRING"),
                bigquery.SchemaField("app_start_time", "STRING"),
                bigquery.SchemaField("start_time", "TIMESTAMP"),
                bigquery.SchemaField("end_time", "TIMESTAMP"),
                bigquery.SchemaField("duration", "FLOAT"),
                bigquery.SchemaField("lang", "STRING"),
                bigquery.SchemaField("copies", "INTEGER"),
                bigquery.SchemaField("filter_fn", "STRING"),
                bigquery.SchemaField("transaction_amount", "INTEGER"),
                bigquery.SchemaField("order_id", "STRING"),
                bigquery.SchemaField("discount", "INTEGER"),
                bigquery.SchemaField("expiry_time", "STRING"),
                bigquery.SchemaField("use_retry", "BOOLEAN"),
                bigquery.SchemaField("retry_attempts", "INTEGER"),
                bigquery.SchemaField("uploaded_images", "STRING"),
                bigquery.SchemaField("total_images", "FLOAT"),
                bigquery.SchemaField("total_bytes", "FLOAT"),
                bigquery.SchemaField("avg_bytes", "FLOAT"),
                bigquery.SchemaField("total_pixels", "FLOAT"),
                bigquery.SchemaField("avg_pixels", "FLOAT"),
            ],
            write_disposition="WRITE_APPEND",
        )

        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(
                source_file,
                table_ref,
                job_config=job_config
            )

        job.result()  # Wait for the load job to complete
        return f"Data loaded to BigQuery: {table_ref}"

    # Define task dependencies
    api_data = extract_api_data()
    transformed_data = transform_data(api_data)
    load_to_bigquery(transformed_data)

# Instantiate the DAG
bahela_etl_dag = bahela_etl_pipeline()