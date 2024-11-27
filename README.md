# ETL Pipeline for Bahela  

This repository contains an ETL pipeline designed to automate data extraction, transformation, and loading (ETL) tasks for Bahela's photobooth services. The pipeline processes session and image data, loads it into **Google BigQuery**, and emails daily summaries to stakeholders.  

---

## Project Overview

Bahela is a photobooth service tailored for installed photobooths at cafes, providing users with an interactive and fun experience to capture memories. The service is supported by an application that tracks session activities and integrates with an image hosting platform, Cloudinary, for storing generated photos. This project focuses on creating a data engineering pipeline that centralizes session data from Bahela's application and image metadata from Cloudinary into Google BigQuery.

The goal is to transform this data into actionable insights that stakeholders can access through a daily email summary. The data loaded into BigQuery can be used for reporting, analytics, and further data processing. The pipeline is built using Apache Airflow, leveraging its powerful scheduling, task dependencies, and data orchestration capabilities.

---

## Features  

- **Automated ETL Process**: From data extraction to summary email delivery.  
- **Google BigQuery Integration**: Centralized data storage for analysis.  
- **Daily Insights**: Email reports with key metrics and attached JSON summaries.  

---

## Workflow Overview  

1. **Data Extraction**  
   - Fetch session data from the MongoDB API.  
   - Fetch image metadata from the Cloudinary API.  

2. **Data Transformation**  
   - Combine session and image data into a unified format.  
   - Generate a daily summary by aggregating key metrics, such as revenue, session durations, and image statistics.  
   - Save transformed data and the summary to local storage as JSON for subsequent steps.  

3. **Data Loading**  
   - Load the transformed data into **Google BigQuery**, following a predefined schema to ensure consistency and accuracy.  
   - Append new data daily to keep the dataset up-to-date for analysis.  

4. **Email Delivery**  
   - Create a detailed HTML email summarizing the day’s insights, such as total transactions, revenue, and image statistics.  
   - Attach the JSON summary for stakeholders who prefer raw data.  
   - Send the email automatically using **Airflow’s EmailOperator**.  

---

## Metrics Tracked  

### Session Data (from MongoDB API)  
- **Session ID**: Unique identifier for each session.  
- **Duration**: Total time spent in the session.  
- **Transaction Amount**: Total revenue generated.  
- **Retry Attempts**: Number of retries for failed operations.  
- **Copies Printed**: Total prints requested during a session.  

### Image Data (from Cloudinary API)  
- **Total Images**: Number of images uploaded per session.  
- **Total Bytes**: Combined size of all uploaded images.  
- **Average Resolution**: Average size and quality of uploaded images.  

### Daily Aggregates  
- **Total Revenue**: Sum of all transaction amounts.  
- **Total Sessions**: Count of sessions processed.  
- **Average Session Duration**: Mean time spent per session.  
- **Image Metrics**: Aggregated and average image sizes and resolutions.  

---

## Airflow DAG Structure  

### Tasks  
1. **Extract API Data**  
   - Fetch session data from MongoDB.  
   - Fetch image metadata from Cloudinary.  

2. **Transform Data**  
   - Process and clean the data.  
   - Merge session data with image data.  
   - Generate a daily summary.  

3. **Load to BigQuery**  
   - Upload transformed data into BigQuery under the specified dataset and table.  

4. **Prepare Email Content**  
   - Format the daily summary into an HTML table.  
   - Include key metrics and attach the JSON summary.  

5. **Send Email**  
   - Automatically deliver the email to stakeholders.  

### DAG Dependencies  
```text
Extract API Data → Transform Data → [Load to BigQuery, Prepare Email Content] → Send Email
```

---

## Installation and Usage
### Prerequisites
1. Airflow Setup
   - Ensure Airflow is installed and configured.
   - Add necessary connections for MongoDB, Cloudinary, BigQuery, and SMTP.
2. Google Cloud Setup
   - Create a BigQuery Dataset and table with the required schema.
   - Authenticate with a Google Cloud service account.
3. Cloudinary Account
   - Configure API keys for accessing image metadata.

### Running the Pipeline
1. Deploy the provided DAG to your Airflow instance.
2. Ensure all dependencies are satisfied.
3. Trigger the DAG manually or let it run on its daily schedule.
4. Monitor task statuses in the Airflow web UI.

### Future Enhancements
- Error Handling: Add advanced retry mechanisms for failed API calls.
- Visualization Dashboard: Create interactive dashboards using BigQuery and Data Studio.
- Scalability: Optimize the pipeline for larger datasets and additional data sources.
