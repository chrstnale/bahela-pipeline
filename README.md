# ETL Pipeline for Bahela  

This repository contains ETL pipelines designed to automate data extraction, transformation, and loading (ETL) tasks for Bahela's photobooth services. The pipelines process session and image data, load it into **Google BigQuery**, send daily summaries, and provide weekly analytics with predictions.

---

## Project Overview

Bahela is a photobooth service tailored for installed photobooths at cafes, providing users with an interactive and fun experience to capture memories. The service is supported by an application that tracks session activities and integrates with an image hosting platform, Cloudinary, for storing generated photos. This project focuses on two main data engineering pipelines:

1. **Daily ETL Pipeline**: Centralizes session data from Bahela's application and image metadata from Cloudinary into Google BigQuery.
2. **Weekly Analytics Pipeline**: Processes historical data to generate predictions and insights for stakeholders.

---

## Features  

### Daily ETL Pipeline (`etl_bahela.py`)
- **Automated ETL Process**: From data extraction to summary email delivery
- **Google BigQuery Integration**: Centralized data storage for analysis
- **Daily Insights**: Email reports with key metrics and attached JSON summaries

### Weekly Analytics Pipeline (`weekly_bahela_analytics.py`)
- **Historical Data Analysis**: Processes 2 weeks of historical data
- **Predictive Modeling**: Uses Random Forest to predict key metrics
- **Weekly Reports**: Comprehensive analytics with next week's predictions
- **Key Predictions**: Revenue, transactions, duration, and image generation forecasts

---

## Workflow Overview  

### Daily ETL Pipeline
1. **Data Extraction**  
   - Fetch session data from the MongoDB API
   - Fetch image metadata from the Cloudinary API

2. **Data Transformation**  
   - Combine session and image data into a unified format
   - Generate daily summary
   - Save transformed data for BigQuery

3. **Data Loading**  
   - Load transformed data into BigQuery
   - Send daily summary email

### Weekly Analytics Pipeline
1. **Historical Data Processing**
   - Extract 2 weeks of historical data
   - Aggregate data into daily metrics
   - Calculate rolling averages and trends

2. **Predictive Modeling**
   - Train Random Forest models for key metrics
   - Generate predictions for the next week
   - Analyze feature importance

3. **Weekly Reporting**
   - Summarize last week's performance
   - Present next week's predictions
   - Send comprehensive weekly report

---

## Metrics Tracked  

### Daily Metrics
- **Session Data**: ID, duration, transaction amount, etc.
- **Image Data**: Count, size, resolution
- **Daily Aggregates**: Revenue, session count, etc.

### Weekly Analytics
- **Historical Trends**
  - Daily revenue patterns
  - Transaction volume trends
  - Session duration analysis
  - Image generation statistics

- **Predicted Metrics**
  - Next week's daily revenue
  - Expected transaction volume
  - Projected session durations
  - Anticipated image generation

---

## Airflow DAG Structure  

### Daily ETL DAG (`etl_bahela.py`)
1. Extract API Data
2. Transform Data
3. Load to BigQuery
4. Send Daily Summary

### Weekly Analytics DAG (`weekly_bahela_analytics.py`)
1. Extract Historical Data (2 weeks)
2. Transform and Aggregate Data
3. Train Prediction Models
4. Generate Next Week's Predictions
5. Send Weekly Report

### DAG Dependencies
```text
Daily ETL:
Extract API Data → Transform Data → [Load to BigQuery, Prepare Email Content] → Send Email

Weekly Analytics:
Extract Historical → Transform Historical → Train Models → Generate Predictions → Send Weekly Report
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

### Additional Requirements for Weekly Analytics
- scikit-learn for predictive modeling
- Sufficient historical data (minimum 2 weeks)
- Properly configured SMTP for weekly reports

### Running the Pipelines
1. Daily ETL runs automatically every day
2. Weekly Analytics runs every Monday at midnight
3. Both can be triggered manually through Airflow UI

### Monitoring
- Check Airflow UI for task status
- Review logs for prediction accuracy
- Monitor email reports for insights

---

## Future Enhancements
- Enhanced prediction models with more features
- Interactive dashboards for predictions
- Real-time anomaly detection
- Integration with more data sources
