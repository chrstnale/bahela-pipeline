import os
import pytest
from airflow.models import DagBag
from airflow.utils.dates import days_ago

# Configuration for tags and retries
APPROVED_TAGS = {"etl", "data-pipeline"}  # Example tags for validation
MIN_RETRIES = 2  # Minimum retries for tasks

# Utility to suppress logging for cleaner test output
from contextlib import contextmanager
import logging

@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value

def get_dagbag():
    """Helper to load the DagBag."""
    with suppress_logging("airflow"):
        return DagBag(include_examples=False)

@pytest.fixture(scope="module")
def dag_bag():
    """Load the DAG Bag once for all tests."""
    return get_dagbag()

@pytest.fixture
def dag(dag_bag):
    """Retrieve the specific DAG."""
    dag_id = "bahela_etl_pipeline"  # Replace with your DAG's id
    dag = dag_bag.get_dag(dag_id)
    assert dag, f"DAG '{dag_id}' not found in DagBag."
    return dag

def test_dag_import_errors(dag_bag):
    """Check if the DAG imports without errors."""
    assert len(dag_bag.import_errors) == 0, f"Import errors: {dag_bag.import_errors}"

def test_dag_tags(dag):
    """Check that the DAG has valid tags."""
    assert dag.tags, f"{dag.dag_id} has no tags."
    assert set(dag.tags).issubset(APPROVED_TAGS), f"Invalid tags for {dag.dag_id}: {dag.tags}"

def test_task_retries(dag):
    """Ensure all tasks have at least the minimum retries set."""
    for task_id, task in dag.task_dict.items():
        retries = task.retries or dag.default_args.get("retries", 0)
        assert retries >= MIN_RETRIES, f"Task '{task_id}' in {dag.dag_id} has retries < {MIN_RETRIES}."

def test_task_dependencies(dag):
    """Verify task dependencies are correctly defined."""
    expected_tasks = ["extract_api_data", "transform_data", "load_to_bigquery"]
    assert set(expected_tasks).issubset(dag.task_dict.keys()), f"Missing tasks in {dag.dag_id}."

def test_bigquery_schema():
    """Validate schema against BigQuery compatibility."""
    from google.cloud.bigquery import SchemaField

    schema = [
        SchemaField("id", "STRING"),
        SchemaField("session_id", "STRING"),
        SchemaField("app_start_time", "STRING"),
        SchemaField("start_time", "TIMESTAMP"),
        SchemaField("end_time", "TIMESTAMP"),
        SchemaField("duration", "FLOAT"),
        SchemaField("lang", "STRING"),
        SchemaField("copies", "INTEGER"),
        SchemaField("filter_fn", "STRING"),
        SchemaField("transaction_amount", "INTEGER"),
        SchemaField("order_id", "STRING"),
        SchemaField("discount", "INTEGER"),
        SchemaField("expiry_time", "STRING"),
        SchemaField("use_retry", "BOOLEAN"),
        SchemaField("retry_attempts", "INTEGER"),
        SchemaField("uploaded_images", "STRING"),
        SchemaField("total_bytes", "INTEGER"),
        SchemaField("avg_bytes", "INTEGER"),
        SchemaField("total_pixels", "INTEGER"),
        SchemaField("avg_pixels", "INTEGER"),
    ]

    assert len(schema) == 20, "Mismatch in schema field count."
    for field in schema:
        assert isinstance(field, SchemaField), f"Invalid schema field: {field}."

