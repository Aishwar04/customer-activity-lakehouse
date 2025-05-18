# flows.py

from prefect import flow, serve
from prefect.client.schemas.schedules import CronSchedule
from typing import Dict, Any
import os

# Import functions from your data_processing file (no @task decorator)
from data_processing import (
    get_s3_filesystem,
    read_csv_with_dask,
    transform_dask_dataframe as transform_raw_csv,
    write_dask_dataframe_to_minio as write_raw_parquet,
    read_parquet_from_minio,
    transform_raw_to_curated, 
    write_parquet_to_minio,
    generate_products_task, 
    generate_customers_task,
    transform_raw_customer_to_curated,
    transform_raw_product_to_curated
)

import dask.dataframe as dd
import s3fs

# ---- Child flows (reusable ingestion flows) ----

@flow(name="customer_transactions_ingestion_flow")
def customer_transactions_ingestion_flow(
    csv_filepath: str,
    minio_config: Dict[str, Any],
    minio_bucket: str,
    minio_output_prefix: str = "customer_transactions_raw_parquet"
) :
    """
    Flow to ingest a large CSV file using Dask and write to MinIO.
    Calls functions from data_processing.py.
    """
    print(f"Starting ingestion flow for {csv_filepath}")
    # Call functions directly as they are not Prefect tasks
    s3_filesystem = get_s3_filesystem(minio_config=minio_config)
    dask_df = read_csv_with_dask(csv_filepath=csv_filepath)
    transformed_ddf = transform_raw_csv(ddf=dask_df)
    write_raw_parquet(
        ddf=transformed_ddf,
        s3_filesystem=s3_filesystem,
        bucket_name=minio_bucket,
        prefix=minio_output_prefix
    )
    print("Ingestion flow completed.")

@flow(name="raw_to_curated_flow")
def raw_to_curated_flow(
    minio_config: Dict[str, Any],
    raw_bucket: str,
    raw_prefix: str,
    curated_bucket: str,
    curated_prefix: str,
    curated_partition_cols: list
):
    """
    Flow to transform raw Parquet data to curated Parquet data.
    Calls functions from data_processing.py.
    """
    print("Starting raw to curated flow...")
    # Call functions directly as they are not Prefect tasks
    s3_filesystem = get_s3_filesystem(minio_config=minio_config)
    raw_ddf = read_parquet_from_minio(
        s3_filesystem=s3_filesystem,
        bucket_name=raw_bucket,
        prefix=raw_prefix
    )
    curated_ddf = transform_raw_to_curated(ddf=raw_ddf)
    # Use write_parquet_to_minio for writing to the curated layer
    write_parquet_to_minio(
        ddf=curated_ddf,
        s3_filesystem=s3_filesystem,
        bucket_name=curated_bucket,
        prefix=curated_prefix,
        partition_cols=curated_partition_cols
    )
    print("Raw to curated flow completed.")

# --- Child flows for Dimension Ingestion ---

@flow(name="product_dimension_ingestion_flow")
def product_dimension_ingestion_flow(
    minio_config: Dict[str, Any],
    raw_bucket: str,
    raw_prefix: str = "product_data_raw_parquet", # Prefix for raw product data
    num_products: int = 900, # Number of products to generate (matches your script's range size)
    product_id_prefix: str = 'PROD',
    product_id_range: range = range(100, 1000)
):
    """
    Flow to generate mock product data and ingest it into the raw bucket.
    Calls functions from data_processing.py.
    """
    print("Starting Product Dimension Ingestion Flow...")

    # Define a temporary local file path for the generated CSV
    generated_csv_path = os.path.join(os.getcwd(), 'generated_product_data.csv')

    # 1. Generate the product data CSV by calling the function
    # The function returns the path to the generated file
    generated_file_path = generate_products_task(
        num_products=num_products,
        output_file=generated_csv_path,
        product_id_prefix=product_id_prefix,
        product_id_range=product_id_range
    )

    # 2. Get the S3 filesystem object for MinIO by calling the function
    s3_filesystem = get_s3_filesystem(minio_config=minio_config)

    # 3. Read the generated CSV into a Dask DataFrame by calling the function
    print(f"Reading generated CSV into Dask DataFrame: {generated_file_path}")
    try:
        # Define dtypes specifically for product data if known
        product_dtypes = {
            'product_id': 'object', 'product_name': 'object',
            'product_category': 'object', 'product_brand': 'object',
            'product_weight_kg': 'float64'
        }
        product_ddf = dd.read_csv(generated_file_path, dtype=product_dtypes)
        print("Dask DataFrame created from generated product CSV.")
    except Exception as e:
        print(f"Error reading generated product CSV: {e}")
        # Re-raise to fail the flow if reading fails
        raise


    # 4. Write the Dask DataFrame to MinIO as Parquet in the raw bucket by calling the function
    write_parquet_to_minio(
        ddf=product_ddf,
        s3_filesystem=s3_filesystem,
        bucket_name=raw_bucket,
        prefix=raw_prefix,
        partition_cols=[] # No partitioning for raw dimension data typically
    )

    # Optional: Clean up the local generated CSV file after writing to MinIO
    try:
        os.remove(generated_csv_path)
        print(f"Cleaned up local file: {generated_csv_path}")
    except OSError as e:
        print(f"Error removing local file {generated_csv_path}: {e}")


    print("Product Dimension Ingestion Flow completed.")

@flow(name="customer_dimension_ingestion_flow")
def customer_dimension_ingestion_flow(
    minio_config: Dict[str, Any],
    raw_bucket: str,
    raw_prefix: str = "customer_data_raw_parquet",
    num_customers: int = 10000,
    customer_id_range: range = range(1000, 50001)
):
    """
    Flow to generate mock customer data and ingest it into the raw bucket.
    Calls functions from data_processing.py.
    """
    print("Starting Customer Dimension Ingestion Flow...")

    # Define a temporary local file path for the generated CSV
    generated_csv_path = os.path.join(os.getcwd(), 'generated_customer_data.csv')

    # 1. Generate the customer data CSV by calling the function
    generated_file_path = generate_customers_task(
        num_customers=num_customers,
        output_file=generated_csv_path,
        customer_id_range=customer_id_range
    )

    # 2. Get the S3 filesystem object for MinIO by calling the function
    s3_filesystem = get_s3_filesystem(minio_config=minio_config)

    # 3. Read the generated CSV into a Dask DataFrame by calling the function
    print(f"Reading generated CSV into Dask DataFrame: {generated_file_path}")
    try:
        customer_dtypes = {
            'customer_id': 'int64', # Assuming int64 based on your transaction generation
            'customer_name': 'object',
            'customer_email': 'object',
            'customer_city': 'object',
            'customer_country': 'object',
            'registration_date': 'object', # Read as object, can convert later if needed
            'customer_segment': 'object'
        }
        customer_ddf = dd.read_csv(generated_file_path, dtype=customer_dtypes)
        print("Dask DataFrame created from generated customer CSV.")
    except Exception as e:
        print(f"Error reading generated customer CSV: {e}")
        # Re-raise to fail the flow if reading fails
        raise

    # 4. Write the Dask DataFrame to MinIO as Parquet in the raw bucket by calling the function
    write_parquet_to_minio(
        ddf=customer_ddf,
        s3_filesystem=s3_filesystem,
        bucket_name=raw_bucket,
        prefix=raw_prefix,
        partition_cols=[] # No partitioning for raw dimension data typically
    )

    # Optional: Clean up the local generated CSV file after writing to MinIO
    try:
        os.remove(generated_csv_path)
        print(f"Cleaned up local file: {generated_csv_path}")
    except OSError as e:
        print(f"Error removing local file {generated_csv_path}: {e}")


    print("Customer Dimension Ingestion Flow completed.")


# ---- Child flows for Raw to Curated Dimension Transformation ----

@flow(name="raw_customer_to_curated_flow")
def raw_customer_to_curated_flow(
    minio_config: Dict[str, Any],
    raw_bucket: str,
    raw_prefix: str, # Prefix for raw customer data
    curated_bucket: str,
    curated_prefix: str = "dim_customer", # Prefix for curated customer dimension
    curated_partition_cols: list = [] # Dimensions often not partitioned or simple key
):
    """
    Flow to transform raw customer data to curated customer dimension.
    Calls functions from data_processing.py.
    """
    print("Starting raw customer to curated flow...")
    s3_filesystem = get_s3_filesystem(minio_config=minio_config)
    raw_ddf = read_parquet_from_minio(
        s3_filesystem=s3_filesystem,
        bucket_name=raw_bucket,
        prefix=raw_prefix
    )
    # Use the dedicated transformation function for customer data
    curated_ddf = transform_raw_customer_to_curated(ddf=raw_ddf)
    write_parquet_to_minio(
        ddf=curated_ddf,
        s3_filesystem=s3_filesystem,
        bucket_name=curated_bucket,
        prefix=curated_prefix,
        partition_cols=curated_partition_cols # Use specified partitioning (likely empty)
    )
    print("Raw customer to curated flow completed.")

@flow(name="raw_product_to_curated_flow")
def raw_product_to_curated_flow(
    minio_config: Dict[str, Any],
    raw_bucket: str,
    raw_prefix: str, # Prefix for raw product data
    curated_bucket: str,
    curated_prefix: str = "dim_product", # Prefix for curated product dimension
    curated_partition_cols: list = [] # Dimensions often not partitioned or simple key
):
    """
    Flow to transform raw product data to curated product dimension.
    Calls functions from data_processing.py.
    """
    print("Starting raw product to curated flow...")
    s3_filesystem = get_s3_filesystem(minio_config=minio_config)
    raw_ddf = read_parquet_from_minio(
        s3_filesystem=s3_filesystem,
        bucket_name=raw_bucket,
        prefix=raw_prefix
    )
    # Use the dedicated transformation function for product data
    curated_ddf = transform_raw_product_to_curated(ddf=raw_ddf)
    write_parquet_to_minio(
        ddf=curated_ddf,
        s3_filesystem=s3_filesystem,
        bucket_name=curated_bucket,
        prefix=curated_prefix,
        partition_cols=curated_partition_cols # Use specified partitioning (likely empty)
    )
    print("Raw product to curated flow completed.")


# ---- Master flow to orchestrate ----

@flow(name="customer_transactions_master_flow")
def customer_transactions_master_flow():
    """
    Master flow that runs ingestion followed by raw-to-curated transformation.
    Calls child flows.
    """
    print("Starting Master Flow for Customer Transactions...")

    # Configuration
    minio_config = {
        "endpoint_url": "http://192.168.1.16:9010", # Ensure this is correct
        "access_key": "minioadmin", # Ensure these match your running Minio server
        "secret_key": "minioadmin", # Ensure these match your running Minio server
        "secure": False
    }

    # Raw bucket configuration
    raw_minio_bucket = "raw-customer-activity"
    raw_transactions_prefix = "customer_transactions_raw_parquet"
    raw_products_prefix = "product_data_raw_parquet"
    raw_customers_prefix = "customer_data_raw_parquet"

    # Curated bucket configuration
    curated_minio_bucket = "curated-customer-activity"
    curated_transactions_prefix = "customer_transactions_curated_parquet"
    curated_customer_prefix = "dim_customer" # Prefix for curated customer dimension
    curated_product_prefix = "dim_product" # Prefix for curated product dimension

    # Partitioning for curated layers
    curated_transactions_partitioning = ['transaction_date'] # Partitioning for curated transactions
    curated_dimension_partitioning = [] # Dimensions often not partitioned or simple key

    # --- Ingestion Stage ---

    print("--- Starting Ingestion Stage ---")

    # Step 1: Run ingestion for Transactions
    transactions_ingestion_run = customer_transactions_ingestion_flow(
        csv_filepath=r"customer_transactions.csv", # Ensure this path is correct
        minio_config=minio_config,
        minio_bucket=raw_minio_bucket,
        minio_output_prefix=raw_transactions_prefix
    )

    # Step 2: Run ingestion for Product Dimension
    product_ingestion_run = product_dimension_ingestion_flow(
         minio_config=minio_config,
         raw_bucket=raw_minio_bucket,
         raw_prefix=raw_products_prefix,
         # Optional: Pass num_products, product_id_prefix, product_id_range if needed
    )

    # Step 3: Run ingestion for Customer Dimension
    customer_ingestion_run = customer_dimension_ingestion_flow(
         minio_config=minio_config,
         raw_bucket=raw_minio_bucket,
         raw_prefix=raw_customers_prefix,
         # Optional: Pass num_customers, customer_id_range if needed
    )

    print("--- Ingestion Stage Completed ---")
    print("--- Starting Raw to Curated Transformation Stage ---")


    # --- Raw to Curated Transformation Stage ---
    # These flows will read from the raw prefixes and write to the curated prefixes

    # Step 4: Transform Raw Transactions to Curated Transactions
    raw_to_curated_flow(
        minio_config=minio_config,
        raw_bucket=raw_minio_bucket,
        raw_prefix=raw_transactions_prefix, # Read from raw transactions
        curated_bucket=curated_minio_bucket,
        curated_prefix=curated_transactions_prefix, # Write to curated transactions
        curated_partition_cols=curated_transactions_partitioning
    )

    # Step 5: Transform Raw Customer Data to Curated Customer Dimension
    raw_customer_to_curated_flow(
        minio_config=minio_config,
        raw_bucket=raw_minio_bucket,
        raw_prefix=raw_customers_prefix, # Read from raw customer data
        curated_bucket=curated_minio_bucket,
        curated_prefix=curated_customer_prefix, # Write to curated customer dimension
        curated_partition_cols=curated_dimension_partitioning # Likely empty list
    )

    # Step 6: Transform Raw Product Data to Curated Product Dimension
    raw_product_to_curated_flow(
        minio_config=minio_config,
        raw_bucket=raw_minio_bucket,
        raw_prefix=raw_products_prefix, # Read from raw product data
        curated_bucket=curated_minio_bucket,
        curated_prefix=curated_product_prefix, # Write to curated product dimension
        curated_partition_cols=curated_dimension_partitioning # Likely empty list
    )


    print("--- Raw to Curated Transformation Stage Completed ---")
    print("Master flow completed successfully!")

# ---- Serve the master flow only ----

if __name__ == "__main__":
    master_schedule = CronSchedule(
        cron="0 1 * * *",  # Everyday at 1 AM UTC
        timezone="UTC"
    )

    serve(
        customer_transactions_master_flow.to_deployment(
            name="customer-transactions-master-deployment",
            schedules=[master_schedule]
        )
    )
