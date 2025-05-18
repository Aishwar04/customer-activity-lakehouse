import csv
import random
from faker import Faker
from prefect import task
import dask.dataframe as dd
import s3fs
from typing import Dict, Any, List


## Function to create a connection to MinIO S3 compatible storage

def get_s3_filesystem(minio_config: Dict[str, Any]):
    """Task to get an S3 compatible filesystem object for MinIO."""
    try:
        print("Attempting to create S3 filesystem object for MinIO...")
        s3 = s3fs.S3FileSystem(
            key=minio_config['access_key'],
            secret=minio_config['secret_key'],
            client_kwargs={
                'endpoint_url': minio_config['endpoint_url'],
                'verify': minio_config.get('secure', False)
            }
        )
        print("S3 filesystem object for MinIO created.")
        return s3
    except Exception as e:
        print(f"Error creating S3 filesystem for MinIO: {e}")
        raise




# --- Function to generate customer data ---
def generate_customers_task(num_customers: int, output_file: str, customer_id_range: range):
    """
    Function to generate mock customer data and save it to a CSV file.

    Args:
        num_customers (int): The number of customer records to generate.
        output_file (str): The path to the output CSV file.
        customer_id_range (range): The range of customer IDs to use.
    """
    fake = Faker()
    customers = []

    # Add header row
    customers.append([
        'customer_id', 'customer_name', 'customer_email',
        'customer_city', 'customer_country', 'registration_date',
        'customer_segment' # Example additional attribute
    ])

    # Ensure we don't try to generate more customers than available IDs in the range
    if num_customers > len(customer_id_range):
        print(f"Warning: Requested {num_customers} customers, but only {len(customer_id_range)} unique IDs available in the range. Generating {len(customer_id_range)} records.")
        num_customers = len(customer_id_range)

    # Shuffle the range and take the required number of IDs to ensure randomness
    customer_ids_to_use = random.sample(list(customer_id_range), num_customers)

    print(f"Generating {num_customers} customer records to {output_file}...")

    for customer_id in customer_ids_to_use:
         customers.append([
            customer_id,
            fake.name(),
            fake.email(),
            fake.city(),
            fake.country(),
            fake.date_this_year().strftime('%Y-%m-%d'), # Registration date within the last year
            random.choice(['A', 'B', 'C', 'D']) # Example customer segment
        ])

    # Write to CSV file
    with open(output_file, 'w', newline='\n', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(customers)

    print(f"Finished generating {num_customers} customer records.")
    return output_file # Return the path to the generated file



# --- Function to generate product data ---
def generate_products_task(num_products: int, output_file: str, product_id_prefix: str, product_id_range: range):
    """
    Function to generate mock product data and save it to a CSV file.
    (Note: When not a Prefect task, this runs as a regular function call)

    Args:
        num_products (int): The number of product records to generate.
        output_file (str): The path to the output CSV file.
        product_id_prefix (str): The prefix used for product IDs (e.g., 'PROD').
        product_id_range (range): The numerical range used for product IDs.
    """
    fake = Faker()
    products = []

    # Add header row
    products.append([
        'product_id', 'product_name', 'product_category', 'product_brand',
        'product_weight_kg' # Example additional attribute
    ])

    # Generate product IDs based on the range and prefix from transaction script
    available_product_ids = [f'{product_id_prefix}{i}' for i in product_id_range]

    if num_products > len(available_product_ids):
         print(f"Warning: Requested {num_products} products, but only {len(available_product_ids)} unique IDs available in the range. Generating {len(available_product_ids)} records.")
         num_products = len(available_product_ids)

    product_ids_to_use = random.sample(list(available_product_ids), num_products) # Ensure list() for random.sample

    print(f"Generating {num_products} product records to {output_file}...")

    for product_id in product_ids_to_use:
         products.append([
            product_id,
            fake.word().capitalize() + " " + fake.word(), # Simple product name
            fake.random_element(elements=('Electronics', 'Books', 'Clothing', 'Home Goods', 'Groceries', 'Outdoors', 'Toys', 'Beauty')), # Example categories
            fake.company(), # Use company name as brand
            round(random.uniform(0.1, 20.0), 2) # Example weight
        ])

    # Write to CSV file
    with open(output_file, 'w', newline='\n', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(products)

    print(f"Finished generating {num_products} product records.")
    return output_file # Return the path to the generated file

## Functions for transaction data ingestion and transformation

def read_csv_with_dask(csv_filepath: str) -> dd.DataFrame:
    """Task to read a large CSV file into a Dask DataFrame."""
    print(f"Reading CSV file into Dask DataFrame: {csv_filepath}")
    try:
        # Define dtypes explicitly for robustness and performance
        # Read 'transaction_timestamp' as object/string initially
        dtypes = {
            'transaction_id': 'object',
            'customer_id': 'int64',
            'product_id': 'object',
            'transaction_timestamp': 'object',
            'quantity': 'int64',
            'price': 'float64',
            'store_location': 'object',
            'payment_method': 'object'
        }

        ddf = dd.read_csv(csv_filepath, dtype=dtypes)
        print("Dask DataFrame created (lazy).")
        return ddf
    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_filepath}")
        raise
    except Exception as e:
        print(f"Error reading CSV with Dask: {e}")
        raise


def transform_dask_dataframe(ddf: dd.DataFrame) -> dd.DataFrame:
    """Task to perform transformations for raw ingestion, including creating 'transaction_date' for partitioning."""
    print("Performing transformations for raw ingestion (lazy)...")

    # Convert 'transaction_timestamp' to datetime objects for filtering
    datetime_series = dd.to_datetime(ddf['transaction_timestamp'], errors='coerce')

    # Filter out rows where the timestamp conversion failed (resulted in NaT)
    # This prevents potential issues with invalid partition names
    valid_ddf = ddf[~datetime_series.isna()].copy()  # Use .copy() to avoid SettingWithCopyWarning

    # Create the 'transaction_date' column (YYYY-MM-DD string)
    transformed_ddf = valid_ddf.assign(
        transaction_date=dd.to_datetime(valid_ddf['transaction_timestamp'], errors='coerce').dt.strftime('%Y-%m-%d')
    )

    # Ensure the new 'transaction_date' column is explicitly treated as string (should already be string from strftime)
    transformed_ddf['transaction_date'] = transformed_ddf['transaction_date'].astype(str)

    print("Raw ingestion transformations defined (lazy): creating 'transaction_date' for partitioning and filtering invalid rows.")

    return transformed_ddf


def repartition_dask_dataframe(ddf: dd.DataFrame, target_partitions: int) -> dd.DataFrame:
    """Task to repartition the Dask DataFrame to a target number of partitions."""
    print(f"Repartitioning Dask DataFrame into {target_partitions} partitions...")
    try:
        repartitioned_ddf = ddf.repartition(npartitions=target_partitions)
        print(f"Dask DataFrame successfully repartitioned into {target_partitions} partitions.")
        return repartitioned_ddf
    except Exception as e:
        print(f"Error during repartitioning: {e}")
        raise


## Write raw data to mionio as parquet

def write_dask_dataframe_to_minio(
    ddf: dd.DataFrame,
    s3_filesystem: s3fs.S3FileSystem,
    bucket_name: str,
    prefix: str
):
    """Task to write the Dask DataFrame to MinIO as Parquet for raw ingestion, partitioned by date."""
    output_path = f"{bucket_name}/{prefix}"
    print(f"Writing Dask DataFrame to MinIO as Parquet at s3://{output_path}")

    try:
        dd.to_parquet(
            ddf,
            output_path,
            engine='pyarrow',
            filesystem=s3_filesystem,
            overwrite=True,
            partition_on=['transaction_date']  # Partition by the 'transaction_date' column created in raw transform
        )
        print("Dask DataFrame successfully written to MinIO as Parquet for raw ingestion, partitioned by date.")
    except Exception as e:
        print(f"Error writing Dask DataFrame to MinIO for raw ingestion: {e}")
        raise


def read_parquet_from_minio(s3_filesystem: s3fs.S3FileSystem, bucket_name: str, prefix: str) -> dd.DataFrame:
    """Task to read Parquet data from a MinIO bucket into a Dask DataFrame."""
    source_path = f"s3://{bucket_name}/{prefix}"
    print(f"Reading Parquet data from MinIO: {source_path}")
    try:
        # Dask can read partitioned Parquet datasets
        ddf = dd.read_parquet(
            source_path,
            filesystem=s3_filesystem,
            engine='pyarrow' # Specify pyarrow as the engine
        )
        print("Dask DataFrame created from Parquet (lazy).")
        return ddf
    except FileNotFoundError:
        print(f"Error: No Parquet data found at {source_path}")
        raise
    except Exception as e:
        print(f"Error reading Parquet from MinIO: {e}")
        raise



def transform_raw_to_curated(ddf: dd.DataFrame) -> dd.DataFrame:
    """Task to perform transformations from raw to curated layer."""
    print("Performing transformations from raw to curated (lazy)...")

    # Example Transformations for the Curated Layer:
    curated_columns = [
        'transaction_id',
        'customer_id',
        'product_id',
        'transaction_date',  # Use the date column for partitioning/analysis
        'transaction_timestamp',  # Keep the original timestamp if needed
        'quantity',
        'price',
        'store_location',
        'payment_method'
    ]

    # Ensure all curated_columns actually exist in the input ddf
    for col in curated_columns:
        if col not in ddf.columns:
            print(f"Error: Expected curated column '{col}' not found in input DataFrame for curated transform!")

    curated_ddf = ddf[curated_columns].copy()  # Use .copy() after selection

    # 2. Explicitly cast dtypes for analytical consistency and performance
    try:
        if 'customer_id' in curated_ddf.columns:
            curated_ddf['customer_id'] = curated_ddf['customer_id'].astype('int64')
        if 'quantity' in curated_ddf.columns:
            curated_ddf['quantity'] = curated_ddf['quantity'].astype('int64')
        if 'price' in curated_ddf.columns:
            curated_ddf['price'] = curated_ddf['price'].astype('float64')

        # 'transaction_date' should already be string from the previous transform
        if 'transaction_timestamp' in curated_ddf.columns and curated_ddf['transaction_timestamp'].dtype == 'object':
            print("Attempting to convert 'transaction_timestamp' to datetime in curated transform...")
            curated_ddf['transaction_timestamp'] = dd.to_datetime(curated_ddf['transaction_timestamp'], errors='coerce')

    except Exception as cast_exc:
        print(f"Warning: Error casting dtypes in curated transform: {cast_exc}. Proceeding with current dtypes.")

    print("Transformations for curated layer defined (lazy).")

    return curated_ddf  # Return the transformed Dask DataFrame (still lazy)



# Add transformation logic for customer data
def transform_raw_customer_to_curated(ddf: dd.DataFrame) -> dd.DataFrame:
    """Function to transform raw customer data to curated dimension."""
    print("Performing transformations for curated customer dimension (lazy)...")

    # 1. Select relevant columns for the dimension table
    curated_customer_columns = [
        'customer_id',
        'customer_name',
        'customer_email',
        'customer_city',
        'customer_country',
        'registration_date',
        'customer_segment'
    ]
    # Ensure columns exist before selecting
    for col in curated_customer_columns:
        if col not in ddf.columns:
            print(f"Warning: Expected customer column '{col}' not found in raw customer DataFrame. Skipping selection.")
            pass # Or remove col from curated_customer_columns if strictly necessary

    # Filter the list to only include columns present in the input ddf
    present_curated_customer_columns = [col for col in curated_customer_columns if col in ddf.columns]

    curated_customer_ddf = ddf[present_curated_customer_columns].copy()

    # 2. Type Casting
    try:
        if 'customer_id' in curated_customer_ddf.columns:
            # Ensure customer_id is appropriate type for joining
            curated_customer_ddf['customer_id'] = curated_customer_ddf['customer_id'].astype('int64')
        if 'registration_date' in curated_customer_ddf.columns and curated_customer_ddf['registration_date'].dtype == 'object':
             # Convert registration_date to datetime, then format as string YYYY-MM-DD
             # Use errors='coerce' to turn invalid dates into NaT
             datetime_series = dd.to_datetime(curated_customer_ddf['registration_date'], errors='coerce')
             # Handle NaT if necessary - e.g., fillna('1900-01-01') or drop rows
             curated_customer_ddf['registration_date'] = datetime_series.dt.strftime('%Y-%m-%d')
             curated_customer_ddf['registration_date'] = curated_customer_ddf['registration_date'].astype(str)

    except Exception as cast_exc:
         print(f"Warning: Error casting dtypes in curated customer transform: {cast_exc}. Proceeding with current dtypes.")


    # 3. Handle Missing Values (Example: Fill missing customer_segment)
    if 'customer_segment' in curated_customer_ddf.columns:
        curated_customer_ddf['customer_segment'] = curated_customer_ddf['customer_segment'].fillna('Unknown')

    # 4. Handle Duplicates (Example: Keep the first occurrence based on customer_id)
    # Dask's drop_duplicates can be computationally expensive on large datasets
    # If customer_id should be unique, this is important.
    # curated_customer_ddf = curated_customer_ddf.drop_duplicates(subset=['customer_id'], keep='first')


    print("Curated customer dimension transformations defined (lazy).")
    return curated_customer_ddf

# Add transformation logic for product data
def transform_raw_product_to_curated(ddf: dd.DataFrame) -> dd.DataFrame:
    """Function to transform raw product data to curated dimension."""
    print("Performing transformations for curated product dimension (lazy)...")

    # Example Refinements for Product Data:

    # 1. Select relevant columns for the dimension table
    curated_product_columns = [
        'product_id',
        'product_name',
        'product_category',
        'product_brand',
        'product_weight_kg'
    ]
    # Ensure columns exist before selecting
    for col in curated_product_columns:
        if col not in ddf.columns:
            print(f"Warning: Expected product column '{col}' not found in raw product DataFrame. Skipping selection.")
            pass # Or remove col from curated_product_columns if strictly necessary

    # Filter the list to only include columns present in the input ddf
    present_curated_product_columns = [col for col in curated_product_columns if col in ddf.columns]

    curated_product_ddf = ddf[present_curated_product_columns].copy()


    # 2. Type Casting
    try:
        if 'product_weight_kg' in curated_product_ddf.columns:
            curated_product_ddf['product_weight_kg'] = curated_product_ddf['product_weight_kg'].astype('float64')
        # product_id is likely object/string, which is fine for a dimension key
    except Exception as cast_exc:
         print(f"Warning: Error casting dtypes in curated product transform: {cast_exc}. Proceeding with current dtypes.")


    # 3. Standardize Formats (Example: Capitalize product_category)
    if 'product_category' in curated_product_ddf.columns:
         # Dask apply can be slow, but for simple string operations it might be okay
         # For more complex string cleaning, consider using Dask's string accessors (.str)
         curated_product_ddf['product_category'] = curated_product_ddf['product_category'].str.capitalize()
    print("Curated product dimension transformations defined (lazy).")
    return curated_product_ddf



## Write curated data to minio as parquet

def write_parquet_to_minio(
    ddf: dd.DataFrame,
    s3_filesystem: s3fs.S3FileSystem,
    bucket_name: str,
    prefix: str,
    partition_cols: List[str] = None,  # Specify columns to partition by in the curated layer
    target_partitions: int = 1    # Target number of partitions for output, defualt to 1
):
    """Task to write the Dask DataFrame to a MinIO bucket as Parquet, with partitioning."""
    output_path = f"{bucket_name}/{prefix}"
    print(f"Writing Dask DataFrame to MinIO as Parquet at s3://{output_path}")

    try:
        # Step 1: Repartition the Dask DataFrame to control the number of output files
        ddf = repartition_dask_dataframe(ddf, target_partitions)

        # Step 2: Ensure all partition columns exist in the DataFrame before writing
        for col in partition_cols:
            if col not in ddf.columns:
                print(f"Error: Partitioning column '{col}' not found in DataFrame before writing!")
                raise ValueError(f"Partitioning column '{col}' is missing from the DataFrame.")

        # Step 3: Write to MinIO as Parquet with partitioning
        dd.to_parquet(
            ddf,
            output_path,
            engine='pyarrow',
            filesystem=s3_filesystem,
            overwrite=True,  # Be cautious with overwrite in production
            partition_on=partition_cols  # Partitioning for the curated layer
        )
        print(f"Dask DataFrame successfully written to MinIO as Parquet for curated layer, partitioned by {partition_cols}.")
        print("Dask DataFrame successfully written to MinIO as Parquet for curated layer, with partitioning.")

    except Exception as e:
        print(f"Error writing Dask DataFrame to MinIO for curated layer: {e}")
        raise