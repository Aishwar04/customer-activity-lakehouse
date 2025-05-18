Customer Activity Data Lakehouse
This project demonstrates a basic data pipeline designed to simulate, ingest, and transform customer transaction and dimension data into a curated data lakehouse structure (Star Schema) stored in an S3-compatible object storage (MinIO). The pipeline is orchestrated using Prefect.

Project Description
The goal of this project is to build a foundational data platform capable of handling customer activity data. It simulates incoming data, lands it in a raw zone, transforms it into a structured format suitable for analytics (a Star Schema with fact and dimension tables), and stores it in a curated zone within MinIO. The entire process is automated and managed using Prefect workflows.

Architecture
The project follows a layered architecture:

Data Generation: Python scripts simulate customer transactions, product data, and customer profiles.

Object Storage (MinIO): Used as the data lake storage layer, divided into:

Raw Zone: Stores data in its initial format (Parquet, partitioned by date for transactions).

Curated Zone: Stores transformed data organized into a Star Schema (Fact and Dimension tables) in Parquet format.

Data Processing (Dask & PyArrow): Dask is used for handling potentially larger-than-memory datasets during ingestion and transformation. PyArrow is used under the hood by Dask for efficient Parquet reading and writing.

Orchestration (Prefect): Prefect workflows manage the sequence of steps, from data generation and ingestion to transformation into the curated layer.

Data Modeling: The curated layer is structured as a Star Schema, with a fact_customer_transactions table and dim_customer and dim_product dimension tables.

Setup and Prerequisites
To run this project, you will need:

Python 3.8+: Install Python.

MinIO: Set up a MinIO server. You'll need its S3 API endpoint (IP and port) and access credentials (access key and secret key). Ensure the API port (typically 9000 or 9010) is accessible from where you run the Python scripts.

Required Python Libraries: Install the necessary libraries using pip:

pip install faker dask s3fs prefect pandas pyarrow minio

(Note: fsspec[s3] is a dependency of s3fs and should be installed automatically)

Prefect Server: For full orchestration and UI capabilities, you should have a Prefect server running. Refer to the Prefect documentation for installation and setup (https://docs.prefect.io/).

Project Structure
buckets.py: Contains a script to create the necessary MinIO buckets (raw-customer-activity and curated-customer-activity).

generate_transaction.py: Script to generate mock customer transaction data into a CSV file (customer_transactions.csv).

generate_customers.py: Script to generate mock customer dimension data into a CSV file (customer_profiles.csv). (Note: The generation logic is also included in data_processing.py for direct use in flows).

generate_product.py: Script to generate mock product dimension data into a CSV file (product_data.csv). (Note: The generation logic is also included in data_processing.py for direct use in flows).

data_processing.py: Contains the core data processing functions using Dask and s3fs for reading/writing to MinIO and performing transformations. Includes functions for getting the S3 filesystem, reading/writing Parquet, and applying transformations for both raw ingestion and raw-to-curated steps for transactions and dimensions.

flows.py: Defines the Prefect flows that orchestrate the entire pipeline. It includes child flows for ingestion and transformation, and a master flow that runs the complete process. It also includes the Prefect deployment configuration to serve the master flow on a schedule.

How to Run
Start MinIO: Ensure your MinIO server is running and accessible at the endpoint specified in your configuration (e.g., http://192.168.1.10:9010).

Create Buckets: Run the buckets.py script to create the necessary raw and curated buckets in MinIO:

python buckets.py

Generate Initial Data (Optional but Recommended): Run the generation scripts once manually to create the initial CSV files that the ingestion flows will read.

python generate_transaction.py

Start Prefect Agent: Ensure you have a Prefect agent running that can pick up deployments.

Deploy the Master Flow: Run the flows.py script to deploy the master flow to your Prefect server. This will register the customer-transactions-master-deployment and schedule it to run according to the cron schedule defined in the script (0 1 * * * - every day at 1 AM UTC).

python flows.py

Monitor in Prefect UI: Access your Prefect UI to monitor the execution of the customer-transactions-master-flow. You can also trigger runs manually from the UI.

The master flow will execute the following steps:

Generate and ingest raw transaction data from customer_transactions.csv into raw-customer-activity/customer_transactions_raw_parquet/.

Generate and ingest raw product data into raw-customer-activity/product_data_raw_parquet/.

Generate and ingest raw customer data into raw-customer-activity/customer_data_raw_parquet/.

Transform raw transaction data into curated-customer-activity/customer_transactions_curated_parquet/ (partitioned by transaction_date).

Transform raw customer data into curated-customer-activity/dim_customer/.

Transform raw product data into curated-customer-activity/dim_product/.

Querying the Curated Data
Once the data is in the curated layer in MinIO, you can query it using various tools:

DuckDB: As explored, DuckDB can directly query Parquet files in MinIO using its httpfs extension from the CLI or Python.

Trino/Presto: Configure a Hive connector in Trino/Presto to point to your MinIO endpoint and define tables pointing to the curated prefixes.

Apache Spark SQL: Use Spark to read Parquet data from MinIO and query it via Spark SQL.

BI Tools (Power BI, Metabase, Superset): Connect these tools to a query engine (like Trino or Spark) that is querying your MinIO data. Direct connections to S3/MinIO might be possible but less performant for analytical queries on large datasets.

Future Enhancements
Transactional Layer: Implement a transactional data lake format like Delta Lake, Apache Hudi, or Apache Iceberg on the curated layer to support ACID transactions, upserts, and deletes.

Streaming Ingestion: Explore tools like Apache Kafka and Flink or Spark Structured Streaming for near real-time streaming ingestion into the raw layer.

Schema Evolution: Implement robust schema evolution handling as data sources change.

Data Quality Checks: Add data validation and quality checks within the pipeline using libraries like Great Expectations or Deequ.

Monitoring and Alerting: Set up more comprehensive monitoring and alerting for pipeline health and data quality.

Scalability: Deploy Dask and Prefect on a distributed cluster for handling larger data volumes.

CI/CD: Implement Continuous Integration and Continuous Deployment for the pipeline code.