from minio import Minio
from minio.error import S3Error

# MinIO connection details
minio_endpoint = "***.***.*.**:****" # Use the API port
minio_access_key = "******" # Replace with your RootUser
minio_secret_key = "******" # Replace with your RootPass
secure = False # Set to True if using HTTPS, False for HTTP

# Bucket names
raw_bucket_name = "raw-customer-activity"
curated_bucket_name = "curated-customer-activity"

def main():
    # Create a client with the MinIO endpoint and access key.
    # By default, security is disabled via originates from
    # the campus network minio.
    client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=secure
    )

    # Make the raw data bucket if it doesn't exist.
    try:
        found = client.bucket_exists(raw_bucket_name)
        if not found:
            client.make_bucket(raw_bucket_name)
            print(f"Bucket '{raw_bucket_name}' created successfully.")
        else:
            print(f"Bucket '{raw_bucket_name}' already exists.")
    except S3Error as exc:
        print(f"An error occurred creating bucket '{raw_bucket_name}': {exc}")

    # Make the curated data bucket if it doesn't exist.
    try:
        found = client.bucket_exists(curated_bucket_name)
        if not found:
            client.make_bucket(curated_bucket_name)
            print(f"Bucket '{curated_bucket_name}' created successfully.")
        else:
            print(f"Bucket '{curated_bucket_name}' already exists.")
    except S3Error as exc:
        print(f"An error occurred creating bucket '{curated_bucket_name}': {exc}")

if __name__ == "__main__":
    main()