# Metaflow config for local dev with MinIO + Bacalhau
# Copy to ~/.metaflowconfig/config.json or source as env vars (see env.sh)

METAFLOW_DEFAULT_DATASTORE = "s3"
METAFLOW_DATASTORE_SYSROOT_S3 = "s3://metaflow"

# Point boto3 at MinIO
METAFLOW_S3_ENDPOINT_URL = "http://localhost:9000"
METAFLOW_S3_VERIFY_CERTIFICATE = False
