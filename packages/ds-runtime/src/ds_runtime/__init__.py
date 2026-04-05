import os


DEPLOYMENT_NAME = os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", "local")
STORAGE_PATH = os.getenv('STORAGE_PATH', '/tmp')