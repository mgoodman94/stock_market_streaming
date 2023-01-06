import os


class Environment:
    # API information
    API_KEY = os.environ.get("API_KEY")
    API_URL = os.environ.get("API_URL")
    RETRY_COUNT = int(os.environ.get("RETRY_COUNT", 5))
    GCP_BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME")
