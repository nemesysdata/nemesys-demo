import os
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
STORAGE_KEY = os.getenv("STORAGE_KEY")
BLOB_CONTAINER = os.getenv("BLOB_CONTAINER")
LAKEHOUSE_PATH = os.getenv("LAKEHOUSE_PATH")

S3_URL=os.getenv("S3_URL")
S3_ACCESS_KEY=os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY=os.getenv("S3_SECRET_KEY")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

postgresHostname = os.getenv("POSTGRES_HOSTNAME")
postgresDatabase = os.getenv("POSTGRES_DATABASE")
postgresPort = os.getenv("POSTGRES_PORT")
postgresUrl = f"jdbc:postgresql://{postgresHostname}:{postgresPort}/{postgresDatabase}?"
postgresUsername = os.getenv("POSTGRES_USERNAME")
postgresPassword = os.getenv("POSTGRES_PASSWORD")
