import logging
import logging.config
import os
from pathlib import Path

import boto3

supported_environments = ["dev", "qa"]


class Config:
    def __init__(self, settings: dict):
        # General AWS Settings
        self.aws_region = settings["region"]

        # tables
        self.glue_database = settings["glue_database"]
        self.processed_data_table = settings["processed_data_table"]
        self.total_value_aggregation_table = settings["total_value_aggregation_table"]

        # Glue jobs
        self.data_ingestion_job = settings["data_ingestion_job"]
        self.total_value_aggregation_job = settings["total_value_aggregation_job"]

        #S3 Buckets
        self.data_bucket = settings["data_bucket"]
        self.athena_data_bucket = settings["athena_data_bucket"]
        self.athena_bucket_resource_uri=settings["athena_bucket_resource_uri"]

        # Boto client
        self.aws_client = boto3.session.Session(
            profile_name=os.getenv("aws_profile", "default")
        )

        self.resource_path = os.path.join(Path(os.getcwd()), "resources")


def configure() -> Config:
    env = os.getenv("env", default="qa")

    if env == "dev":
        from .dev import SETTINGS, LOGGING
    elif env == "qa":
        from .qa import SETTINGS, LOGGING
    else:
        raise NotImplementedError(f"{env} environment is not yet configured.")

    logging.config.dictConfig(LOGGING)

    return Config(SETTINGS)
