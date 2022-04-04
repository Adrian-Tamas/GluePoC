import logging
import os

logging.info(f"Initializing configuration for the dev environment")

SETTINGS = {
    "region": "eu-west-2",
    "processed_data_table": "processed_data",
    "total_value_aggregation_table": "total_value_aggregation",
    "data_ingestion_job": "data_ingestion_job",
    "total_value_aggregation_job": "total_value_aggregation",
    "data_bucket": "data_lake",
    "glue_database": "gluedb",
    "athena_data_bucket": "data-athena",
    "athena_bucket_resource_uri": "s3://data-athena/"
}

LOGGING = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "standard": {
            "format": "%(asctime)s - %(module)s::%(funcName)s - [%(levelname)s] - %(message)s"
        }
    },
    "handlers": {
        "default": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": "standard",
        }
    },
    "loggers": {
        "glue-poc": {
            "handlers": ["default"],
            "level": "DEBUG",
            "propagate": True,
        }
    },
}
