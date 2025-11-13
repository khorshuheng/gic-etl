import argparse

import yaml
from pydantic import BaseModel


class DataStore(BaseModel):
    url: str
    driver_jar_path: str


class ExternalFundsIngestion(BaseModel):
    src: str


class Ingestion(BaseModel):
    checkpoint: str
    external_funds: ExternalFundsIngestion


class PricingReconciliation(BaseModel):
    dest: str


class TopPerformingFund(BaseModel):
    dest: str


class Report(BaseModel):
    pricing_reconciliation: PricingReconciliation
    top_performing_funds: TopPerformingFund


class Config(BaseModel):
    datastore: DataStore
    ingestion: Ingestion
    report: Report


def parse_config() -> Config:
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", required=True, type=str)
    cmd_args = parser.parse_args()
    config_file_path = cmd_args.config
    with open(config_file_path, "r") as f:
        config = yaml.safe_load(f)
    return Config(**config)
