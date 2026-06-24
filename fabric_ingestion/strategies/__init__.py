# Stub de compatibilidade — lógica movida para strategies/readers/ e strategies/writers/
from fabric_ingestion.strategies.readers.read_strategy import ReadStrategy
from fabric_ingestion.strategies.readers.delta_read_strategy import DeltaReadStrategy
from fabric_ingestion.strategies.readers.parquet_read_strategy import ParquetReadStrategy
from fabric_ingestion.strategies.readers.spark_format_read_strategy import SparkFormatReadStrategy
from fabric_ingestion.strategies.writers.write_strategy import WriteStrategy
from fabric_ingestion.strategies.writers.full_load_strategy import FullLoadStrategy
from fabric_ingestion.strategies.writers.merge_strategy import MergeStrategy

__all__ = [
    # Leitura
    "ReadStrategy",
    "DeltaReadStrategy",
    "ParquetReadStrategy",
    "SparkFormatReadStrategy",
    # Escrita
    "WriteStrategy",
    "FullLoadStrategy",
    "MergeStrategy",
]
