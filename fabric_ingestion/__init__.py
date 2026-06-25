"""
fabric-ingestion
================
Framework de ingestão de dados para Microsoft Fabric.

Exports públicos
----------------
Construção de pipeline::

    from fabric_ingestion import (
        PipelineConfig,
        LakeToLakePipeline,
        # Estratégias de leitura
        DeltaReadStrategy,
        ParquetReadStrategy,
        SparkFormatReadStrategy,
        # Estratégias de escrita
        MergeStrategy,
        FullLoadStrategy,
        IngestionLogger,
    )

Extensão (criando pipelines customizados)::

    from fabric_ingestion import PipelineBase, ReadStrategy, WriteStrategy, PipelineConfig
"""

from fabric_ingestion.base.pipeline_base import PipelineBase
from fabric_ingestion.base.pipeline_config import PipelineConfig
from fabric_ingestion.logging.ingestion_logger import IngestionLogger
from fabric_ingestion.sources.lake_to_lake import LakeToLakePipeline
from fabric_ingestion.strategies.readers.delta_read_strategy import DeltaReadStrategy
from fabric_ingestion.strategies.readers.parquet_read_strategy import ParquetReadStrategy

# Estratégias de leitura — caminho canônico: strategies/readers/
from fabric_ingestion.strategies.readers.read_strategy import ReadStrategy
from fabric_ingestion.strategies.readers.spark_format_read_strategy import SparkFormatReadStrategy
from fabric_ingestion.strategies.writers.full_load_strategy import FullLoadStrategy
from fabric_ingestion.strategies.writers.merge_strategy import MergeStrategy

# Estratégias de escrita — caminho canônico: strategies/writers/
from fabric_ingestion.strategies.writers.write_strategy import WriteStrategy

__all__ = [
    # Core
    "PipelineBase",
    "PipelineConfig",
    # Estratégias de leitura
    "ReadStrategy",
    "DeltaReadStrategy",
    "ParquetReadStrategy",
    "SparkFormatReadStrategy",
    # Estratégias de escrita
    "WriteStrategy",
    "FullLoadStrategy",
    "MergeStrategy",
    # Pipelines concretos
    "LakeToLakePipeline",
    # Utilitários
    "IngestionLogger",
]
