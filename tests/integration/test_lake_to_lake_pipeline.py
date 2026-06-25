"""
Teste de integração end-to-end para LakeToLakePipeline.

Executa o fluxo completo com tabelas Delta reais em diretórios temporários,
cobrindo as combinações de ReadStrategy x WriteStrategy mais comuns.
"""

from __future__ import annotations

import logging

import pytest
from pyspark.sql import SparkSession

from fabric_ingestion import LakeToLakePipeline
from fabric_ingestion.base.pipeline_config import PipelineConfig
from fabric_ingestion.strategies.readers.delta_read_strategy import DeltaReadStrategy
from fabric_ingestion.strategies.readers.parquet_read_strategy import ParquetReadStrategy
from fabric_ingestion.strategies.writers.full_load_strategy import FullLoadStrategy
from fabric_ingestion.strategies.writers.merge_strategy import MergeStrategy

logger = logging.getLogger(__name__)


@pytest.fixture
def base_config(tmp_origin_path, tmp_destiny_path):
    return PipelineConfig(
        origin_path=tmp_origin_path,
        destiny_path=tmp_destiny_path,
        unique_columns=["id"],
        date_column="updated_at",
        date_format="yyyy-MM-dd",
    )


def _write_delta(spark, df, path):
    """Helper: escreve DataFrame como tabela Delta."""
    df.write.format("delta").mode("overwrite").save(path)


@pytest.mark.integration
class TestLakeToLakeDeltaToDelta:
    """Delta (Files ou Tables) → Delta: FullLoad e Merge."""

    def test_full_load_creates_destination_table(
        self, spark: SparkSession, base_config, tmp_origin_path, tmp_destiny_path
    ):
        """FullLoad deve criar a tabela de destino com todos os registros."""
        df = spark.createDataFrame(
            [("1", "Alice", "2024-01-15"), ("2", "Bob", "2024-01-16")],
            ["id", "name", "updated_at"],
        )
        _write_delta(spark, df, tmp_origin_path)

        pipeline = LakeToLakePipeline(
            spark=spark,
            config=base_config,
            read_strategy=DeltaReadStrategy(),
            write_strategy=FullLoadStrategy(),
        )
        pipeline.execute(end_date="2024-12-31")

        result = spark.read.format("delta").load(tmp_destiny_path)
        assert result.count() == 2

    def test_full_load_with_date_filter(
        self, spark: SparkSession, base_config, tmp_origin_path, tmp_destiny_path
    ):
        """FullLoad com filtro de período deve carregar apenas registros dentro do intervalo."""
        df = spark.createDataFrame(
            [
                ("1", "Alice", "2024-01-15"),
                ("2", "Bob", "2024-06-01"),  # fora do intervalo
            ],
            ["id", "name", "updated_at"],
        )
        _write_delta(spark, df, tmp_origin_path)

        pipeline = LakeToLakePipeline(
            spark=spark,
            config=base_config,
            read_strategy=DeltaReadStrategy(),
            write_strategy=FullLoadStrategy(),
        )
        pipeline.execute(start_date="2024-01-01", end_date="2024-03-31")

        result = spark.read.format("delta").load(tmp_destiny_path)
        assert result.count() == 1
        assert result.collect()[0]["name"] == "Alice"

    def test_merge_upsert_flow(
        self, spark: SparkSession, base_config, tmp_origin_path, tmp_destiny_path
    ):
        """Merge deve inserir novos e atualizar existentes em execuções consecutivas."""
        # Carga inicial
        df_v1 = spark.createDataFrame(
            [("1", "Alice", "2024-01-01"), ("2", "Bob", "2024-01-01")],
            ["id", "name", "updated_at"],
        )
        _write_delta(spark, df_v1, tmp_origin_path)

        pipeline = LakeToLakePipeline(
            spark=spark,
            config=base_config,
            read_strategy=DeltaReadStrategy(),
            write_strategy=MergeStrategy(),
        )
        pipeline.execute(end_date="2024-12-31")

        # Segunda carga: atualiza "1" e insere "3"
        df_v2 = spark.createDataFrame(
            [("1", "Alice Updated", "2024-02-01"), ("3", "Carol", "2024-02-01")],
            ["id", "name", "updated_at"],
        )
        # Sobrescreve a origem para simular nova extração
        writer = df_v2.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        writer.save(tmp_origin_path)

        pipeline.execute(end_date="2024-12-31")

        rows = spark.read.format("delta").load(tmp_destiny_path).collect()
        result = {r["id"]: r["name"] for r in rows}
        assert result["1"] == "Alice Updated"
        assert result["2"] == "Bob"
        assert result["3"] == "Carol"


@pytest.mark.integration
class TestLakeToLakeParquetToDeltal:
    """Parquet (Files) → Delta: leitura de parquet puro."""

    def test_parquet_to_delta_full_load(
        self, spark: SparkSession, base_config, tmp_origin_path, tmp_destiny_path
    ):
        """Deve ler Parquet e escrever Delta corretamente."""
        df = spark.createDataFrame([("1", "Alice", "2024-01-10")], ["id", "name", "updated_at"])
        df.write.format("parquet").mode("overwrite").save(tmp_origin_path)

        pipeline = LakeToLakePipeline(
            spark=spark,
            config=base_config,
            read_strategy=ParquetReadStrategy(),
            write_strategy=FullLoadStrategy(),
        )
        pipeline.execute(end_date="2024-12-31")

        result = spark.read.format("delta").load(tmp_destiny_path)
        assert result.count() == 1
