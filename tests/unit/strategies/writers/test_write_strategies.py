"""Testes unitários para as WriteStrategies (FullLoad e Merge)."""

from __future__ import annotations

import logging

import pytest
from pyspark.sql import SparkSession

from fabric_ingestion.base.pipeline_config import PipelineConfig
from fabric_ingestion.strategies.writers.full_load_strategy import FullLoadStrategy
from fabric_ingestion.strategies.writers.merge_strategy import MergeStrategy

logger = logging.getLogger(__name__)


@pytest.fixture
def config(tmp_origin_path, tmp_destiny_path):
    return PipelineConfig(
        origin_path=tmp_origin_path,
        destiny_path=tmp_destiny_path,
        unique_columns=["id"],
    )


# ── FullLoadStrategy ──────────────────────────────────────────────────────────


@pytest.mark.unit
class TestFullLoadStrategy:
    def test_creates_delta_table_on_first_run(self, spark: SparkSession, config, tmp_destiny_path):
        """Deve criar a tabela Delta no destino quando ela não existe."""
        df = spark.createDataFrame([("1", "Alice")], ["id", "name"])

        FullLoadStrategy().execute(df, config, spark, logger)

        result = spark.read.format("delta").load(tmp_destiny_path)
        assert result.count() == 1

    def test_overwrites_existing_data(self, spark: SparkSession, config, tmp_destiny_path):
        """Segunda execução deve sobrescrever completamente os dados anteriores."""
        df_v1 = spark.createDataFrame([("1", "Alice"), ("2", "Bob")], ["id", "name"])
        df_v2 = spark.createDataFrame([("3", "Carol")], ["id", "name"])

        strategy = FullLoadStrategy()
        strategy.execute(df_v1, config, spark, logger)
        strategy.execute(df_v2, config, spark, logger)

        result = spark.read.format("delta").load(tmp_destiny_path)
        assert result.count() == 1
        assert result.collect()[0]["name"] == "Carol"

    def test_partition_by_is_applied(self, spark: SparkSession, tmp_origin_path, tmp_destiny_path):
        """Deve particionamento por coluna quando configurado."""
        config = PipelineConfig(
            origin_path=tmp_origin_path,
            destiny_path=tmp_destiny_path,
            unique_columns=["id"],
            partition_by=["region"],
        )
        df = spark.createDataFrame([("1", "north"), ("2", "south")], ["id", "region"])

        FullLoadStrategy().execute(df, config, spark, logger)

        result = spark.read.format("delta").load(tmp_destiny_path)
        assert result.count() == 2


# ── MergeStrategy ─────────────────────────────────────────────────────────────


@pytest.mark.unit
class TestMergeStrategy:
    def test_creates_table_when_destination_does_not_exist(
        self, spark: SparkSession, config, tmp_destiny_path
    ):
        """Quando destino não existe, deve criar a tabela via write inicial."""
        df = spark.createDataFrame([("1", "Alice")], ["id", "name"])

        MergeStrategy().execute(df, config, spark, logger)

        result = spark.read.format("delta").load(tmp_destiny_path)
        assert result.count() == 1

    def test_inserts_new_records(self, spark: SparkSession, config, tmp_destiny_path):
        """Registros novos devem ser inseridos no destino."""
        df_initial = spark.createDataFrame([("1", "Alice")], ["id", "name"])
        df_new = spark.createDataFrame([("2", "Bob")], ["id", "name"])

        strategy = MergeStrategy()
        strategy.execute(df_initial, config, spark, logger)
        strategy.execute(df_new, config, spark, logger)

        result = spark.read.format("delta").load(tmp_destiny_path)
        assert result.count() == 2

    def test_updates_existing_records(self, spark: SparkSession, config, tmp_destiny_path):
        """Registros com a mesma chave devem ter seus valores atualizados."""
        df_initial = spark.createDataFrame([("1", "Alice")], ["id", "name"])
        df_updated = spark.createDataFrame([("1", "Alice Updated")], ["id", "name"])

        strategy = MergeStrategy()
        strategy.execute(df_initial, config, spark, logger)
        strategy.execute(df_updated, config, spark, logger)

        result = spark.read.format("delta").load(tmp_destiny_path)
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice Updated"

    def test_upsert_inserts_and_updates_in_same_run(
        self, spark: SparkSession, config, tmp_destiny_path
    ):
        """Deve inserir novos E atualizar existentes em uma única execução de MERGE."""
        df_initial = spark.createDataFrame([("1", "Alice"), ("2", "Bob")], ["id", "name"])
        df_upsert = spark.createDataFrame([("1", "Alice V2"), ("3", "Carol")], ["id", "name"])

        strategy = MergeStrategy()
        strategy.execute(df_initial, config, spark, logger)
        strategy.execute(df_upsert, config, spark, logger)

        rows = spark.read.format("delta").load(tmp_destiny_path).collect()
        result = {r["id"]: r["name"] for r in rows}
        assert result["1"] == "Alice V2"  # atualizado
        assert result["2"] == "Bob"  # preservado
        assert result["3"] == "Carol"  # inserido
