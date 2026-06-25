"""Testes unitários para DedupStep."""

from __future__ import annotations

import logging
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from fabric_ingestion.base.pipeline_config import PipelineConfig
from fabric_ingestion.steps.dedup_step import DedupStep

logger = logging.getLogger(__name__)


@pytest.fixture
def config_with_date(tmp_path):
    return PipelineConfig(
        origin_path=str(tmp_path / "origin"),
        destiny_path=str(tmp_path / "destiny"),
        unique_columns=["id"],
        date_column="updated_at",
    )


@pytest.fixture
def config_without_date(tmp_path):
    return PipelineConfig(
        origin_path=str(tmp_path / "origin"),
        destiny_path=str(tmp_path / "destiny"),
        unique_columns=["id"],
    )


@pytest.mark.unit
class TestDedupByLatest:
    """DedupStep com date_column configurada — usa Window + row_number."""

    def test_keeps_most_recent_row(self, spark: SparkSession, config_with_date):
        """Deve manter apenas a linha com a data mais recente para cada chave."""
        data = [
            ("1", datetime(2024, 1, 2, 12, 0), "novo"),
            ("1", datetime(2024, 1, 1, 12, 0), "antigo"),
        ]
        df = spark.createDataFrame(data, ["id", "updated_at", "value"])

        result = DedupStep(config_with_date, logger).run(df)
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["value"] == "novo"

    def test_multiple_keys_each_deduplicated_independently(
        self, spark: SparkSession, config_with_date
    ):
        """Cada chave única deve ser deduplicada de forma independente."""
        data = [
            ("A", datetime(2024, 3, 1), "A-new"),
            ("A", datetime(2024, 1, 1), "A-old"),
            ("B", datetime(2024, 2, 1), "B-new"),
            ("B", datetime(2024, 1, 15), "B-old"),
        ]
        df = spark.createDataFrame(data, ["id", "updated_at", "value"])

        result = DedupStep(config_with_date, logger).run(df)
        rows = {r["id"]: r["value"] for r in result.collect()}

        assert len(rows) == 2
        assert rows["A"] == "A-new"
        assert rows["B"] == "B-new"

    def test_single_record_unchanged(self, spark: SparkSession, config_with_date):
        """Registro único não deve ser descartado."""
        data = [("1", datetime(2024, 1, 1), "único")]
        df = spark.createDataFrame(data, ["id", "updated_at", "value"])

        result = DedupStep(config_with_date, logger).run(df)

        assert result.count() == 1

    def test_no_dedup_row_num_column_in_result(self, spark: SparkSession, config_with_date):
        """A coluna auxiliar _dedup_row_num não deve aparecer no resultado."""
        data = [
            ("1", datetime(2024, 1, 2), "v2"),
            ("1", datetime(2024, 1, 1), "v1"),
        ]
        df = spark.createDataFrame(data, ["id", "updated_at", "value"])

        result = DedupStep(config_with_date, logger).run(df)

        assert "_dedup_row_num" not in result.columns

    def test_date_column_not_in_df_falls_back_to_drop_duplicates(
        self, spark: SparkSession, tmp_path
    ):
        """Se date_column não existe no DataFrame, usa dropDuplicates."""
        config = PipelineConfig(
            origin_path=str(tmp_path / "o"),
            destiny_path=str(tmp_path / "d"),
            unique_columns=["id"],
            date_column="coluna_inexistente",  # não está no DataFrame
        )
        data = [("1", "a"), ("1", "a"), ("2", "b")]
        df = spark.createDataFrame(data, ["id", "value"])

        result = DedupStep(config, logger).run(df)

        # dropDuplicates por id: "1" e "2" -> 2 linhas
        assert result.count() == 2


@pytest.mark.unit
class TestDedupByDrop:
    """DedupStep sem date_column — usa dropDuplicates."""

    def test_removes_exact_duplicates(self, spark: SparkSession, config_without_date):
        """Linhas com o mesmo id devem ser deduplicadas via dropDuplicates."""
        data = [("1", "a"), ("1", "a"), ("2", "b")]
        df = spark.createDataFrame(data, ["id", "value"])

        result = DedupStep(config_without_date, logger).run(df)

        assert result.count() == 2

    def test_unique_records_unchanged(self, spark: SparkSession, config_without_date):
        """Registros únicos devem ser preservados integralmente."""
        data = [("1", "a"), ("2", "b"), ("3", "c")]
        df = spark.createDataFrame(data, ["id", "value"])

        result = DedupStep(config_without_date, logger).run(df)

        assert result.count() == 3

    def test_empty_dataframe_returns_empty(self, spark: SparkSession, config_without_date):
        """DataFrame vazio não deve lançar erro."""
        df = spark.createDataFrame([], spark.createDataFrame([("x",)], ["id"]).schema)

        result = DedupStep(config_without_date, logger).run(df)

        assert result.count() == 0
