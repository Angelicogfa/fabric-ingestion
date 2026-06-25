"""Testes unitários para ReadStrategies."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from fabric_ingestion.strategies.readers.delta_read_strategy import DeltaReadStrategy
from fabric_ingestion.strategies.readers.parquet_read_strategy import ParquetReadStrategy
from fabric_ingestion.strategies.readers.spark_format_read_strategy import SparkFormatReadStrategy

logger = logging.getLogger(__name__)


@pytest.mark.unit
class TestDeltaReadStrategy:
    def test_format_name_is_delta(self):
        assert DeltaReadStrategy().format_name == "delta"

    def test_reads_delta_table(self, spark: SparkSession, tmp_delta_path):
        """Deve ler uma tabela Delta real criada em diretório temporário."""
        df_written = spark.createDataFrame([("1", "Alice")], ["id", "name"])
        df_written.write.format("delta").mode("overwrite").save(tmp_delta_path)

        result = DeltaReadStrategy().read(spark, tmp_delta_path, logger)

        assert result.count() == 1
        assert result.columns == ["id", "name"]

    def test_time_travel_by_version(self, spark: SparkSession, tmp_delta_path):
        """Deve ler versão específica da tabela via Time Travel."""
        df_v0 = spark.createDataFrame([("1", "v0")], ["id", "value"])
        df_v1 = spark.createDataFrame([("1", "v1")], ["id", "value"])

        df_v0.write.format("delta").mode("overwrite").save(tmp_delta_path)
        df_v1.write.format("delta").mode("overwrite").save(tmp_delta_path)

        result = DeltaReadStrategy(version=0).read(spark, tmp_delta_path, logger)

        assert result.collect()[0]["value"] == "v0"


@pytest.mark.unit
class TestParquetReadStrategy:
    def test_format_name_is_parquet(self):
        assert ParquetReadStrategy().format_name == "parquet"

    def test_reads_parquet_files(self, spark: SparkSession, tmp_path):
        """Deve ler arquivos Parquet de um diretório."""
        parquet_path = str(tmp_path / "parquet_data")
        df_written = spark.createDataFrame([("1", "Alice"), ("2", "Bob")], ["id", "name"])
        df_written.write.format("parquet").mode("overwrite").save(parquet_path)

        result = ParquetReadStrategy().read(spark, parquet_path, logger)

        assert result.count() == 2


@pytest.mark.unit
class TestSparkFormatReadStrategy:
    def test_format_name_matches_source_format(self):
        strategy = SparkFormatReadStrategy(source_format="csv")
        assert strategy.format_name == "csv"

    def test_raises_on_empty_format(self):
        with pytest.raises(ValueError, match="source_format"):
            SparkFormatReadStrategy(source_format="")

    def test_reads_csv_with_header(self, spark: SparkSession, tmp_path):
        """Deve ler CSV com header corretamente."""
        csv_path = str(tmp_path / "data.csv")
        # Cria CSV manualmente
        import csv

        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name"])
            writer.writeheader()
            writer.writerow({"id": "1", "name": "Alice"})
            writer.writerow({"id": "2", "name": "Bob"})

        strategy = SparkFormatReadStrategy(
            source_format="csv",
            options={"header": "true", "inferSchema": "false"},
        )
        result = strategy.read(spark, csv_path, logger)

        assert result.count() == 2
        assert "id" in result.columns
        assert "name" in result.columns

    def test_kwargs_options_override_constructor_options(self, spark: SparkSession, tmp_path):
        """Opções passadas via kwargs devem sobrescrever as do construtor."""
        # Apenas valida que o reader foi construído com as opções corretas
        # usando um mock do SparkSession
        mock_spark = MagicMock(spec=SparkSession)
        mock_reader = MagicMock()
        mock_spark.read = mock_reader
        mock_reader.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.load.return_value = MagicMock()
        mock_reader.load.return_value.schema = MagicMock(fields=[])

        strategy = SparkFormatReadStrategy(
            source_format="csv",
            options={"header": "true"},
        )
        strategy.read(mock_spark, "/some/path", logger, options={"delimiter": ";"})

        # Verifica que ambas as opções foram aplicadas
        option_calls = [str(c) for c in mock_reader.option.call_args_list]
        assert any("header" in c for c in option_calls)
        assert any("delimiter" in c for c in option_calls)
