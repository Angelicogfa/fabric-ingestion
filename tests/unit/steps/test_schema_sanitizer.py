"""Testes unitários para SchemaSanitizer."""

from __future__ import annotations

import logging

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T  # noqa: N812

from fabric_ingestion.steps.schema_sanitizer import SchemaSanitizer

logger = logging.getLogger(__name__)


def _make_schema(*fields) -> T.StructType:
    """Helper para criar StructType de forma concisa nos testes."""
    return T.StructType(list(fields))


@pytest.mark.unit
class TestGetTargetSchema:
    """SchemaSanitizer.get_target_schema com destino mockado."""

    def test_returns_none_when_path_is_not_delta(self, spark: SparkSession, tmp_path):
        """Deve retornar None se o caminho não for uma tabela Delta."""
        sanitizer = SchemaSanitizer(spark, logger)

        result = sanitizer.get_target_schema(str(tmp_path / "nao_existe"))

        assert result is None

    def test_returns_schema_when_delta_table_exists(self, spark: SparkSession, tmp_delta_path):
        """Deve retornar o schema quando a tabela Delta existe."""
        # Cria uma tabela Delta real no path temporário
        data = [("1", "Alice"), ("2", "Bob")]
        df = spark.createDataFrame(data, ["id", "name"])
        df.write.format("delta").mode("overwrite").save(tmp_delta_path)

        sanitizer = SchemaSanitizer(spark, logger)
        schema = sanitizer.get_target_schema(tmp_delta_path)

        assert schema is not None
        column_names = [f.name for f in schema.fields]
        assert "id" in column_names
        assert "name" in column_names


@pytest.mark.unit
class TestSanitize:
    """SchemaSanitizer.sanitize — alinhamento de schemas."""

    def test_nulltype_source_cast_to_target_type(self, spark: SparkSession):
        """Coluna NullType no source deve ser castada para o tipo do target."""
        source_data = [(None,)]
        df = spark.createDataFrame(
            source_data,
            T.StructType(
                [
                    T.StructField("value", T.NullType(), True),
                ]
            ),
        )

        target_schema = _make_schema(
            T.StructField("value", T.LongType(), True),
        )

        sanitizer = SchemaSanitizer(spark, logger)
        result = sanitizer.sanitize(df, target_schema)

        result_field = [f for f in result.schema.fields if f.name == "value"][0]
        assert result_field.dataType == T.LongType()

    def test_nulltype_source_without_target_casts_to_string(self, spark: SparkSession):
        """NullType no source sem campo correspondente no target → cast para StringType."""
        source_data = [(None, "Alice")]
        df = spark.createDataFrame(
            source_data,
            T.StructType(
                [
                    T.StructField("extra", T.NullType(), True),
                    T.StructField("name", T.StringType(), True),
                ]
            ),
        )

        # target_schema não contém "extra"
        target_schema = _make_schema(
            T.StructField("name", T.StringType(), True),
        )

        sanitizer = SchemaSanitizer(spark, logger)
        result = sanitizer.sanitize(df, target_schema)

        extra_field = [f for f in result.schema.fields if f.name == "extra"][0]
        assert extra_field.dataType == T.StringType()

    def test_column_missing_in_source_added_as_null(self, spark: SparkSession):
        """Coluna presente no target mas ausente no source → adicionada como null."""
        source_data = [("1",)]
        df = spark.createDataFrame(source_data, ["id"])

        target_schema = _make_schema(
            T.StructField("id", T.StringType(), True),
            T.StructField("created_at", T.TimestampType(), True),  # ausente no source
        )

        sanitizer = SchemaSanitizer(spark, logger)
        result = sanitizer.sanitize(df, target_schema)

        assert "created_at" in result.columns
        assert result.filter(result["created_at"].isNotNull()).count() == 0

    def test_case_insensitive_column_matching(self, spark: SparkSession):
        """Comparação de nomes de coluna deve ser case-insensitive."""
        source_data = [("1", "Alice")]
        df = spark.createDataFrame(source_data, ["ID", "Name"])  # uppercase

        target_schema = _make_schema(
            T.StructField("id", T.StringType(), True),  # lowercase no target
            T.StructField("name", T.StringType(), True),
        )

        sanitizer = SchemaSanitizer(spark, logger)
        # Não deve lançar exceção — deve reconhecer ID→id e Name→name
        result = sanitizer.sanitize(df, target_schema)

        assert result.count() == 1

    def test_void_target_emits_warning(self, spark: SparkSession, caplog):
        """Coluna NullType no target deve emitir um WARNING."""
        source_data = [("Alice",)]
        df = spark.createDataFrame(
            source_data,
            T.StructType(
                [
                    T.StructField("name", T.StringType(), True),
                ]
            ),
        )

        target_schema = _make_schema(
            T.StructField("name", T.NullType(), True),  # VOID no target
        )

        sanitizer = SchemaSanitizer(spark, logger)
        with caplog.at_level(logging.WARNING):
            sanitizer.sanitize(df, target_schema)

        assert any("VOID" in msg or "CRÍTICO" in msg for msg in caplog.messages)
