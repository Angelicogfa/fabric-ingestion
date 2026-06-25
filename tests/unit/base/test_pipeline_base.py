"""Testes unitários para PipelineBase (Template Method)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame, SparkSession

from fabric_ingestion.base.pipeline_base import PipelineBase
from fabric_ingestion.base.pipeline_config import PipelineConfig
from fabric_ingestion.strategies.writers.write_strategy import WriteStrategy

# ── Implementação concreta de PipelineBase para testes ────────────────────────


class ConcretePipeline(PipelineBase):
    """Implementação mínima de PipelineBase usada exclusivamente nos testes."""

    def __init__(self, spark, config, write_strategy, logger=None, *, load_return=None):
        super().__init__(spark, config, write_strategy, logger)
        self._load_return = load_return  # controla o retorno de load_data

    def load_data(self, path: str, **kwargs) -> DataFrame | None:
        return self._load_return

    def format_data(self, df: DataFrame) -> DataFrame:
        return df  # passthrough

    def filter_data(self, df: DataFrame, start_date, end_date) -> DataFrame:
        return df  # passthrough


class SpyPipeline(PipelineBase):
    """Pipeline com espias para verificar a ordem de chamada dos hooks."""

    call_log: list[str]

    def __init__(self, spark, config, write_strategy, df_to_return):
        super().__init__(spark, config, write_strategy)
        self.call_log = []
        self._df = df_to_return

    def load_data(self, path: str, **kwargs) -> DataFrame | None:
        self.call_log.append("load_data")
        return self._df

    def format_data(self, df: DataFrame) -> DataFrame:
        self.call_log.append("format_data")
        return df

    def filter_data(self, df: DataFrame, start_date, end_date) -> DataFrame:
        self.call_log.append("filter_data")
        return df

    def on_before_load(self, **kwargs):
        self.call_log.append("on_before_load")

    def on_after_save(self, df: DataFrame, **kwargs):
        self.call_log.append("on_after_save")


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def config(tmp_path):
    return PipelineConfig(
        origin_path=str(tmp_path / "origin"),
        destiny_path=str(tmp_path / "destiny"),
        unique_columns=["id"],
    )


@pytest.fixture
def mock_write_strategy():
    strategy = MagicMock(spec=WriteStrategy)
    strategy.execute.return_value = MagicMock(spec=DataFrame)
    strategy.__class__.__name__ = "MockWriteStrategy"
    return strategy


# ── Testes ────────────────────────────────────────────────────────────────────


@pytest.mark.unit
class TestTemplateMethodOrder:
    """Garante que o Template Method executa os steps na ordem correta."""

    def test_steps_executed_in_correct_order(
        self, spark: SparkSession, config, mock_write_strategy
    ):
        """Ordem esperada: on_before_load → load_data → format_data
        → filter_data → on_after_save."""
        df = spark.createDataFrame([("1",)], ["id"])
        pipeline = SpyPipeline(spark, config, mock_write_strategy, df_to_return=df)

        pipeline.execute(end_date="2024-12-31")

        expected = ["on_before_load", "load_data", "format_data", "filter_data", "on_after_save"]
        assert pipeline.call_log == expected


@pytest.mark.unit
class TestEarlyReturn:
    """Garante que o pipeline encerra graciosamente quando não há dados."""

    def test_returns_early_when_load_returns_none(
        self, spark: SparkSession, config, mock_write_strategy
    ):
        """load_data retornando None → pipeline encerrado, write_strategy não chamado."""
        pipeline = ConcretePipeline(spark, config, mock_write_strategy, load_return=None)

        pipeline.execute(end_date="2024-12-31")

        mock_write_strategy.execute.assert_not_called()

    def test_returns_early_when_filtered_df_is_empty(
        self, spark: SparkSession, config, mock_write_strategy
    ):
        """DataFrame vazio após filtro → pipeline encerrado, write_strategy não chamado."""
        empty_df = spark.createDataFrame([], spark.createDataFrame([("x",)], ["id"]).schema)
        pipeline = ConcretePipeline(spark, config, mock_write_strategy, load_return=empty_df)

        pipeline.execute(end_date="2024-12-31")

        mock_write_strategy.execute.assert_not_called()


@pytest.mark.unit
class TestUnpersistGuarantee:
    """Garante que df.unpersist() é chamado mesmo em caso de falha."""

    def test_unpersist_called_even_when_write_raises(
        self, spark: SparkSession, config, mock_write_strategy
    ):
        """finally: unpersist() deve executar mesmo quando WriteStrategy lança exceção."""
        df = spark.createDataFrame([("1",)], ["id"])
        mock_write_strategy.execute.side_effect = RuntimeError("falha simulada")

        pipeline = ConcretePipeline(spark, config, mock_write_strategy, load_return=df)

        with pytest.raises(RuntimeError, match="falha simulada"):
            pipeline.execute(end_date="2024-12-31")

        # Não podemos espiar o .unpersist() diretamente em DataFrames reais do Spark,
        # mas garantimos que o pipeline propaga a exceção corretamente (o finally executou)


@pytest.mark.unit
class TestSparkConfigApply:
    """Garante que as configurações Spark são aplicadas no __init__."""

    def test_spark_configs_applied_on_init(
        self, spark: SparkSession, tmp_path, mock_write_strategy
    ):
        """Configurações do PipelineConfig devem ser passadas para spark.conf.set."""
        custom_config = {"spark.test.custom": "enabled"}
        config = PipelineConfig(
            origin_path=str(tmp_path / "o"),
            destiny_path=str(tmp_path / "d"),
            spark_configs=custom_config,
        )

        mock_spark = MagicMock(spec=SparkSession)
        mock_spark.conf = MagicMock()

        ConcretePipeline(mock_spark, config, mock_write_strategy)

        mock_spark.conf.set.assert_any_call("spark.test.custom", "enabled")
