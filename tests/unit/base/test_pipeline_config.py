"""Testes unitários para PipelineConfig."""

from __future__ import annotations

import pytest

from fabric_ingestion.base.pipeline_config import _DEFAULT_SPARK_CONFIGS, PipelineConfig


@pytest.mark.unit
class TestPipelineConfigDefaults:
    """Comportamento dos valores padrão do dataclass."""

    def test_required_fields_create_minimal_config(self, tmp_path):
        """Deve criar config válida com apenas os campos obrigatórios."""
        config = PipelineConfig(
            origin_path=str(tmp_path / "origin"),
            destiny_path=str(tmp_path / "destiny"),
        )

        assert config.origin_path == str(tmp_path / "origin")
        assert config.destiny_path == str(tmp_path / "destiny")

    def test_default_date_format_is_iso(self, tmp_path):
        """Formato padrão de data deve ser yyyy-MM-dd."""
        config = PipelineConfig(
            origin_path=str(tmp_path / "o"),
            destiny_path=str(tmp_path / "d"),
        )

        assert config.date_format == "yyyy-MM-dd"

    def test_optional_fields_default_to_none(self, tmp_path):
        """unique_columns, partition_by e date_column devem ser None por padrão."""
        config = PipelineConfig(
            origin_path=str(tmp_path / "o"),
            destiny_path=str(tmp_path / "d"),
        )

        assert config.unique_columns is None
        assert config.partition_by is None
        assert config.date_column is None

    def test_default_spark_configs_match_fabric_recommendations(self, tmp_path):
        """Configurações Spark padrão devem incluir otimizações do Fabric."""
        config = PipelineConfig(
            origin_path=str(tmp_path / "o"),
            destiny_path=str(tmp_path / "d"),
        )

        for key, value in _DEFAULT_SPARK_CONFIGS.items():
            assert key in config.spark_configs
            assert config.spark_configs[key] == value

    def test_default_spark_configs_are_isolated_between_instances(self, tmp_path):
        """Mutar spark_configs de uma instância não deve afetar outra."""
        config_a = PipelineConfig(origin_path="a", destiny_path="a")
        config_b = PipelineConfig(origin_path="b", destiny_path="b")

        config_a.spark_configs["custom.key"] = "custom_value"

        assert "custom.key" not in config_b.spark_configs


@pytest.mark.unit
class TestPipelineConfigCustomValues:
    """Configurações customizadas sobrescrevem os padrões."""

    def test_custom_spark_configs_merged(self, tmp_path):
        """Configs customizadas devem ser passadas e acessíveis."""
        custom = {"spark.custom.setting": "true"}
        config = PipelineConfig(
            origin_path=str(tmp_path / "o"),
            destiny_path=str(tmp_path / "d"),
            spark_configs=custom,
        )

        assert config.spark_configs["spark.custom.setting"] == "true"

    def test_partition_by_list_preserved(self, tmp_path):
        """Lista de colunas de particionamento deve ser armazenada corretamente."""
        config = PipelineConfig(
            origin_path=str(tmp_path / "o"),
            destiny_path=str(tmp_path / "d"),
            partition_by=["year", "month"],
        )

        assert config.partition_by == ["year", "month"]

    def test_unique_columns_list_preserved(self, tmp_path):
        """Lista de colunas únicas deve ser armazenada corretamente."""
        config = PipelineConfig(
            origin_path=str(tmp_path / "o"),
            destiny_path=str(tmp_path / "d"),
            unique_columns=["id", "source"],
        )

        assert config.unique_columns == ["id", "source"]
