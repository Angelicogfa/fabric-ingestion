from __future__ import annotations

from dataclasses import dataclass, field


_DEFAULT_SPARK_CONFIGS: dict[str, str] = {
    "spark.microsoft.delta.vorder.enabled": "true",
    "spark.microsoft.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.schema.autoMerge.enabled": "true",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
}


@dataclass
class PipelineConfig:
    """
    Configuração de um pipeline de ingestão.

    Agrupa todos os parâmetros que antes eram espalhados no ``__init__``
    da classe base, tornando a construção explícita e testável.

    Parâmetros
    ----------
    origin_path : str
        Caminho absoluto (ABFS ou local) da fonte de dados Delta.
    destiny_path : str
        Caminho absoluto do destino Delta Lake.
    unique_columns : list[str] | None
        Lista de colunas usadas como chave única no destino.
        Obrigatório quando se usa ``MergeStrategy`` (upsert).
    partition_by : list[str] | None
        Lista de colunas usadas para particionar a tabela no destino.
    date_column : str | None
        Nome da coluna de data/timestamp usada para filtros incrementais.
    date_format : str
        Formato da ``date_column``, usado na conversão via Spark ``to_date()``.
        Padrão: ``"yyyy-MM-dd"``.
    spark_configs : dict[str, str]
        Configurações de runtime Spark aplicadas no início do pipeline.
        Por padrão, inclui as otimizações recomendadas para Microsoft Fabric.
    """

    origin_path: str
    destiny_path: str
    unique_columns: list[str] | None = None
    partition_by: list[str] | None = None
    date_column: str | None = None
    date_format: str = "yyyy-MM-dd"
    spark_configs: dict[str, str] = field(
        default_factory=lambda: dict(_DEFAULT_SPARK_CONFIGS)
    )
