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
    full_load : bool
        Se ``True``, sobrescreve completamente o destino (overwrite).
        Se ``False``, executa MERGE incremental.
    unique_columns : list[str]
        Colunas que identificam unicamente um registro.
        Utilizadas como chave de deduplicação e condição de join no MERGE.
    date_column : str | None
        Nome da coluna de data usada para:
        - filtro de período em ``filter_data``.
        - deduplicação por registro mais recente (se configurada).
    date_format : str | None
        Formato da ``date_column`` (ex.: ``'yyyy-MM-dd'``, ``'dd/MM/yyyy'``).
        Padrão: ``'yyyy-MM-dd'`` quando não especificado.
    partition_by : list[str]
        Colunas de particionamento ao escrever no Delta.
    spark_configs : dict[str, str]
        Configurações de runtime Spark aplicadas no início do pipeline.
        Por padrão, inclui as otimizações recomendadas para Microsoft Fabric.
    """

    origin_path: str
    destiny_path: str
    full_load: bool
    unique_columns: list[str]
    date_column: str | None = None
    date_format: str | None = None
    partition_by: list[str] = field(default_factory=list)
    spark_configs: dict[str, str] = field(
        default_factory=lambda: dict(_DEFAULT_SPARK_CONFIGS)
    )
