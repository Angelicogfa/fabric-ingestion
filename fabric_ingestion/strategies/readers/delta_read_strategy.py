from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from .read_strategy import ReadStrategy


class DeltaReadStrategy(ReadStrategy):
    """
    Estratégia de leitura para dados no **formato Delta Lake**.

    O que determina se um dado é "Delta" é a presença do diretório
    ``_delta_log/`` na pasta, **não** a localização dentro do Lakehouse.
    Portanto, esta strategy funciona para **ambos os casos**:

    - **Lakehouse Tables** — tabelas gerenciadas pelo Fabric::

        abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/<tabela>

    - **Lakehouse Files** — diretórios Delta armazenados na área de arquivos,
      desde que contenham ``_delta_log/``::

        abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Files/<caminho>

    .. note::
        A chamada ``spark.read.format("delta").load(path)`` resolve o path
        pelo conteúdo do diretório (``_delta_log/`` + arquivos ``.parquet``),
        independentemente de estar em ``Tables/`` ou ``Files/``.
        Use :class:`ParquetReadStrategy` apenas quando os arquivos forem
        ``.parquet`` simples **sem** estrutura Delta (sem ``_delta_log/``).

    Parâmetros
    ----------
    version : int | None
        Versão específica do Delta para leitura (Time Travel por versão).
        Se ``None``, lê a versão mais recente.
    timestamp : str | None
        Timestamp para leitura via Time Travel (ex.: ``"2024-01-15T10:00:00"``).
        Ignorado se ``version`` estiver definido.

    Exemplos
    --------
    **Delta em Tables** (comportamento padrão)::

        config = PipelineConfig(
            origin_path="abfss://.../lakehouse/Tables/orders", ...
        )
        pipeline = LakeToLakePipeline(
            spark=spark, config=config,
            read_strategy=DeltaReadStrategy(),
            write_strategy=MergeStrategy(),
        )

    **Delta em Files** (mesmo código, path diferente)::

        config = PipelineConfig(
            origin_path="abfss://.../lakehouse/Files/raw/orders_delta",
            # ^ pasta com _delta_log/ + arquivos .parquet internos
            ...
        )
        pipeline = LakeToLakePipeline(
            spark=spark, config=config,
            read_strategy=DeltaReadStrategy(),   # funciona igual
            write_strategy=MergeStrategy(),
        )

    **Time Travel por versão**::

        read_strategy=DeltaReadStrategy(version=42)

    **Time Travel por timestamp**::

        read_strategy=DeltaReadStrategy(timestamp="2024-06-01T00:00:00")
    """

    def __init__(
        self,
        version: int | None = None,
        timestamp: str | None = None,
    ) -> None:
        self.version = version
        self.timestamp = timestamp

    @property
    def format_name(self) -> str:
        return "delta"

    def read(
        self,
        spark: SparkSession,
        path: str,
        logger: logging.Logger,
        **kwargs,
    ) -> DataFrame:
        logger.info(f"[Read:Delta] Caminho: {path}")

        reader = spark.read.format("delta")

        if self.version is not None:
            logger.info(f"[Read:Delta] Time Travel → versão: {self.version}")
            reader = reader.option("versionAsOf", self.version)
        elif self.timestamp is not None:
            logger.info(f"[Read:Delta] Time Travel → timestamp: {self.timestamp}")
            reader = reader.option("timestampAsOf", self.timestamp)

        for key, value in kwargs.get("options", {}).items():
            reader = reader.option(key, value)

        df = reader.load(path)
        logger.info(f"[Read:Delta] Schema: {[f.name for f in df.schema.fields]}")
        return df
