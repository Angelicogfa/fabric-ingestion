from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fabric_ingestion.base.pipeline_base import PipelineBase
from fabric_ingestion.base.pipeline_config import PipelineConfig
from fabric_ingestion.strategies.readers.delta_read_strategy import DeltaReadStrategy
from fabric_ingestion.strategies.readers.read_strategy import ReadStrategy
from fabric_ingestion.strategies.writers.write_strategy import WriteStrategy


class LakeToLakePipeline(PipelineBase):
    """
    Pipeline concreto: **Lakehouse → Lakehouse**.

    Implementa as etapas do Template Method definido em :class:`PipelineBase`
    para o cenário de ingestão entre tabelas/arquivos dentro do
    Microsoft Fabric Lakehouse.

    A estratégia de **leitura** e de **escrita** são injetadas via construtor
    (padrão Strategy), permitindo qualquer combinação de formato de origem
    e modo de escrita.

    Estratégias de leitura disponíveis
    -----------------------------------
    O critério para escolher a strategy é a **estrutura dos arquivos**,
    não a localização (Tables vs Files):

    - :class:`~fabric_ingestion.strategies.delta_read_strategy.DeltaReadStrategy`
      — **padrão** — pasta com ``_delta_log/`` (formato Delta Lake).
      Funciona tanto em ``Tables/`` quanto em ``Files/``.
    - :class:`~fabric_ingestion.strategies.parquet_read_strategy.ParquetReadStrategy`
      — arquivos ``.parquet`` simples, **sem** ``_delta_log/``.
    - :class:`~fabric_ingestion.strategies.spark_format_read_strategy.SparkFormatReadStrategy`
      — qualquer formato Spark: csv, json, orc, avro, xml…

    Estratégias de escrita disponíveis
    ------------------------------------
    - :class:`~fabric_ingestion.strategies.full_load_strategy.FullLoadStrategy`
      — overwrite completo.
    - :class:`~fabric_ingestion.strategies.merge_strategy.MergeStrategy`
      — upsert incremental via Delta MERGE.

    Exemplos
    --------
    **Delta → Delta (MERGE incremental)** — comportamento padrão::

        from fabric_ingestion import (
            PipelineConfig, LakeToLakePipeline, MergeStrategy,
        )

        pipeline = LakeToLakePipeline(
            spark=spark,
            config=PipelineConfig(
                origin_path="abfss://.../lakehouse_a/Tables/orders",
                destiny_path="abfss://.../lakehouse_b/Tables/orders_curated",
                full_load=False,
                unique_columns=["order_id"],
                date_column="updated_at",
            ),
            write_strategy=MergeStrategy(),
            # read_strategy omitido → DeltaReadStrategy() é o padrão
        )
        pipeline.execute(end_date="2024-12-31", start_date="2024-01-01")

    **Parquet (Files) → Delta (Full Load)**::

        from fabric_ingestion import (
            PipelineConfig, LakeToLakePipeline, FullLoadStrategy,
        )
        from fabric_ingestion.strategies import ParquetReadStrategy

        pipeline = LakeToLakePipeline(
            spark=spark,
            config=PipelineConfig(
                origin_path="abfss://.../lakehouse_a/Files/raw/orders",
                destiny_path="abfss://.../lakehouse_b/Tables/orders_refined",
                full_load=True,
                unique_columns=["order_id"],
                partition_by=["year", "month"],
            ),
            read_strategy=ParquetReadStrategy(merge_schema=True),
            write_strategy=FullLoadStrategy(),
        )
        pipeline.execute(end_date="2024-12-31")

    **CSV (Files) → Delta (MERGE)**::

        from fabric_ingestion.strategies import SparkFormatReadStrategy, MergeStrategy

        pipeline = LakeToLakePipeline(
            spark=spark,
            config=config,
            read_strategy=SparkFormatReadStrategy(
                source_format="csv",
                options={"header": "true", "delimiter": ";", "inferSchema": "true"},
            ),
            write_strategy=MergeStrategy(),
        )
    """

    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig,
        write_strategy: WriteStrategy,
        read_strategy: ReadStrategy | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(spark, config, write_strategy, logger)
        # Default: Delta — retrocompatível com código existente
        self.read_strategy: ReadStrategy = read_strategy or DeltaReadStrategy()

    # ── Template Method Steps ─────────────────────────────────────────────

    def load_data(self, path: str, **kwargs) -> DataFrame | None:
        """
        Carrega dados da origem delegando à ``read_strategy`` injetada.

        O formato lido (Delta, Parquet, CSV, etc.) é determinado
        exclusivamente pela estratégia — este método não conhece o formato.

        Parâmetros extras (via ``kwargs``):
            ``options`` (dict): opções extras repassadas ao DataFrameReader
            da estratégia (sobrescrevem as opções do construtor da strategy).

        Retorna
        -------
        DataFrame | None
            DataFrame carregado, ou ``None`` se o path não existir.
        """
        self.logger.info(
            f"[Load] Formato: {self.read_strategy.format_name.upper()} | "
            f"Caminho: {path}"
        )
        try:
            return self.read_strategy.read(
                spark=self.spark,
                path=path,
                logger=self.logger,
                **kwargs,
            )
        except Exception as exc:
            self.logger.error(f"[Load] Falha ao carregar dados de '{path}': {exc}")
            raise

    def format_data(self, df: DataFrame) -> DataFrame:
        """
        Normalização padrão Lake-to-Lake:

        1. Normaliza nomes de colunas: ``strip``, ``lower``, espaços → ``'_'``.
        2. Adiciona coluna de controle ``_ingestion_timestamp`` com o
           timestamp atual (útil para auditoria e rastreabilidade).

        Subclasses podem chamar ``super().format_data(df)`` e aplicar
        transformações adicionais de negócio em seguida.
        """
        self.logger.info(
            "[Format] Normalizando colunas e adicionando _ingestion_timestamp."
        )

        # Normaliza nomes de colunas
        for col_name in df.columns:
            normalized = col_name.strip().lower().replace(" ", "_")
            if normalized != col_name:
                self.logger.info(
                    f"[Format] Renomeando: '{col_name}' → '{normalized}'"
                )
                df = df.withColumnRenamed(col_name, normalized)

        # Adiciona timestamp de controle de ingestão
        df = df.withColumn("_ingestion_timestamp", F.current_timestamp())

        return df

    def filter_data(
        self, df: DataFrame, start_date: str | None, end_date: str
    ) -> DataFrame:
        """
        Filtra o DataFrame pelo intervalo ``[start_date, end_date]``
        com base em ``config.date_column``.

        Comportamento:
        - Se ``config.date_column`` não estiver configurada ou não existir
          no DataFrame, retorna o DataFrame sem filtro (com aviso de log).
        - A comparação é feita via ``to_date`` com ``config.date_format``
          (padrão: ``'yyyy-MM-dd'``), evitando problemas de timezone e
          comparação de strings.
        - A coluna auxiliar ``__filter_date`` é removida do resultado final.
        """
        date_col = self.config.date_column

        if not date_col or date_col not in df.columns:
            self.logger.info(
                "[Filter] 'date_column' não configurada ou ausente no DataFrame. "
                "Nenhum filtro de período aplicado."
            )
            return df

        date_fmt = self.config.date_format or "yyyy-MM-dd"
        self.logger.info(
            f"[Filter] Filtrando por '{date_col}' (formato: {date_fmt}) | "
            f"Período: {start_date or '(sem início)'} → {end_date}"
        )

        # Coluna auxiliar tipada para comparação segura (evita comparação de strings)
        df = df.withColumn(
            "__filter_date", F.to_date(F.col(date_col), date_fmt)
        )

        df = df.filter(F.col("__filter_date") <= F.to_date(F.lit(end_date)))

        if start_date:
            df = df.filter(
                F.col("__filter_date") >= F.to_date(F.lit(start_date))
            )

        return df.drop("__filter_date")
