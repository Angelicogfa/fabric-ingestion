from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession


class ReadStrategy(ABC):
    """
    Interface (padrão **Strategy**) para estratégias de leitura da origem.

    Permite que o :class:`~fabric_ingestion.sources.lake_to_lake.LakeToLakePipeline`
    leia qualquer tipo de fonte de dados sem alteração de código, bastando
    injetar a estratégia adequada no construtor.

    Implementações disponíveis:
        - :class:`~fabric_ingestion.strategies.readers.delta_read_strategy.DeltaReadStrategy`
          — lê qualquer dado no formato Delta Lake (com ``_delta_log/``).
        - :class:`~fabric_ingestion.strategies.readers.parquet_read_strategy.ParquetReadStrategy`
          — lê arquivos Parquet simples (sem ``_delta_log/``).
        - :class:`~fabric_ingestion.strategies.readers.spark_format_read_strategy.SparkFormatReadStrategy`
          — lê qualquer formato suportado pelo Spark (csv, json, orc, avro…).
    """

    @abstractmethod
    def read(
        self,
        spark: SparkSession,
        path: str,
        logger: logging.Logger,
        **kwargs,
    ) -> DataFrame:
        """
        Carrega dados do ``path`` e retorna um DataFrame.

        Parâmetros
        ----------
        spark : SparkSession
            Sessão Spark ativa.
        path : str
            Caminho ABFS ou local da origem dos dados.
        logger : logging.Logger
            Logger do pipeline pai para rastreabilidade.
        **kwargs
            Opções extras específicas de cada implementação.

        Retorna
        -------
        DataFrame
            DataFrame com os dados carregados da origem.
        """

    @property
    def format_name(self) -> str:
        """Nome descritivo do formato (usado em logs)."""
        return self.__class__.__name__.replace("ReadStrategy", "").lower()
