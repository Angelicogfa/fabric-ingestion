from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from fabric_ingestion.base.pipeline_config import PipelineConfig


class WriteStrategy(ABC):
    """
    Interface (padrão **Strategy**) para estratégias de escrita no destino Delta.

    Permite alternar o comportamento de persistência sem alterar a classe base
    ou as subclasses do pipeline.

    Implementações disponíveis:
        - :class:`~fabric_ingestion.strategies.writers.full_load_strategy.FullLoadStrategy`
          — overwrite completo da tabela.
        - :class:`~fabric_ingestion.strategies.writers.merge_strategy.MergeStrategy`
          — upsert incremental via Delta MERGE.
    """

    @abstractmethod
    def execute(
        self,
        df: DataFrame,
        config: PipelineConfig,
        spark: SparkSession,
        logger: logging.Logger,
        **kwargs,
    ) -> DataFrame:
        """
        Persiste ``df`` no destino definido em ``config.destiny_path``.

        Parâmetros
        ----------
        df : DataFrame
            DataFrame já deduplicado, pronto para escrita.
        config : PipelineConfig
            Configurações do pipeline (caminhos, modo, partições, etc.).
        spark : SparkSession
            Sessão Spark ativa.
        logger : logging.Logger
            Logger do pipeline pai.
        **kwargs
            Opções extras repassadas pelo ``execute()`` do pipeline.

        Retorna
        -------
        DataFrame
            O DataFrame que foi efetivamente escrito no destino.
        """
