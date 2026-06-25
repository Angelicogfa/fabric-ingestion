from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from fabric_ingestion.base.pipeline_config import PipelineConfig

from .write_strategy import WriteStrategy


class FullLoadStrategy(WriteStrategy):
    """
    Estratégia de escrita **Full Load**.

    Sobrescreve completamente a tabela Delta de destino usando
    ``mode=overwrite`` com ``overwriteSchema=true``, garantindo que
    alterações de schema na origem sejam propagadas ao destino.

    Indicado para:
    - Carga inicial de dados.
    - Tabelas pequenas onde truncate + insert é mais simples que MERGE.
    - Situações onde o destino possui colunas VOID (NullType) que impedem MERGE.
    """

    def execute(
        self,
        df: DataFrame,
        config: PipelineConfig,
        spark: SparkSession,
        logger: logging.Logger,
        **kwargs,
    ) -> DataFrame:
        logger.info(f"[FullLoad] Iniciando overwrite em: {config.destiny_path}")

        writer = (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("partitionOverwriteMode", "static")
        )

        if config.partition_by:
            logger.info(f"[FullLoad] Particionando por: {config.partition_by}")
            writer = writer.partitionBy(*config.partition_by)

        writer.save(config.destiny_path)

        logger.info(f"[FullLoad] ✓ Concluído: {config.destiny_path}")
        return df
