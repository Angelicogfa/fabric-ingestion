from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from fabric_ingestion.base.pipeline_config import PipelineConfig
from fabric_ingestion.steps.schema_sanitizer import SchemaSanitizer

from .write_strategy import WriteStrategy


class MergeStrategy(WriteStrategy):
    """
    Estratégia de escrita incremental via Delta **MERGE** (upsert).

    Comportamento:
    1. Verifica se o destino já existe como tabela Delta.
    2. **Destino inexistente**: executa write inicial para criar a tabela.
    3. **Destino existente**:
       a. Sanitiza o schema da origem alinhando-o ao destino (via :class:`SchemaSanitizer`).
       b. Executa ``MERGE`` usando ``config.unique_columns`` como condição de join.
       c. Loga as métricas da operação (inserções, atualizações, leituras).

    Após o MERGE, as métricas são coletadas do histórico Delta e logadas.
    Falhas na coleta de métricas **não** interrompem o pipeline.
    """

    def execute(
        self,
        df: DataFrame,
        config: PipelineConfig,
        spark: SparkSession,
        logger: logging.Logger,
        **kwargs,
    ) -> DataFrame:
        sanitizer = SchemaSanitizer(spark, logger)
        target_schema = sanitizer.get_target_schema(config.destiny_path)
        target_exists = target_schema is not None

        # ── Destino não existe: cria via write inicial ─────────────────────
        if not target_exists:
            logger.info(
                "[Merge] Tabela Delta de destino não encontrada. Realizando escrita inicial\n"
                "(append)."
            )
            writer = df.write.format("delta").mode("append")

            if config.partition_by:
                writer = writer.partitionBy(*config.partition_by)
            writer.save(config.destiny_path)
            logger.info(f"[Merge] ✓ Tabela criada em: {config.destiny_path}")
            return df

        # ── Destino existe: sanitiza schema e executa MERGE ────────────────
        df_sanitized = sanitizer.sanitize(df, target_schema)

        join_condition = " AND ".join(
            [f"target.{col} = source.{col}" for col in config.unique_columns]
        )
        logger.info(f"[Merge] Condição de join: {join_condition}")

        dt_target = DeltaTable.forPath(spark, config.destiny_path)

        (
            dt_target.alias("target")
            .merge(
                source=df_sanitized.alias("source"),
                condition=join_condition,
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        self._log_merge_metrics(dt_target, logger)
        return df_sanitized

    # ── Helpers ───────────────────────────────────────────────────────────

    def _log_merge_metrics(self, dt_target: DeltaTable, logger: logging.Logger) -> None:
        """
        Coleta e loga as métricas da última operação de MERGE.

        Falhas nesta etapa são logadas como ``warning`` e não interrompem
        o fluxo do pipeline (o MERGE já foi concluído com sucesso).
        """
        try:
            last_tx = dt_target.history(1).collect()[0]
            metrics = last_tx.operationMetrics
            logger.info("─── Métricas do MERGE ──────────────────────────────")
            logger.info(f"  Linhas lidas da origem  : {metrics.get('numSourceRows', '?')}")
            logger.info(f"  Linhas inseridas (novas): {metrics.get('numTargetRowsInserted', '?')}")
            logger.info(f"  Linhas atualizadas      : {metrics.get('numTargetRowsUpdated', '?')}")
            logger.info("────────────────────────────────────────────────────")
        except Exception as exc:
            logger.warning(f"[Merge] MERGE concluído, mas falhou ao coletar métricas: {exc}")
