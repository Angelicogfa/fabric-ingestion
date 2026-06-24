from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from fabric_ingestion.base.pipeline_config import PipelineConfig


class DedupStep:
    """
    Etapa isolada de deduplicação do DataFrame.

    Estratégia aplicada automaticamente:

    - **Com** ``config.date_column`` configurada e presente no DataFrame:
      mantém apenas a linha mais recente por ``config.unique_columns``
      (Window + ``row_number``). Ideal para fontes que enviam histórico
      completo de updates de um mesmo registro.

    - **Sem** ``config.date_column``:
      utiliza ``dropDuplicates`` com ``config.unique_columns``.
      Mais simples e performático quando não há coluna de versão/data.
    """

    def __init__(self, config: PipelineConfig, logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger

    def run(self, df: DataFrame) -> DataFrame:
        """Executa a deduplicação e retorna o DataFrame deduplicado."""
        date_col = self.config.date_column

        if date_col and date_col in df.columns:
            return self._dedup_by_latest(df, date_col)

        return self._dedup_by_drop(df)

    # ── Estratégias de dedup ──────────────────────────────────────────────

    def _dedup_by_latest(self, df: DataFrame, date_col: str) -> DataFrame:
        """Mantém a linha mais recente por ``unique_columns`` usando Window."""
        self.logger.info(
            f"[Dedup] Estratégia: Window (mais recente por '{date_col}') | "
            f"Chave: {self.config.unique_columns}"
        )
        window_spec = Window.partitionBy(*self.config.unique_columns).orderBy(
            F.col(date_col).desc()
        )
        return (
            df.withColumn("_dedup_row_num", F.row_number().over(window_spec))
            .filter(F.col("_dedup_row_num") == 1)
            .drop("_dedup_row_num")
        )

    def _dedup_by_drop(self, df: DataFrame) -> DataFrame:
        """Remove duplicatas exatas por ``unique_columns``."""
        self.logger.info(
            f"[Dedup] Estratégia: dropDuplicates | Chave: {self.config.unique_columns}"
        )
        return df.dropDuplicates(subset=self.config.unique_columns)
