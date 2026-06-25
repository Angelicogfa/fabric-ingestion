from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from fabric_ingestion.base.pipeline_config import PipelineConfig
from fabric_ingestion.steps.dedup_step import DedupStep
from fabric_ingestion.strategies.writers.write_strategy import WriteStrategy


class PipelineBase(ABC):
    """
    Classe base abstrata que implementa o padrão **Template Method**
    para pipelines de ingestão de dados no Microsoft Fabric.

    Fluxo definido pelo Template Method (método ``execute``):

        on_before_load
            → load_data
            → format_data
            → filter_data
            → DedupStep.run
            → WriteStrategy.execute
            → on_after_save

    Subclasses **devem** implementar:
        - :meth:`load_data`
        - :meth:`format_data`
        - :meth:`filter_data`

    Subclasses **podem** sobrescrever os hooks:
        - :meth:`on_before_load`
        - :meth:`on_after_save`

    A estratégia de escrita é injetada via ``write_strategy`` (padrão Strategy),
    permitindo alternar entre :class:`FullLoadStrategy` e :class:`MergeStrategy`
    sem modificar a classe base ou as subclasses.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig,
        write_strategy: WriteStrategy,
        logger: logging.Logger | None = None,
    ) -> None:
        self.spark = spark
        self.config = config
        self.write_strategy = write_strategy
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self._apply_spark_configs()

    # ── Configuração ──────────────────────────────────────────────────────

    def _apply_spark_configs(self) -> None:
        """Aplica as configurações Spark definidas em :class:`PipelineConfig`."""
        for key, value in self.config.spark_configs.items():
            try:
                self.spark.conf.set(key, value)
            except Exception as exc:
                self.logger.warning(f"[Config] Não foi possível definir '{key}={value}': {exc}")

    # ── Contrato de subclasses (steps do Template Method) ─────────────────

    @abstractmethod
    def load_data(self, path: str, **kwargs) -> DataFrame | None:
        """
        Carrega dados da origem.

        Retorna ``None`` se não houver dados disponíveis.
        O pipeline será encerrado graciosamente nesse caso.
        """

    @abstractmethod
    def format_data(self, df: DataFrame) -> DataFrame:
        """Aplica transformações e ajustes de tipagem ao DataFrame."""

    @abstractmethod
    def filter_data(self, df: DataFrame, start_date: str | None, end_date: str) -> DataFrame:
        """Filtra o DataFrame pelo período de interesse."""

    # ── Hooks opcionais ───────────────────────────────────────────────────

    def on_before_load(self, **kwargs) -> None:  # noqa: B027
        """
        Hook executado antes de ``load_data``.

        Útil para validações, autenticação ou setup de recursos externos.
        Implementação padrão é no-op.
        """

    def on_after_save(self, df: DataFrame, **kwargs) -> None:  # noqa: B027
        """
        Hook executado após a persistência bem-sucedida.

        Útil para notificações, atualização de catálogos ou limpeza.
        Implementação padrão é no-op.
        """

    # ── Template Method ───────────────────────────────────────────────────

    def execute(
        self,
        end_date: str,
        start_date: str | None = None,
        **kwargs,
    ) -> None:
        """
        Executa o pipeline completo de ingestão.

        Parâmetros
        ----------
        end_date : str
            Data de corte superior do período de ingestão.
        start_date : str | None
            Data de corte inferior. Sem limite inferior se ``None``.
        **kwargs : dict
            Argumentos extras repassados para os steps:
            - ``kwargs["load"]``  → repassado a :meth:`load_data`
            - ``kwargs["write"]`` → repassado a :meth:`WriteStrategy.execute`
        """
        self.logger.info(
            "\n" + "=" * 52 + "\n"
            f"  Pipeline : {self.__class__.__name__}\n"
            f"  Origem   : {self.config.origin_path}\n"
            f"  Destino  : {self.config.destiny_path}\n"
            f"  Modo     : {self.write_strategy.__class__.__name__}\n"
            f"  Período  : {start_date or '(sem início)'} → {end_date}\n" + "=" * 52
        )

        self.on_before_load(**kwargs)

        # ── 1. Carga ──────────────────────────────────────────────────────
        self.logger.info("[Step 1/4] Carregando dados da origem...")
        df_loaded = self.load_data(self.config.origin_path, **kwargs.get("load", {}))
        if df_loaded is None:
            self.logger.warning("Sem dados na origem. Pipeline encerrado.")
            return

        # ── 2. Formatação ─────────────────────────────────────────────────
        self.logger.info("[Step 2/4] Formatando dados...")
        df_formatted = self.format_data(df_loaded)

        # ── 3. Filtro de período ──────────────────────────────────────────
        self.logger.info("[Step 3/4] Filtrando dados por período...")
        df_filtered = self.filter_data(df_formatted, start_date, end_date)

        # Persiste para evitar re-computação no count + dedup + write
        df_filtered = df_filtered.persist()
        try:
            count = df_filtered.count()
            self.logger.info(f"Registros após filtro: {count:,}")

            if count == 0:
                self.logger.warning("Sem dados após filtro de período. Pipeline encerrado.")
                return

            # ── 4. Deduplicação + Escrita ─────────────────────────────────
            self.logger.info("[Step 4/4] Deduplicando e persistindo no destino...")
            df_deduped = DedupStep(self.config, self.logger).run(df_filtered)

            df_saved = self.write_strategy.execute(
                df_deduped,
                self.config,
                self.spark,
                self.logger,
                **kwargs.get("write", {}),
            )

            self.logger.info("✓ Pipeline concluído com sucesso.")
            self.on_after_save(df_saved, **kwargs)

        finally:
            # Garante liberação de cache mesmo em caso de falha
            df_filtered.unpersist()
