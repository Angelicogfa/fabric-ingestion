from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

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

        if self.config.spark_configs:
            for key, value in self.config.spark_configs.items():
                try:
                    self.spark.conf.set(key, value)
                except Exception as exc:
                    self.logger.warning(f"[Config] Não foi possível definir '{key}={value}': {exc}")

        if self.config.v_order:
            self.spark.conf.set(
                "spark.microsoft.delta.vorder.enabled", str(self.config.v_order).lower()
            )
        if self.config.optimize_write:
            self.spark.conf.set(
                "spark.microsoft.delta.optimizeWrite.enabled",
                str(self.config.optimize_write).lower(),
            )
        if self.config.auto_compact:
            self.spark.conf.set(
                "spark.microsoft.delta.autoCompact.enabled", str(self.config.auto_compact).lower()
            )
        if self.config.auto_merge_schema:
            self.spark.conf.set(
                "spark.databricks.delta.schema.autoMerge.enabled",
                str(self.config.auto_merge_schema).lower(),
            )
        if self.config.partition_overwrite_mode:
            self.spark.conf.set(
                "spark.sql.sources.partitionOverwriteMode", self.config.partition_overwrite_mode
            )

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

    def on_optimize_data(self, df: DataFrame, config: PipelineConfig, **kwargs) -> None:  # noqa: B027
        """
        Hook executado após a escrita, caso a otimização esteja habilitada.

        Útil para executar operações de otimização específicas do destino.
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
            self._ensure_destination_exists(df_schema=None, **kwargs)
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
                self._ensure_destination_exists(df_schema=df_filtered.schema, **kwargs)
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

    # ── Helpers internos ──────────────────────────────────────────────────

    def _ensure_destination_exists(
        self,
        df_schema: StructType | None,
        **kwargs,
    ) -> None:
        """
        Garante que o destino Delta existe mesmo quando não há dados para escrever.

        Chamado nos pontos de encerramento antecipado do Template Method para
        evitar que pipelines subsequentes falhem ao tentar ler um destino que
        nunca foi criado.

        Comportamento
        -------------
        - **Destino já existe**: nenhuma ação é tomada.
        - **Destino não existe + schema disponível**: cria um DataFrame vazio
          com o schema fornecido e executa a :attr:`write_strategy`, garantindo
          que a estrutura Delta (``_delta_log/`` + schema) seja criada.
        - **Destino não existe + schema desconhecido** (``df_schema=None``,
          ocorre quando ``load_data`` retorna ``None``): apenas loga um aviso
          e encerra — sem schema de referência não é possível criar o destino.

        Parâmetros
        ----------
        df_schema : StructType | None
            Schema do DataFrame formatado, disponível quando o pipeline chegou
            até a etapa de filtro. ``None`` quando a origem não carregou dados.
        **kwargs
            Argumentos extras repassados ao :meth:`WriteStrategy.execute`
            (chave ``"write"`` é extraída, assim como no Template Method).
        """
        from delta.tables import DeltaTable

        if DeltaTable.isDeltaTable(self.spark, self.config.destiny_path):
            self.logger.info("[EnsureDestination] Destino já existe. Nenhuma ação necessária.")
            return

        if df_schema is None:
            self.logger.warning(
                "[EnsureDestination] Destino não existe e o schema da origem é desconhecido "
                "(load_data retornou None). Não é possível criar o destino automaticamente."
            )
            return

        self.logger.info(
            "[EnsureDestination] Destino não encontrado. "
            "Criando estrutura vazia para garantir disponibilidade do schema em "
            f"pipelines subsequentes: {self.config.destiny_path}"
        )
        empty_df = self.spark.createDataFrame([], df_schema)
        self.write_strategy.execute(
            empty_df,
            self.config,
            self.spark,
            self.logger,
            **kwargs.get("write", {}),
        )
        self.logger.info("[EnsureDestination] ✓ Estrutura do destino criada com sucesso.")
