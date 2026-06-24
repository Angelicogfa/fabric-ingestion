from __future__ import annotations

import logging


class IngestionLogger:
    """
    Factory de ``logging.Logger`` configurado para pipelines de ingestão.

    Suporta encadeamento de métodos (fluent interface) para configuração
    declarativa e legível.

    Uso básico
    ----------
    ::

        from fabric_ingestion.logging.ingestion_logger import IngestionLogger

        logger = IngestionLogger("orders_pipeline").with_default().build()

    Com nível personalizado
    -----------------------
    ::

        logger = (
            IngestionLogger("orders_pipeline")
            .with_default()
            .with_level(logging.DEBUG)
            .build()
        )

    Com handler adicional (ex.: arquivo de log)
    -------------------------------------------
    ::

        import logging
        file_handler = logging.FileHandler("pipeline.log")
        logger = (
            IngestionLogger("orders_pipeline")
            .with_default()
            .with_handler(file_handler)
            .build()
        )
    """

    def __init__(self, logger_name: str) -> None:
        self.logger_name = logger_name
        self.logger = logging.getLogger(logger_name)

    def build(self) -> logging.Logger:
        """Retorna o ``logging.Logger`` configurado."""
        return self.logger

    def with_default(self) -> IngestionLogger:
        """
        Aplica a configuração padrão ao logger:

        - Remove handlers existentes (evita duplicação em re-runs de notebooks).
        - Adiciona ``StreamHandler`` com formato ``timestamp | name | level | message``.
        - Define nível ``INFO``.
        - Desabilita propagação para o root logger.
        """
        for handler in list(self.logger.handlers):
            self.logger.removeHandler(handler)

        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        return self

    def with_level(self, level: int) -> IngestionLogger:
        """Ajusta o nível de log (ex.: ``logging.DEBUG``, ``logging.WARNING``)."""
        self.logger.setLevel(level)
        return self

    def with_handler(self, handler: logging.Handler) -> IngestionLogger:
        """
        Adiciona um handler customizado.

        Útil para integração com sistemas externos de logging como
        Azure Monitor, arquivo de log, ou handlers do Fabric.
        """
        self.logger.addHandler(handler)
        return self
