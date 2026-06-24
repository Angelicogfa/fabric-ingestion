from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from .read_strategy import ReadStrategy


class SparkFormatReadStrategy(ReadStrategy):
    """
    Estratégia de leitura **genérica** — suporta qualquer formato do Spark.

    Use quando nenhuma das estratégias especializadas cobrir seu caso:
    CSV, JSON, ORC, Avro, JDBC, XML, Excel (com biblioteca adicional), etc.

    Parâmetros
    ----------
    source_format : str
        Formato reconhecido pelo Spark DataFrameReader.
        Exemplos: ``"csv"``, ``"json"``, ``"orc"``, ``"avro"``, ``"xml"``.
    options : dict[str, str] | None
        Opções repassadas ao DataFrameReader via ``.option(k, v)``.
        Cada formato possui suas próprias opções suportadas.

    Exemplos
    --------
    CSV com cabeçalho e delimitador ```;```::

        read_strategy=SparkFormatReadStrategy(
            source_format="csv",
            options={"header": "true", "delimiter": ";", "inferSchema": "true"},
        )

    JSON multiline::

        read_strategy=SparkFormatReadStrategy(
            source_format="json",
            options={"multiLine": "true"},
        )

    ORC::

        read_strategy=SparkFormatReadStrategy(source_format="orc")

    Avro (requer ``spark-avro`` no classpath)::

        read_strategy=SparkFormatReadStrategy(source_format="avro")
    """

    def __init__(
        self,
        source_format: str,
        options: dict[str, str] | None = None,
    ) -> None:
        if not source_format:
            raise ValueError("'source_format' não pode ser vazio.")
        self.source_format = source_format
        self.options = options or {}

    @property
    def format_name(self) -> str:
        return self.source_format

    def read(
        self,
        spark: SparkSession,
        path: str,
        logger: logging.Logger,
        **kwargs,
    ) -> DataFrame:
        logger.info(
            f"[Read:{self.source_format.upper()}] Caminho: {path} | "
            f"Opções: {self.options or '(nenhuma)'}"
        )

        reader = spark.read.format(self.source_format)

        for key, value in self.options.items():
            reader = reader.option(key, value)

        # Opções extras via kwargs sobrescrevem as do construtor
        for key, value in kwargs.get("options", {}).items():
            reader = reader.option(key, value)

        df = reader.load(path)
        logger.info(
            f"[Read:{self.source_format.upper()}] "
            f"Schema: {[f.name for f in df.schema.fields]}"
        )
        return df
