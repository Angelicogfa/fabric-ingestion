from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from .read_strategy import ReadStrategy


class ParquetReadStrategy(ReadStrategy):
    """
    Estratégia de leitura para arquivos **Parquet simples** (sem estrutura Delta).

    Use esta strategy quando os arquivos de origem são ``.parquet`` puros,
    **sem** ``_delta_log/`` — ou seja, **não** estão no formato Delta Lake.

    Se a pasta de origem contiver ``_delta_log/``, use
    :class:`DeltaReadStrategy` em vez desta.

    Localização típica no Fabric
    ----------------------------
    Arquivos Parquet brutos costumam estar na área de **Files** do Lakehouse,
    mas a localização em si não é o critério — o critério é a **ausência**
    de estrutura Delta::

        # Parquet simples — use ParquetReadStrategy:
        abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Files/raw/orders/

        # Pasta com _delta_log/ — use DeltaReadStrategy mesmo estando em Files:
        abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Files/delta/orders/

    Parâmetros
    ----------
    merge_schema : bool
        Se ``True``, ativa ``mergeSchema`` para lidar com arquivos Parquet
        que possuem schemas ligeiramente diferentes entre partições
        (colunas adicionadas ao longo do tempo). Padrão: ``False``.
    recursive_file_lookup : bool
        Se ``True``, percorre subdiretórios recursivamente para encontrar
        arquivos Parquet. Útil quando os dados estão particionados em
        pastas (ex.: ``year=2024/month=01/``). Padrão: ``True``.

    Exemplos
    --------
    Leitura de diretório particionado com schemas evolutivos::

        pipeline = LakeToLakePipeline(
            spark=spark, config=config,
            read_strategy=ParquetReadStrategy(merge_schema=True),
            write_strategy=FullLoadStrategy(),
        )

    Leitura de arquivo único (sem recursão)::

        pipeline = LakeToLakePipeline(
            spark=spark, config=config,
            read_strategy=ParquetReadStrategy(recursive_file_lookup=False),
            write_strategy=MergeStrategy(),
        )
    """

    def __init__(
        self,
        merge_schema: bool = False,
        recursive_file_lookup: bool = True,
    ) -> None:
        self.merge_schema = merge_schema
        self.recursive_file_lookup = recursive_file_lookup

    @property
    def format_name(self) -> str:
        return "parquet"

    def read(
        self,
        spark: SparkSession,
        path: str,
        logger: logging.Logger,
        **kwargs,
    ) -> DataFrame:
        logger.info(
            f"[Read:Parquet] Caminho: {path} | "
            f"mergeSchema={self.merge_schema} | "
            f"recursiveLookup={self.recursive_file_lookup}"
        )

        reader = (
            spark.read.format("parquet")
            .option("mergeSchema", str(self.merge_schema).lower())
            .option("recursiveFileLookup", str(self.recursive_file_lookup).lower())
        )

        for key, value in kwargs.get("options", {}).items():
            reader = reader.option(key, value)

        df = reader.load(path)
        logger.info(f"[Read:Parquet] Schema: {[f.name for f in df.schema.fields]}")
        return df
