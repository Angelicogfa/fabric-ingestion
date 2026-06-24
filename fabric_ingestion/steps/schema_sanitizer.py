from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


class SchemaSanitizer:
    """
    Utilitário que alinha o schema do DataFrame de origem ao schema
    Delta do destino, prevenindo erros de MERGE por incompatibilidade de tipos.

    Casos tratados
    --------------
    1. **Coluna NullType no source** → cast para o tipo correspondente no target
       (fallback: ``StringType``).
    2. **Coluna VOID (NullType) no target** → cast forçado para ``NullType``,
       emite warning. Requer FULL LOAD posterior para corrigir a tabela física.
    3. **Coluna presente no target mas ausente no source** → adicionada como
       ``null`` com o tipo do target, evitando ``DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS``.
    4. **Comparação case-insensitive** → nomes de colunas são normalizados para
       lowercase na comparação, mas preservados no resultado.
    """

    def __init__(self, spark: SparkSession, logger: logging.Logger) -> None:
        self.spark = spark
        self.logger = logger

    # ── API pública ───────────────────────────────────────────────────────

    def get_target_schema(self, destiny_path: str):
        """
        Retorna o schema Delta do destino, ou ``None`` se o caminho
        não existir ou não for uma tabela Delta.
        """
        self.logger.info(f"[Schema] Verificando destino: {destiny_path}")
        try:
            self.spark.catalog.refreshByPath(destiny_path)
        except Exception:
            pass  # Ignora: path pode ainda não existir

        if DeltaTable.isDeltaTable(self.spark, destiny_path):
            schema = self.spark.read.format("delta").load(destiny_path).schema
            self.logger.info(
                f"[Schema] Tabela Delta encontrada. "
                f"Colunas: {[f.name for f in schema.fields]}"
            )
            return schema

        self.logger.info("[Schema] Destino não é uma tabela Delta ou ainda não existe.")
        return None

    def sanitize(self, df_origin: DataFrame, target_schema) -> DataFrame:
        """
        Alinha ``df_origin`` ao ``target_schema``.

        Retorna um novo DataFrame com as colunas ajustadas e prontas
        para a operação de MERGE.
        """
        self.logger.info("[Schema] Iniciando sanitização de schema (case-insensitive).")

        target_field_map = (
            {f.name.lower(): f for f in target_schema.fields}
            if target_schema
            else {}
        )
        source_fields_lower = {f.name.lower() for f in df_origin.schema.fields}

        select_expr = [
            self._resolve_column(field, target_field_map.get(field.name.lower()))
            for field in df_origin.schema.fields
        ]

        # Colunas presentes no target mas ausentes no source
        for target_field in (target_schema.fields if target_schema else []):
            if target_field.name.lower() not in source_fields_lower:
                self.logger.info(
                    f"[Schema] Coluna '{target_field.name}' ausente no source → "
                    f"adicionando como null ({target_field.dataType})."
                )
                select_expr.append(
                    F.lit(None).cast(target_field.dataType).alias(target_field.name)
                )

        return df_origin.select(*select_expr)

    # ── Helpers ───────────────────────────────────────────────────────────

    def _resolve_column(self, source_field, target_field):
        """Determina o cast correto para uma coluna do source."""
        if source_field.dataType.typeName() == "null":
            target_type = target_field.dataType if target_field else T.StringType()
            self.logger.info(
                f"[Schema] '{source_field.name}' é NullType no source → "
                f"cast para {target_type}."
            )
            return (
                F.col(source_field.name).cast(target_type).alias(source_field.name)
            )

        if target_field and target_field.dataType.typeName() == "null":
            self.logger.warning(
                f"[Schema] CRÍTICO: '{source_field.name}' é VOID no target. "
                "Forçando cast NullType. Execute um FULL LOAD para corrigir!"
            )
            return (
                F.col(source_field.name).cast(T.NullType()).alias(source_field.name)
            )

        return F.col(source_field.name)
