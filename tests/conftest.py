"""
Fixtures compartilhadas por toda a suíte de testes.

A SparkSession é criada uma única vez por sessão de teste (scope="session")
para evitar o overhead de inicialização do JVM a cada teste.

O Delta Lake é configurado via configure_spark_with_delta_pip, que inclui
automaticamente os JARs necessários para suporte a tabelas Delta locais.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    SparkSession local com suporte a Delta Lake.

    Configurada com:
    - ``local[1]``: single-thread para testes determinísticos e rápidos
    - Delta extensions: suporte a leitura/escrita de tabelas Delta locais
    - Logs reduzidos: apenas WARN para não poluir a saída dos testes
    """
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("fabric-ingestion-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()

    # Reduz verbosidade dos logs do Spark durante os testes
    spark_session.sparkContext.setLogLevel("WARN")
    logging.getLogger("py4j").setLevel(logging.WARNING)

    yield spark_session
    spark_session.stop()


@pytest.fixture
def tmp_delta_path(tmp_path: Path) -> str:
    """Retorna um path temporário (string) para escrita de tabelas Delta nos testes."""
    return str(tmp_path / "delta_table")


@pytest.fixture
def tmp_origin_path(tmp_path: Path) -> str:
    """Retorna um path de origem temporário distinto do de destino."""
    return str(tmp_path / "origin")


@pytest.fixture
def tmp_destiny_path(tmp_path: Path) -> str:
    """Retorna um path de destino temporário distinto do de origem."""
    return str(tmp_path / "destiny")
