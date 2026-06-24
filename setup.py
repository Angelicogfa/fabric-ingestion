from setuptools import setup, find_packages

# Lê as dependências do arquivo requirements.txt
with open("requirements.txt") as f:
    requirements = f.read().splitlines()

# Remove comentários e linhas vazias
requirements = [
    req.strip() 
    for req in requirements 
    if req.strip() and not req.strip().startswith("#")
]

with open("version.txt") as f:
    version = f.read().strip()

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="fabric-ingestion",
    version=version,
    description="SDK com modelos de ETL para a plataforma de dados Microsoft Fabric",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Guilherme Fernando Angelico",
    author_email="guilherme.fernando@invillia.com",
    packages=find_packages(
        include=["fabric_ingestion", "fabric_ingestion.*"]
    ),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.12",
)