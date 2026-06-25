from __future__ import annotations

import os


class NotebookUtilsMock:
    """
    Mock local de ``notebookutils`` do Microsoft Fabric.

    Permite executar e testar pipelines fora do ambiente Fabric
    usando variáveis de ambiente no lugar de secrets do Key Vault.

    Convenção de nomes de variável de ambiente
    ------------------------------------------
    O nome da variável é derivado de ``key_vault`` e ``secret_name``
    concatenados com ``_``, normalizados para ``UPPER_CASE`` e com
    hífens substituídos por underscores.

    Exemplo::

        key_vault   = "meu-kv"
        secret_name = "db-password"
        → variável de ambiente: MEU_KV_DB_PASSWORD

    Uso
    ---
    ::

        # Em desenvolvimento local:
        # export MEU_KV_DB_PASSWORD=minha_senha_local

        from fabric_ingestion.mocks.notebook_utils_mock import notebookutils

        password = notebookutils.credentials.getSecret("meu-kv", "db-password")
    """

    class _Credentials:
        @staticmethod
        def getSecret(key_vault: str, secret_name: str) -> str:  # noqa: N802
            """
            Busca um secret como variável de ambiente.

            Lança ``ValueError`` se a variável não estiver definida,
            com mensagem clara indicando qual variável deve ser configurada.
            """
            env_var = f"{key_vault.upper()}_{secret_name.upper()}".replace("-", "_")
            secret = os.getenv(env_var)
            if not secret:
                raise ValueError(
                    f"Secret não encontrado. "
                    f"Para desenvolvimento local, defina a variável de ambiente: {env_var}"
                )
            return secret

    class _FS:
        @staticmethod
        def rm(dir_path: str, recurse: bool = False) -> bool:
            """
            Mock para notebookutils.fs.rm
            No ambiente local, não apaga o caminho físico por segurança.
            Apenas simula sucesso.
            """
            print(
                f"[NotebookUtilsMock] Simulação de exclusão (fs.rm) de '{dir_path}' \n"
                f"(recurse={recurse})"
            )
            return True

    credentials = _Credentials()
    fs = _FS()


# Instância global — espelha a API do `notebookutils` do Microsoft Fabric
notebookutils = NotebookUtilsMock()
