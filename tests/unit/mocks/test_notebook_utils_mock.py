"""Testes unitários para NotebookUtilsMock."""

from __future__ import annotations

import pytest

from fabric_ingestion.mocks.notebook_utils_mock import NotebookUtilsMock, notebookutils


@pytest.mark.unit
class TestNotebookUtilsMock:
    def test_get_secret_returns_env_var_value(self, monkeypatch):
        """Deve retornar o valor da variável de ambiente quando definida."""
        monkeypatch.setenv("MYKV_MYPASSWORD", "supersecret")

        result = notebookutils.credentials.getSecret("mykv", "mypassword")

        assert result == "supersecret"

    def test_get_secret_raises_when_env_var_not_set(self, monkeypatch):
        """Deve lançar ValueError com mensagem clara quando a variável não existe."""
        monkeypatch.delenv("MYKV_MISSING_SECRET", raising=False)

        with pytest.raises(ValueError, match="MYKV_MISSING_SECRET"):
            notebookutils.credentials.getSecret("mykv", "missing-secret")

    def test_naming_convention_hyphen_to_underscore(self, monkeypatch):
        """Hífens em key_vault e secret_name devem virar underscores no nome da var."""
        monkeypatch.setenv("MEU_KV_DB_PASSWORD", "senha123")

        result = notebookutils.credentials.getSecret("meu-kv", "db-password")

        assert result == "senha123"

    def test_naming_convention_uppercase(self, monkeypatch):
        """key_vault e secret_name devem ser convertidos para UPPER_CASE."""
        monkeypatch.setenv("VAULT_SECRET", "value")

        result = notebookutils.credentials.getSecret("vault", "secret")

        assert result == "value"

    def test_module_level_instance_is_notebookutils_mock(self):
        """A instância global `notebookutils` deve ser do tipo NotebookUtilsMock."""
        assert isinstance(notebookutils, NotebookUtilsMock)

    def test_error_message_contains_env_var_name(self, monkeypatch):
        """Mensagem de erro deve indicar exatamente qual variável configurar."""
        monkeypatch.delenv("MY_VAULT_MY_SECRET", raising=False)

        with pytest.raises(ValueError) as exc_info:
            notebookutils.credentials.getSecret("my-vault", "my-secret")

        assert "MY_VAULT_MY_SECRET" in str(exc_info.value)
