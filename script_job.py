"""
Job Watchdog - Monitoramento em tempo real de jobs Databricks

Este script monitora jobs ativos e alerta quando excedem o tempo limite configurado.
Pode ser executado como:
- Job agendado no Databricks (serverless para menor custo)
"""

from databricks.sdk import WorkspaceClient
from datetime import datetime, timezone
from typing import Optional
import requests
import os


# =============================================================================
# CONFIGURAÇÕES
# =============================================================================

MAX_RUNTIME_HOURS = 5/60  # Tempo máximo antes de alertar
AUTO_CANCEL = True    # Se True, cancela automaticamente jobs que excedem o limite
OWNER_ONLY = True      # Se True, filtra apenas jobs do usuário atual

# Profile do Databricks CLI (None = usar default ou variáveis de ambiente)
DATABRICKS_PROFILE = "databricks-profile"


# =============================================================================
# FUNÇÕES
# =============================================================================

def get_workspace_client(profile: Optional[str] = None) -> WorkspaceClient:
    """
    Retorna o cliente do Databricks.
    Se executar dentro do Databricks, usa autenticação automática.
    Se executar fora, usa profile ou variáveis de ambiente.
    """
    if profile:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient()


def get_current_user(client: WorkspaceClient) -> str:
    """Retorna o email do usuário atual."""
    return client.current_user.me().user_name


def get_active_long_running_jobs(
    client: WorkspaceClient,
    max_hours: float,
    owner_only: bool = False,
    current_user: Optional[str] = None
) -> list[dict]:
    """
    Lista todos os jobs ativos que estão rodando além do tempo limite.
    Se owner_only=True, filtra apenas jobs do usuário atual.
    """
    long_running_jobs = []

    active_runs = client.jobs.list_runs(active_only=True)

    for run in active_runs:
        if run.start_time is None:
            continue

        # Filtrar por owner se configurado
        if owner_only and current_user:
            if run.creator_user_name != current_user:
                continue

        start_time = datetime.fromtimestamp(run.start_time / 1000, tz=timezone.utc)
        runtime_hours = (datetime.now(timezone.utc) - start_time).total_seconds() / 3600

        if runtime_hours > max_hours:
            long_running_jobs.append({
                "run_id": run.run_id,
                "job_id": run.job_id,
                "run_name": run.run_name or f"Job {run.job_id}",
                "hours_running": round(runtime_hours, 2),
                "started_by": run.creator_user_name or "unknown",
                "start_time": start_time.isoformat(),
                "run_page_url": run.run_page_url
            })

    return long_running_jobs


def cancel_jobs(client: WorkspaceClient, jobs: list[dict]) -> list[dict]:
    """
    Cancela os jobs especificados e retorna a lista dos que foram cancelados.
    """
    cancelled = []

    for job in jobs:
        try:
            client.jobs.cancel_run(job["run_id"])
            print(f"Job cancelado: {job['run_name']} (run_id: {job['run_id']})")
            cancelled.append(job)
        except Exception as e:
            print(f"Erro ao cancelar job {job['run_name']}: {e}")

    return cancelled


def run_watchdog(
    max_hours: float = MAX_RUNTIME_HOURS,
    auto_cancel: bool = AUTO_CANCEL,
    owner_only: bool = OWNER_ONLY,
    webhook_url: Optional[str] = None,
    profile: Optional[str] = DATABRICKS_PROFILE
) -> dict:
    """
    Executa o watchdog completo:
    1. Busca jobs de longa duração
    2. Envia alertas
    3. Opcionalmente cancela os jobs

    Retorna um resumo da execução.
    """

    client = get_workspace_client(profile)

    # Identificar usuário atual se filtro de owner estiver ativo
    current_user = None
    if owner_only:
        current_user = get_current_user(client)
        print(f"Iniciando watchdog - Limite: {max_hours}h | Owner: {current_user}")
    else:
        print(f"Iniciando watchdog - Limite: {max_hours}h | Todos os jobs")
    print("-" * 50)

    # Buscar jobs problemáticos
    long_running_jobs = get_active_long_running_jobs(client, max_hours, owner_only, current_user)

    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "jobs_found": len(long_running_jobs),
        "jobs": long_running_jobs,
        "alert_sent": False,
        "jobs_cancelled": []
    }

    if not long_running_jobs:
        print("✅ Nenhum job excedendo o tempo limite.")
        return result

    # Mostrar jobs encontrados
    print(f"⚠️ Encontrado(s) {len(long_running_jobs)} job(s) excedendo {max_hours}h:\n")
    for job in long_running_jobs:
        print(f"  • {job['run_name']}")
        print(f"    Tempo: {job['hours_running']}h | Por: {job['started_by']}")
        print(f"    URL: {job['run_page_url']}\n")


    # Cancelar se configurado
    if auto_cancel:
        print("\n🛑 Auto-cancel ativado. Cancelando jobs...")
        result["jobs_cancelled"] = cancel_jobs(client, long_running_jobs)

    return result


# =============================================================================
# EXECUÇÃO
# =============================================================================

if __name__ == "__main__":
    result = run_watchdog()
    print("\n" + "=" * 50)
    print(f"Resumo: {result['jobs_found']} job(s) encontrado(s)")
    if result["jobs_cancelled"]:
        print(f"Jobs cancelados: {len(result['jobs_cancelled'])}")



