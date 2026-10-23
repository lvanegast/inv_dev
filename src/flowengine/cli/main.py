"""Interfaz de línea de comandos (CLI) principal de FlowEngine."""

import asyncio
import json
from pathlib import Path

import typer
import uvicorn
import yaml

from flowengine.cli.visualizer import console, print_flow_tree, print_run_result, print_runs_history
from flowengine.core.dag import DAGGraph, DAGValidationError
from flowengine.core.models import FlowDefinition
from flowengine.core.runner import FlowRunner
from flowengine.storage.repository import FlowRepository

app = typer.Typer(
    name="flow",
    help="FlowEngine: Réplica del motor de automatizaciones, DAG y Webhooks estilo Zapier / Make",
    add_completion=False,
)


def load_flow_from_file(path: Path) -> FlowDefinition:
    """Carga un flujo desde archivo YAML o JSON."""
    if not path.exists():
        raise typer.BadParameter(f"El archivo '{path}' no existe.")

    content = path.read_text(encoding="utf-8")
    data = yaml.safe_load(content) if path.suffix.lower() in [".yaml", ".yml"] else json.loads(content)
    return FlowDefinition.model_validate(data)


@app.command()
def validate(
    path: Path = typer.Argument(..., help="Ruta al archivo del flujo (.yaml o .json)"),
):
    """Valida la sintaxis del flujo y la integridad del Grafo (DAG)."""
    try:
        flow = load_flow_from_file(path)
        DAGGraph(flow)
        console.print(f"[bold green][OK] El flujo '{flow.name}' es valido y aciclico.[/bold green]")
        print_flow_tree(flow)
    except DAGValidationError as e:
        console.print(f"[bold red][ERROR] Error en el Grafo (DAG):[/bold red] {e}")
        raise typer.Exit(code=1)
    except Exception as e:
        console.print(f"[bold red][ERROR] Error de validacion:[/bold red] {e}")
        raise typer.Exit(code=1)


@app.command()
def run(
    path: Path = typer.Argument(..., help="Ruta al archivo del flujo (.yaml o .json)"),
    payload: str | None = typer.Option(
        None, "--payload", "-p", help="Payload JSON para el disparador inicial"
    ),
    save: bool = typer.Option(True, "--save/--no-save", help="Guardar ejecución en la base de datos"),
):
    """Ejecuta un flujo localmente de forma inmediata."""
    flow = load_flow_from_file(path)
    trigger_data = {}
    if payload:
        try:
            trigger_data = json.loads(payload)
        except Exception:
            p_path = Path(payload)
            if p_path.exists():
                trigger_data = json.loads(p_path.read_text(encoding="utf-8"))

    console.print(f"[bold cyan][>] Ejecutando flujo:[/bold cyan] {flow.name} [dim]({flow.id})[/dim]")
    print_flow_tree(flow)

    runner = FlowRunner(flow)
    result = asyncio.run(runner.run(trigger_payload=trigger_data))

    print_run_result(result, flow_name=flow.name)

    if save:
        repo = FlowRepository()

        async def _save():
            await repo.initialize()
            await repo.save_flow(flow)
            await repo.save_run(result)

        asyncio.run(_save())
        console.print("[dim green][OK] Ejecucion guardada en la base de datos local.[/dim green]")


@app.command()
def register(
    path: Path = typer.Argument(..., help="Ruta al archivo del flujo (.yaml o .json)"),
):
    """Registra o actualiza un flujo en la base de datos para recibir webhooks."""
    flow = load_flow_from_file(path)
    repo = FlowRepository()

    async def _reg():
        await repo.initialize()
        await repo.save_flow(flow)

    asyncio.run(_reg())
    msg = (
        f"[bold green][OK] Flujo registrado exitosamente.[/bold green]\n"
        f"Webhook URL: [cyan]/webhook/{flow.id}[/cyan]"
    )
    console.print(msg)


@app.command()
def history(
    flow_id: str | None = typer.Option(None, "--flow-id", "-f", help="Filtrar por ID de flujo"),
    limit: int = typer.Option(20, "--limit", "-n", help="Número de ejecuciones a mostrar"),
):
    """Muestra el historial de ejecuciones registradas en la base de datos."""
    repo = FlowRepository()

    async def _hist():
        await repo.initialize()
        return await repo.list_runs(flow_id=flow_id, limit=limit)

    runs = asyncio.run(_hist())
    if not runs:
        console.print("[yellow]No hay ejecuciones registradas todavía.[/yellow]")
        return

    print_runs_history(runs)


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", "--host", "-h", help="Host del servidor"),
    port: int = typer.Option(8000, "--port", "-p", help="Puerto HTTP"),
    reload: bool = typer.Option(False, "--reload", help="Modo auto-recarga para desarrollo"),
    with_scheduler: bool = typer.Option(
        False, "--with-scheduler", help="Iniciar también el planificador en segundo plano (Cron/Interval)"
    ),
):
    """Inicia el servidor de Webhooks y API de FlowEngine."""
    import os

    if with_scheduler:
        os.environ["FLOWENGINE_ENABLE_SCHEDULER"] = "true"
        console.print("[bold yellow]⏰ Scheduler en segundo plano habilitado.[/bold yellow]")

    console.print(f"[bold green]Iniciando FlowEngine Webhook Server en http://{host}:{port}[/bold green]")
    uvicorn.run("flowengine.server.app:app", host=host, port=port, reload=reload)


@app.command()
def schedule():
    """Inicia el planificador autónomo (Scheduler) en primer plano para ejecutar flujos periódicos."""
    import contextlib

    from flowengine.core.scheduler import SchedulerService

    repo = FlowRepository()
    service = SchedulerService(repo)

    async def _run():
        await service.start()
        console.print("[bold green][OK] Scheduler iniciado. Presiona Ctrl+C para detener.[/bold green]")
        try:
            while True:
                await asyncio.sleep(1)
        except (KeyboardInterrupt, asyncio.CancelledError):
            await service.stop()
            console.print("[dim yellow]Scheduler detenido limpiamente.[/dim yellow]")

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(_run())


if __name__ == "__main__":
    app()
