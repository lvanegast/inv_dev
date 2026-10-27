"""Visualizador en terminal de alta fidelidad con Rich para FlowEngine compatible con Windows."""

import contextlib
import sys
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree

from flowengine.core.models import ExecutionRun, FlowDefinition, NodeStatus, RunStatus

# Consola con fallback seguro para Windows
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    with contextlib.suppress(Exception):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

console = Console(force_terminal=True, legacy_windows=False if sys.platform == "win32" else None)


def print_flow_tree(flow: FlowDefinition) -> None:
    """Muestra el árbol del flujo con sus dependencias."""
    tree = Tree(f"[bold cyan]Flow: {flow.name}[/bold cyan] [dim]({flow.id})[/dim]")
    trig_name = flow.trigger.name or flow.trigger.id
    trigger_node = tree.add(
        f"[bold yellow]Trigger:[/bold yellow] {trig_name} [dim]({flow.trigger.type})[/dim]"
    )

    # Construir mapa de conexiones
    outgoing = {}
    for edge in flow.edges:
        outgoing.setdefault(edge.source, []).append(edge)

    def add_children(parent_tree_node, source_id: str, visited: set):
        if source_id in visited:
            return
        visited.add(source_id)

        for edge in outgoing.get(source_id, []):
            child_node = flow.get_node(edge.target)
            if not child_node:
                continue

            cond_str = f" [italic magenta](if {edge.condition})[/italic magenta]" if edge.condition else ""
            c_name = child_node.name or child_node.id
            child_branch = parent_tree_node.add(
                f"[bold green]Node:[/bold green] {c_name} [dim]({child_node.type})[/dim]{cond_str}"
            )
            add_children(child_branch, edge.target, visited.copy())

    add_children(trigger_node, flow.trigger.id, set())
    console.print(Panel(tree, title="Estructura del Flujo (DAG)", border_style="cyan"))


def print_run_result(run: ExecutionRun, flow_name: str = "") -> None:
    """Muestra el resultado detallado de la ejecución en una tabla formateada."""
    status_style = "bold green" if run.status == RunStatus.SUCCESS else "bold red"
    name = flow_name or run.flow_id
    st_val = run.status.value.upper()
    title = (
        f"Ejecución {run.run_id} | Flujo: {name} | "
        f"Estado: [{status_style}]{st_val}[/{status_style}] | "
        f"Duración: {run.duration_ms:.2f}ms"
    )

    table = Table(title=title, show_lines=True, header_style="bold blue")
    table.add_column("Nodo ID", style="bold white")
    table.add_column("Estado", justify="center")
    table.add_column("Duración", justify="right")
    table.add_column("Detalle / Salida / Error", style="dim")

    for node_id, step in run.step_results.items():
        if step.status == NodeStatus.SUCCESS:
            st = "[bold green][OK] SUCCESS[/bold green]"
            detail = str(step.output_data)[:100] + ("..." if len(str(step.output_data)) > 100 else "")
        elif step.status == NodeStatus.SKIPPED:
            st = "[bold yellow][-] SKIPPED[/bold yellow]"
            detail = f"[italic yellow]{step.error or 'Condición no cumplida'}[/italic yellow]"
        elif step.status == NodeStatus.FAILED:
            st = "[bold red][ERR] FAILED[/bold red]"
            detail = f"[bold red]{step.error}[/bold red]"
        else:
            st = f"[dim]{step.status.value}[/dim]"
            detail = ""

        table.add_row(
            node_id,
            st,
            f"{step.duration_ms:.2f}ms",
            detail,
        )

    console.print(table)


def print_runs_history(runs: list[dict]) -> None:
    """Muestra el historial de ejecuciones en una tabla."""
    table = Table(title="Historial de Ejecuciones Recientes", show_lines=True, header_style="bold blue")
    table.add_column("Run ID", style="dim")
    table.add_column("Flow ID", style="bold white")
    table.add_column("Estado", justify="center")
    table.add_column("Inicio", justify="center")
    table.add_column("Duración", justify="right")

    for r in runs:
        st = r.get("status", "")
        if st == "success":
            st_formatted = "[bold green]SUCCESS[/bold green]"
        elif st == "failed":
            st_formatted = "[bold red]FAILED[/bold red]"
        else:
            st_formatted = f"[yellow]{st}[/yellow]"

        table.add_row(
            r.get("run_id", "")[:12] + "...",
            r.get("flow_id", ""),
            st_formatted,
            r.get("started_at", "")[:19],
            f"{r.get('duration_ms', 0):.2f}ms",
        )

    console.print(table)


def print_available_nodes(nodes: list[dict[str, Any]]) -> None:
    """Muestra el catálogo de nodos y plugins registrados en una tabla."""
    table = Table(
        title="Catálogo de Nodos y Plugins de FlowEngine",
        show_lines=True,
        header_style="bold cyan",
    )
    table.add_column("Tipo (type)", style="bold yellow")
    table.add_column("Categoría", justify="center")
    table.add_column("Origen", justify="center")
    table.add_column("Clase Python", style="dim")
    table.add_column("Descripción")

    for n in nodes:
        origin = "[blue]Incorporado[/blue]" if n.get("is_builtin") else "[bold magenta]Plugin[/bold magenta]"
        cat = n.get("category", "")
        if cat == "trigger":
            cat_fmt = "[bold yellow]TRIGGER[/bold yellow]"
        elif cat == "batch":
            cat_fmt = "[cyan]BATCH[/cyan]"
        else:
            cat_fmt = "[green]ACTION[/green]"

        table.add_row(
            n.get("type", ""),
            cat_fmt,
            origin,
            n.get("class_name", ""),
            n.get("description", ""),
        )

    console.print(table)
