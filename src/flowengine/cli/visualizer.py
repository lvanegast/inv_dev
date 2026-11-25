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


def print_dlq_runs(runs: list[dict[str, Any]]) -> None:
    """Muestra las ejecuciones fallidas retenidas en la Dead Letter Queue (DLQ)."""
    table = Table(
        title="Dead Letter Queue (DLQ) - Ejecuciones Fallidas Retenidas",
        show_lines=True,
        header_style="bold red",
    )
    table.add_column("Run ID", style="bold yellow")
    table.add_column("Flow ID", style="bold white")
    table.add_column("Fecha y Hora", justify="center")
    table.add_column("Paso Fallido", style="bold cyan")
    table.add_column("Mensaje de Error", style="bold red")

    for r in runs:
        steps = r.get("step_results", {})
        failed_node = "-"
        for nid, s in steps.items():
            if s.get("status") == "failed":
                failed_node = nid
                break

        err = str(r.get("error") or "Error no especificado")
        if len(err) > 75:
            err = err[:72] + "..."

        table.add_row(
            r.get("run_id", "")[:12] + "...",
            r.get("flow_id", ""),
            r.get("started_at", "")[:19],
            failed_node,
            err,
        )

    console.print(table)


def print_test_suite_results(suite: Any) -> None:
    """Muestra el reporte visual de ejecución de una suite de pruebas para un flujo."""
    status_style = "bold green" if suite.passed else "bold red"
    status_label = "[ÉXITO] TODOS LOS TESTS PASARON" if suite.passed else "[FALLO] ALGUNOS TESTS FALLARON"

    summary_panel = Panel(
        f"[bold cyan]Flujo:[/bold cyan] {suite.flow_name} [dim]({suite.flow_id})[/dim]\n"
        f"[bold]Total Casos:[/bold] {suite.total_cases} | "
        f"[bold green]Exitosos:[/bold green] {suite.passed_cases} | "
        f"[bold red]Fallidos:[/bold red] {suite.failed_cases} | "
        f"[bold yellow]Tiempo:[/bold yellow] {suite.duration_ms}ms\n"
        f"[{status_style}]{status_label}[/{status_style}]",
        title="[bold yellow]Reporte de Pruebas Unitarias de Flujo (Testing Framework)[/bold yellow]",
        border_style="green" if suite.passed else "red",
    )
    console.print(summary_panel)

    table = Table(title="Detalle de Casos de Prueba y Aserciones", show_lines=True)
    table.add_column("Caso de Prueba", style="bold white")
    table.add_column("Estado", justify="center")
    table.add_column("Duración", justify="right")
    table.add_column("Aserciones y Validaciones")

    for case in suite.case_results:
        c_status = "[bold green]PASS[/bold green]" if case.passed else "[bold red]FAIL[/bold red]"
        lines = []
        if case.error:
            lines.append(f"[bold red]Error:[/bold red] {case.error}")
        for a in case.assertion_results:
            icon = "[bold green]✔[/bold green]" if a.passed else "[bold red]✘[/bold red]"
            detail = f" [dim red]({a.error})[/dim red]" if not a.passed and a.error else ""
            lines.append(f"{icon} {a.assertion}{detail}")

        assertions_col = "\n".join(lines) if lines else "[dim]Sin aserciones explícitas[/dim]"
        table.add_row(case.name, c_status, f"{case.duration_ms}ms", assertions_col)

    console.print(table)


def print_connections_table(connections: list[Any]) -> None:
    """Muestra la lista de conexiones registradas en el Connection Vault."""
    table = Table(
        title="Connection Vault - Credenciales Cifradas",
        show_lines=True,
        header_style="bold cyan",
    )
    table.add_column("Connection ID", style="bold yellow")
    table.add_column("Nombre", style="bold white")
    table.add_column("Tipo", style="bold green", justify="center")
    table.add_column("Estado de Cifrado", justify="center")

    for c in connections:
        table.add_row(
            c.id,
            c.name or "-",
            c.type.value if hasattr(c.type, "value") else str(c.type),
            "[bold green]AES-256 Fernet (Cifrado)[/bold green]",
        )

    console.print(table)


def print_flow_versions_table(flow_id: str, versions: list[dict[str, Any]]) -> None:
    """Muestra el historial de versiones inmutables de un flujo."""
    table = Table(
        title=f"Historial de Versiones - Flujo: {flow_id}",
        show_lines=True,
        header_style="bold magenta",
    )
    table.add_column("Versión", style="bold yellow", justify="center")
    table.add_column("Changelog / Descripción", style="bold white")
    table.add_column("Nodos", justify="center")
    table.add_column("Fecha de Creación", justify="center")

    for v in versions:
        table.add_row(
            f"v{v['version']}",
            v.get("changelog") or "-",
            str(v.get("node_count", 0)),
            str(v.get("created_at", ""))[:19],
        )

    console.print(table)


def print_jobs_table(jobs: list[dict[str, Any]]) -> None:
    """Muestra la tabla de trabajos encolados y en segundo plano."""
    table = Table(
        title="Cola de Trabajos Asíncronos (Execution Jobs)",
        show_lines=True,
        header_style="bold cyan",
    )
    table.add_column("Job ID", style="bold yellow")
    table.add_column("Flow ID", style="bold white")
    table.add_column("Estado", justify="center")
    table.add_column("Prioridad", justify="center")
    table.add_column("Creado", justify="center")
    table.add_column("Completado", justify="center")

    status_styles = {
        "queued": "[bold yellow]QUEUED[/bold yellow]",
        "processing": "[bold cyan]PROCESSING[/bold cyan]",
        "completed": "[bold green]COMPLETED[/bold green]",
        "failed": "[bold red]FAILED[/bold red]",
        "cancelled": "[dim]CANCELLED[/dim]",
    }

    for j in jobs:
        st = j.get("status", "queued")
        st_badge = status_styles.get(st, st)
        table.add_row(
            j["job_id"][:12] + "...",
            j["flow_id"],
            st_badge,
            j.get("priority", "normal").upper(),
            str(j.get("created_at", ""))[:19],
            str(j.get("completed_at", "") or "-")[:19],
        )

    console.print(table)


def print_queue_stats(stats: dict[str, Any]) -> None:
    """Muestra métricas del pool de workers y cola."""
    db = stats.get("database", {})
    pool = stats.get("pool", {})

    table = Table(title="Estado del Worker Pool y Colas", header_style="bold green")
    table.add_column("Métrica", style="bold white")
    table.add_column("Valor", style="bold cyan")

    table.add_row("Workers Configurados", str(pool.get("workers_configured", 0)))
    table.add_row("Workers Activos en Ejecución", str(pool.get("active_workers", 0)))
    table.add_row("Profundidad de Cola en Memoria", str(pool.get("queue_depth", 0)))
    table.add_row("Trabajos en Cola (DB)", str(db.get("queued", 0)))
    table.add_row("Trabajos en Procesamiento (DB)", str(db.get("processing", 0)))
    table.add_row("Trabajos Completados Exitosos", str(db.get("completed", 0)))
    table.add_row("Trabajos Fallidos", str(db.get("failed", 0)))
    table.add_row("Trabajos Cancelados", str(db.get("cancelled", 0)))

    console.print(table)


def print_subscriptions_table(subs: list[dict[str, Any]]) -> None:
    """Muestra la tabla de suscripciones activas a eventos de webhook (Fan-Out)."""
    table = Table(
        title="Suscripciones de Webhooks Salientes (CDC / Fan-Out)",
        show_lines=True,
        header_style="bold magenta",
    )
    table.add_column("Subscription ID", style="bold yellow")
    table.add_column("Tema (Topic)", style="bold cyan")
    table.add_column("URL Destino", style="bold white")
    table.add_column("Firma HMAC", justify="center")
    table.add_column("Estado", justify="center")
    table.add_column("Fallos", justify="center")

    for s in subs:
        has_secret = "[bold green]Sí (HMAC-SHA256)[/bold green]" if s.get("secret") else "[dim]No[/dim]"
        st = "[bold green]ACTIVO[/bold green]" if s.get("status") == "active" else "[dim]PAUSADO[/dim]"
        table.add_row(
            s["id"][:12] + "...",
            s.get("topic", "*"),
            s.get("target_url", ""),
            has_secret,
            st,
            str(s.get("failure_count", 0)),
        )

    console.print(table)


def print_templates_table(templates: list[Any]) -> None:
    """Muestra la tabla del catálogo de plantillas y blueprints en la terminal."""
    table = Table(
        title="Catálogo de Plantillas de Flujo (Flow Templates / Blueprints)",
        show_lines=True,
        header_style="bold cyan",
    )
    table.add_column("ID", style="bold yellow", no_wrap=True)
    table.add_column("Plantilla", style="bold white")
    table.add_column("Categoría", justify="center")
    table.add_column("Dificultad", justify="center")
    table.add_column("Etiquetas", style="dim")
    table.add_column("Descripción", style="dim")

    diff_map = {
        "beginner": "[bold green]Principiante[/bold green]",
        "intermediate": "[bold yellow]Intermedio[/bold yellow]",
        "advanced": "[bold magenta]Avanzado[/bold magenta]",
    }

    for t in templates:
        t_id = t.id if hasattr(t, "id") else t.get("id", "")
        name = t.name if hasattr(t, "name") else t.get("name", "")
        icon = t.icon if hasattr(t, "icon") else t.get("icon", "⚡")
        category = t.category if hasattr(t, "category") else t.get("category", "")
        difficulty = t.difficulty if hasattr(t, "difficulty") else t.get("difficulty", "beginner")
        tags = t.tags if hasattr(t, "tags") else t.get("tags", [])
        desc = t.description if hasattr(t, "description") else t.get("description", "")

        diff_badge = diff_map.get(str(difficulty).lower(), str(difficulty))
        tags_str = ", ".join(tags) if isinstance(tags, list) else str(tags)
        short_desc = desc[:70] + "..." if len(desc) > 70 else desc

        table.add_row(
            t_id,
            f"{icon} {name}",
            category.upper(),
            diff_badge,
            tags_str,
            short_desc,
        )

    console.print(table)


def print_template_details(template: Any) -> None:
    """Muestra los detalles completos, variables requeridas y YAML de una plantilla."""
    t_id = template.id if hasattr(template, "id") else template.get("id", "")
    name = template.name if hasattr(template, "name") else template.get("name", "")
    icon = template.icon if hasattr(template, "icon") else template.get("icon", "⚡")
    desc = template.description if hasattr(template, "description") else template.get("description", "")
    cat = template.category if hasattr(template, "category") else template.get("category", "")
    diff = template.difficulty if hasattr(template, "difficulty") else template.get("difficulty", "")
    vars_schema = (
        template.variables_schema
        if hasattr(template, "variables_schema")
        else template.get("variables_schema", {})
    )
    yaml_def = (
        template.definition_yaml
        if hasattr(template, "definition_yaml")
        else template.get("definition_yaml", "")
    )

    console.print(
        Panel(
            f"[bold cyan]ID:[/bold cyan] {t_id}\n"
            f"[bold cyan]Nombre:[/bold cyan] {icon} {name}\n"
            f"[bold cyan]Categoría:[/bold cyan] {cat.upper()} | [bold cyan]Dificultad:[/bold cyan] {diff}\n"
            f"[bold cyan]Descripción:[/bold cyan] {desc}",
            title=f"Detalles de Plantilla: {t_id}",
            border_style="cyan",
        )
    )

    if vars_schema:
        var_table = Table(
            title="Variables Requeridas / Parametrizables",
            show_lines=True,
            header_style="bold yellow",
        )
        var_table.add_column("Variable", style="bold magenta")
        var_table.add_column("Tipo", justify="center")
        var_table.add_column("Valor por Defecto", style="bold green")
        var_table.add_column("Etiqueta / Descripción", style="dim")

        for k, v in vars_schema.items():
            if isinstance(v, dict):
                v_type = v.get("type", "string")
                v_def = str(v.get("default", ""))
                v_label = v.get("label", "") or v.get("description", "")
            else:
                v_type = "string"
                v_def = str(v)
                v_label = ""

            var_table.add_row(f"{{{{vars.{k}}}}}", v_type, v_def, v_label)

        console.print(var_table)

    console.print(
        Panel(
            yaml_def,
            title="Definición YAML de la Plantilla",
            border_style="dim white",
        )
    )
