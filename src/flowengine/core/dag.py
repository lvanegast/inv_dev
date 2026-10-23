"""Motor de Grafos Acíclicos Dirigidos (DAG) para FlowEngine."""

from collections import defaultdict, deque
from typing import Any

from flowengine.core.expressions import SafeConditionEvaluator
from flowengine.core.models import EdgeDefinition, FlowDefinition, NodeDefinition


class DAGValidationError(Exception):
    """Excepción lanzada cuando la estructura del Grafo es inválida."""

    pass


class DAGGraph:
    """Representación y validador del Grafo de un Flujo."""

    def __init__(self, flow: FlowDefinition) -> None:
        self.flow = flow
        self.nodes_by_id: dict[str, NodeDefinition] = {
            flow.trigger.id: flow.trigger,
            **{n.id: n for n in flow.nodes},
        }
        self.outgoing_edges: dict[str, list[EdgeDefinition]] = defaultdict(list)
        self.incoming_edges: dict[str, list[EdgeDefinition]] = defaultdict(list)

        for edge in flow.edges:
            self.outgoing_edges[edge.source].append(edge)
            self.incoming_edges[edge.target].append(edge)

        self.validate()

    def validate(self) -> None:
        """Valida integridad referencial y ausencia de ciclos."""
        # 1. Verificar que todos los nodos referenciados en aristas existan
        for edge in self.flow.edges:
            if edge.source not in self.nodes_by_id:
                raise DAGValidationError(f"El nodo origen '{edge.source}' en la arista no existe.")
            if edge.target not in self.nodes_by_id:
                raise DAGValidationError(f"El nodo destino '{edge.target}' en la arista no existe.")

        # 2. Detección de ciclos usando algoritmo de Kahn (Topological Sort)
        in_degrees = {node_id: 0 for node_id in self.nodes_by_id}
        for edge in self.flow.edges:
            in_degrees[edge.target] += 1

        queue = deque([node_id for node_id, deg in in_degrees.items() if deg == 0])
        visited_count = 0

        while queue:
            node_id = queue.popleft()
            visited_count += 1
            for edge in self.outgoing_edges[node_id]:
                in_degrees[edge.target] -= 1
                if in_degrees[edge.target] == 0:
                    queue.append(edge.target)

        if visited_count != len(self.nodes_by_id):
            raise DAGValidationError(
                "Se detectó un ciclo en el grafo del flujo. Un flujo debe ser un DAG acíclico."
            )

    def get_topological_batches(self) -> list[list[str]]:
        """Devuelve los nodos agrupados en niveles o lotes que se pueden ejecutar en paralelo."""
        in_degrees = {node_id: 0 for node_id in self.nodes_by_id}
        for edge in self.flow.edges:
            in_degrees[edge.target] += 1

        current_batch = [node_id for node_id, deg in in_degrees.items() if deg == 0]
        batches = []

        while current_batch:
            batches.append(current_batch)
            next_batch = []
            for node_id in current_batch:
                for edge in self.outgoing_edges[node_id]:
                    in_degrees[edge.target] -= 1
                    if in_degrees[edge.target] == 0:
                        next_batch.append(edge.target)
            current_batch = next_batch

        return batches

    def should_execute_node(self, node_id: str, context: dict[str, Any]) -> tuple[bool, str | None]:
        """Evalúa si un nodo debe ejecutarse o saltarse basándose en las condiciones de sus aristas entrantes.

        Regla de Router/Bifurcación:
        - Si es el trigger (sin aristas entrantes), siempre se ejecuta.
        - Si tiene aristas entrantes: se ejecuta si al menos una arista entrante se satisfizo.
        """
        incoming = self.incoming_edges.get(node_id, [])
        if not incoming:
            return True, None

        for edge in incoming:
            source_status = context.get(f"__node_status_{edge.source}")
            # Si el nodo padre fue saltado o falló, no podemos satisfacer a través de este enlace
            if source_status != "success":
                continue

            if not edge.condition:
                return True, None

            # Evaluar condición de la arista
            condition_met = SafeConditionEvaluator.evaluate(edge.condition, context)
            if condition_met:
                return True, None

        return False, f"Ninguna condición de entrada se cumplió para el nodo '{node_id}'."

    def find_loop_block(self, iterator_id: str) -> tuple[list[str], str | None]:
        """Encuentra los nodos que pertenecen al bucle entre un Iterator y su Aggregator.

        Retorna: (loop_node_ids_en_orden_topológico, aggregator_node_id_o_None)
        """
        # 1. Buscar si hay un aggregator alcanzable downstream
        visited: set[str] = set()
        queue = deque([iterator_id])
        aggregator_id: str | None = None

        while queue:
            curr = queue.popleft()
            for edge in self.outgoing_edges[curr]:
                target = edge.target
                if target not in visited:
                    visited.add(target)
                    node_def = self.nodes_by_id[target]
                    if node_def.type == "action_aggregator":
                        aggregator_id = target
                        break
                    queue.append(target)
            if aggregator_id:
                break

        if not aggregator_id:
            return [], None

        # 2. Encontrar nodos alcanzables desde el iterator que puedan llegar al aggregator
        reachable_from_iter: set[str] = set()
        q = deque([iterator_id])
        while q:
            curr = q.popleft()
            for edge in self.outgoing_edges[curr]:
                target = edge.target
                if target != aggregator_id and target not in reachable_from_iter:
                    reachable_from_iter.add(target)
                    q.append(target)

        can_reach_agg: set[str] = set()

        def can_reach(nid: str, seen: set[str]) -> bool:
            if nid == aggregator_id:
                return True
            if nid in can_reach_agg:
                return True
            if nid in seen:
                return False
            seen.add(nid)
            for e in self.outgoing_edges[nid]:
                if can_reach(e.target, seen.copy()):
                    can_reach_agg.add(nid)
                    return True
            return False

        loop_nodes = [nid for nid in reachable_from_iter if can_reach(nid, set()) and nid != aggregator_id]

        # Ordenar según orden topológico
        topo_order = [nid for batch in self.get_topological_batches() for nid in batch]
        sorted_loop_nodes = [nid for nid in topo_order if nid in loop_nodes]

        return sorted_loop_nodes, aggregator_id
