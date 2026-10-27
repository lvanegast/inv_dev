"""Demostración de Plugin Personalizado para FlowEngine.

Este ejemplo muestra cómo crear un nodo de acción personalizado (por ejemplo, para
enviar notificaciones a Discord o Slack) y registrarlo dinámicamente en FlowEngine.
"""

import asyncio
from typing import Any

from flowengine.core.models import EdgeDefinition, FlowDefinition, NodeDefinition
from flowengine.core.runner import FlowRunner
from flowengine.nodes.base import BaseNode
from flowengine.nodes.registry import list_available_nodes, register_node


class DiscordWebhookAction(BaseNode):
    """Acción de plugin: Envía un mensaje formateado a un Webhook de Discord."""

    async def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        cfg = self.resolve_config(context)
        channel = cfg.get("channel", "general")
        content = cfg.get("content", "")

        # Simulación de despacho a API externa
        print(f" [Discord Plugin] Enviando mensaje a #{channel}: '{content}'")

        return {
            "delivered": True,
            "channel": channel,
            "content": content,
            "bytes_sent": len(content.encode("utf-8")),
        }


async def main():
    print("=== 1. Registrando plugin personalizado 'action_discord' ===")
    register_node("action_discord", DiscordWebhookAction)

    print("\n=== 2. Catálogo actual de nodos en el sistema ===")
    for n in list_available_nodes():
        if not n["is_builtin"]:
            print(f" -> Plugin encontrado: {n['type']} ({n['class_name']}) - {n['description']}")

    print("\n=== 3. Construyendo y ejecutando flujo con el plugin ===")
    flow = FlowDefinition(
        id="demo-plugin-flow",
        name="Discord Notification Flow",
        trigger=NodeDefinition(id="manual_start", type="trigger_manual"),
        nodes=[
            NodeDefinition(
                id="notify_team",
                type="action_discord",
                config={
                    "channel": "dev-ops",
                    "content": "Alerta de despliegue exitoso desde FlowEngine v0.1.0",
                },
            )
        ],
        edges=[EdgeDefinition(source="manual_start", target="notify_team")],
    )

    runner = FlowRunner(flow)
    result = await runner.run()

    print(f"\nResultado de ejecución: {result.status.value.upper()}")
    print("Salida del plugin:", result.step_results["notify_team"].output_data)


if __name__ == "__main__":
    asyncio.run(main())
