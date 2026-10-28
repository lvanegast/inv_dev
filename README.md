# FlowEngine ⚡
> **Réplica de alto rendimiento del motor de orquestación, DAG y Webhooks estilo Zapier / Make.com**
> Desarrollado en Python moderno (3.11+) gestionado con [`uv`](https://github.com/astral-sh/uv).

[![CI Tests](https://img.shields.io/badge/tests-96%20passed-brightgreen.svg)](tests/)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue.svg)](pyproject.toml)
[![Toolchain](https://img.shields.io/badge/toolchain-uv-purple.svg)](https://docs.astral.sh/uv/)
[![Linter](https://img.shields.io/badge/linter-ruff-black.svg)](https://docs.astral.sh/ruff/)

---

## 📖 Contexto & Metodología de Réplica (REA Framework)

Las plataformas SaaS comerciales como **Zapier** y **Make.com** dominan la orquestación de automatizaciones comerciales, pero cobran suscripciones elevadas y limitan el número mensual de ejecuciones ("zaps" o tareas).

Aplicando la metodología del repositorio **REA (`morluto/rea`)** (Reverse Engineer Anything), este proyecto realiza una **reconstrucción limpia (*clean-room*)** de los componentes esenciales que dan vida a un motor de automatizaciones empresarial:

1. **Grafo Acíclico Dirigido (DAG):** Resolución matemática de dependencias entre pasos con ordenación topológica y paralelismo nativo.
2. **Evaluador Seguro de Expresiones:** Resolución de variables dinámicas `{{trigger.body.cliente.email}}` y operadores condicionales sin el riesgo de `eval()`.
3. **Enrutamiento Condicional (Routers If/Else):** Bifurcación inteligente de ramas donde solo los nodos cuyas aristas cumplan las condiciones se ejecutan, saltando (*SKIPPED*) las ramas inactivas en tiempo constante.
4. **Resiliencia & Exponential Backoff:** Reintentos automáticos configurables por nodo ante caídas de red de APIs externas.
5. **Persistencia Local de Alta Velocidad:** Base de datos SQLite embebida en modo `WAL` (*Write-Ahead Logging*) que guarda el historial de auditoría de cada ejecución y los payloads de cada paso en submilisegundos.

---

## 🚀 Inicio Rápido con `uv`

Este proyecto utiliza el gestor ultra rápido [`uv`](https://github.com/astral-sh/uv).

### 1. Clonar e instalar dependencias
```powershell
cd C:\Proyectos\inv_dev
uv sync
```

### 2. Validar la estructura de un flujo (DAG)
```powershell
uv run flow validate examples/demo_zap.yaml
```

### 3. Ejecutar un flujo de inmediato
```powershell
# Caso 1: Pedido confirmado (ejecuta rama VIP y cálculo de puntos)
uv run flow run examples/demo_zap.yaml --payload '{"body": {"status": "confirmed", "total": 120.0, "items": 3, "customer": {"first_name": "Leonardo", "last_name": "Vanegas", "email": "leo@example.com"}}}'

# Caso 2: Pedido cancelado (bifurca hacia rama de descarte)
uv run flow run examples/demo_zap.yaml --payload '{"body": {"status": "cancelled", "total": 40.0, "customer": {"first_name": "Carlos", "email": "carlos@example.com"}}}'
```

### 4. Consultar el historial de ejecuciones en terminal
```powershell
uv run flow history
```

### 5. Iniciar el Servidor y Dashboard Web Interactivo
```powershell
# Registrar los flujos de ejemplo en la base de datos
uv run flow register examples/demo_zap.yaml
uv run flow register examples/batch_order_processing.yaml
uv run flow register examples/functions_and_env_demo.yaml
uv run flow register examples/scheduled_health_check.yaml

# Levantar el servidor y dashboard
uv run flow serve --port 8000
```

Abre tu navegador en **`http://127.0.0.1:8000/dashboard`** para disfrutar de:
* **Visualizador del DAG:** Arquitectura visual de los nodos y aristas de cada flujo en tiempo real.
* **Test Runner Interactivo:** Envío de payloads JSON arbitrarios con retroalimentación instantánea y cálculo de latencia.
* **Historial de Ejecuciones en Vivo:** Monitoreo con auto-refresh (3s) y desglose de auditoría paso a paso (`StepResult`).
* **Métricas Globales:** Total de flujos, ejecuciones, tasa de éxito % y tiempo promedio.

También puedes disparar webhooks externamente mediante `curl` o cualquier cliente HTTP:
```bash
curl -X POST http://127.0.0.1:8000/webhook/order-processing-zap \
  -H "Content-Type: application/json" \
  -d '{"status": "confirmed", "total": 95.0, "customer": {"first_name": "Ana", "email": "ana@example.com"}}'
```

---

## 🛠️ Catálogo de Nodos y Sistema de Plugins

FlowEngine incluye nodos esenciales listos para producción y un **Registro Dinámico de Plugins** extensible mediante `register_node` o a través de `entry-points` de Python (`flowengine.nodes`):

```powershell
# Inspeccionar todos los nodos integrados y plugins instalados
uv run flow nodes
```

| Tipo de Nodo | Categoría | Origen | Descripción |
| :--- | :--- | :--- | :--- |
| `trigger_webhook` | Trigger | Incorporado | Despierta el flujo al recibir una llamada HTTP POST/GET externa. |
| `trigger_manual` | Trigger | Incorporado | Disparo manual por CLI o scripts. |
| `trigger_schedule` | Trigger | Incorporado | Temporizador basado en intervalos o expresiones cron. |
| `action_http` | Acción | Incorporado | Llamadas HTTP asíncronas (GET, POST, PUT, DELETE) con autenticación Bearer/Basic. |
| `action_filter` | Acción | Incorporado | Filtro condicional (*Only continue if...*). |
| `action_transform` | Acción | Incorporado | Mapeo y formateo de estructuras de datos JSON. |
| `action_code` | Acción | Incorporado | Ejecución de funciones de transformación personalizadas en Python. |
| `action_log` | Acción | Incorporado | Registro de eventos y alertas formateadas en consola. |
| `action_iterator` | Lotes | Incorporado | Desempaqueta un array para ejecutar pasos posteriores por cada elemento (`{{item}}`). |
| `action_aggregator` | Lotes | Incorporado | Agrupa y consolida los resultados del bucle (sumatorias, listas, conteos). |
| `* (plugins)` | Variable | Externo | Nodos desarrollados por terceros registrados dinámicamente. |

### Cómo Crear un Plugin Personalizado
Los paquetes externos o módulos locales pueden definir nodos heredando de `BaseNode`:
```python
from flowengine.nodes.base import BaseNode
from flowengine.nodes.registry import register_node

class DiscordNotifier(BaseNode):
    async def execute(self, context: dict) -> dict:
        cfg = self.resolve_config(context)
        return {"delivered": True, "message": cfg.get("content")}

register_node("action_discord", DiscordNotifier)
```

O vía `pyproject.toml` usando entrypoints estándar:
```toml
[project.entry-points."flowengine.nodes"]
action_discord = "mi_paquete.discord:DiscordNotifier"
```

---

## 🛡️ Ramas de Respaldo ante Fallos ("On Error" Branches)

FlowEngine soporta bifurcaciones de rescate en el DAG mediante el atributo `on_error: true` en las aristas:

```yaml
edges:
  - source: api_pago
    target: enviar_recibo
    on_error: false        # Rama de éxito

  - source: api_pago
    target: alertar_soporte_slack
    on_error: true         # Se activa si 'api_pago' falla o agota sus reintentos
```

Cuando un nodo falla, el contexto de los nodos de rescate recibe automáticamente el objeto `{{error}}` con el detalle del error y el nodo origen para auditoría inmediata.

---

## 📬 Dead Letter Queue (DLQ) & Reintentos Manuales (Smart Resume)

FlowEngine almacena todas las ejecuciones fallidas en una **Dead Letter Queue (DLQ)** persistente, permitiendo reintentar ejecuciones directamente desde la CLI o el Dashboard Web interactivo:

```powershell
# 1. Consultar la lista de ejecuciones fallidas retenidas en la DLQ
uv run flow dlq

# 2. Reintentar una ejecución con Smart Resume (reutiliza pasos exitosos y reanuda desde el paso fallido)
uv run flow retry <run_id> --resume

# 3. O reiniciar la corrida completa desde cero con el payload original
uv run flow retry <run_id> --full
```

* **Smart Resume:** A diferencia de un reintento ciego, FlowEngine preserva los outputs de los nodos previos que ya se completaron con éxito (ej. cobro de tarjeta o creación de registro), evitando duplicación de operaciones y reanudando la ejecución únicamente desde el nodo que originó la falla.
* **Integración API y Dashboard:** Endpoint `GET /api/dlq` para monitoreo y botón interactivo `🔄 Reintentar` en cada corrida fallida del historial.

---

## 📜 Especificación OpenAPI 3.1 Automática por Flujo

Cada webhook registrado cuenta con su propio contrato OpenAPI 3.1 generado en tiempo real con documentación de esquemas, parámetros y autenticación HMAC/Bearer:

```powershell
# Generar contrato OpenAPI 3.1 desde la terminal
uv run flow openapi examples/secure_stripe_webhook.yaml

# O exportar a un archivo JSON
uv run flow openapi examples/secure_stripe_webhook.yaml -o stripe_api.json
```

Con el servidor en ejecución (`flow serve`), cada flujo expone:
* **Especificación JSON:** `GET /api/flows/{flow_id}/openapi.json`
* **Swagger UI Interactivo Dedicado:** `GET /api/flows/{flow_id}/docs`
* **Catálogo Global de Nodos:** `GET /api/nodes`

---

## 🧮 Biblioteca de Funciones y Secretos de Entorno

FlowEngine permite el uso de funciones y variables de entorno directamente en las plantillas `{{...}}`:

* **Variables de entorno y secretos:** `{{env.API_KEY}}`, `{{secrets.DATABASE_URL}}`.
* **Texto:** `{{upper(nombre)}}`, `{{lower(email)}}`, `{{trim(campo)}}`, `{{title(texto)}}`, `{{slugify(titulo)}}`, `{{replace(texto, 'viejo', 'nuevo')}}`.
* **Matemáticas y Aritmética:** `{{round(total * 0.19, 2)}}`, `{{abs(valor)}}`, `{{min(...)}}`, `{{max(...)}}`, `{{sum(items)}}`.
* **Fechas:** `{{now()}}`, `{{format_date(fecha, '%d/%m/%Y')}}`, `{{add_days(fecha, 7)}}`.
* **Serialización:** `{{json(objeto)}}`.

---

## ⏰ Planificador Autónomo (Cron & Intervalos)

FlowEngine incluye un motor planificador en segundo plano que vigila y dispara flujos desatendidos con temporizadores periódicos o expresiones Cron estándar de 5 campos:

```yaml
trigger:
  id: timer_trigger
  name: "Temporizador de Monitoreo"
  type: trigger_schedule
  config:
    interval_seconds: 60  # Ejecutar cada 60 segundos
    # O bien cron estándar:
    # cron: "*/5 * * * *"  # Cada 5 minutos
```

Puedes ejecutar el scheduler de forma dedicada o junto al servidor de webhooks:

```powershell
# Opción A: Modo scheduler dedicado en terminal
uv run flow schedule

# Opción B: Servidor HTTP con worker del Scheduler integrado
uv run flow serve --port 8000 --with-scheduler
```

---

## 🔐 Seguridad y Verificación Criptográfica de Webhooks

FlowEngine protege los endpoints de webhooks con verificación criptográfica de firma **HMAC-SHA256**, compatible con los estándares de **Stripe, GitHub, Shopify y Slack**:

```yaml
trigger:
  id: stripe_trigger
  type: trigger_webhook
  config:
    auth:
      type: hmac_sha256
      secret: "{{env.STRIPE_WEBHOOK_SECRET}}"  # Variable de entorno — nunca en texto plano
      header: "Stripe-Signature"               # Cabecera donde viene la firma
      signature_prefix: ""                     # Prefijo a ignorar antes del hex (ej. "sha256=")
```

También soporta autenticación **Bearer Token** y **Basic Auth**:
```yaml
config:
  auth:
    type: bearer
    token: "{{env.API_SECRET_TOKEN}}"
```

Las firmas se verifican en **tiempo constante** (`hmac.compare_digest`) para prevenir timing attacks. Llamadas sin firma válida reciben `401 Unauthorized` antes de ejecutar un solo nodo.

### Mitigaciones de Seguridad y Rendimiento
* **Aislamiento AST en `action_code`:** Bloqueo en tiempo de compilación AST contra `import`, acceso a introspección MRO (`__class__`, `__subclasses__`), builtins peligrosos (`eval`, `exec`, `open`) y bucles infinitos no acotados (`while`).
* **Protección contra Inyección de Plantillas:** Bloqueo de atributos dunder (`__*`) en la evaluación de expresiones `{{...}}`.
* **Enmascaramiento de Secretos (`mask_sensitive_data`):** Redacción automática de passwords, tokens y API keys (`******`) en los logs persistentes de auditoría y trazas de auditoría.
* **Protección DoS por Payloads Masivos:** Rechazo inmediato con `413 Payload Too Large` ante webhooks entrantes que superen el límite configurable (10 MB por defecto).
* **Ejecución Paralela de Ramas en DAG:** Los nodos dentro de la misma capa topológica se ejecutan concurrentemente con `asyncio.gather`.
* **Connection Pooling HTTP:** Pool persistente con keep-alive en `action_http` (`httpx.Limits`) reduciendo latencias de handshake TCP/TLS.
* **Caché LRU en Expresiones:** Cacheo en memoria de árboles sintácticos AST de expresiones recurrentes.
* **Índices SQLite Optimizados:** B-Trees dedicados para ordenación y filtrado temporal de ejecuciones.

---

## 🧪 Pruebas Automatizadas y Calidad

Para ejecutar la suite de 92 pruebas automatizadas y validar el 100% de los componentes:

```powershell
# Ejecutar tests con pytest
uv run pytest -v

# Verificar linting y formato con ruff
uv run ruff check .
uv run ruff format --check .
```
