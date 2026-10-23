# FlowEngine ⚡
> **Réplica de alto rendimiento del motor de orquestación, DAG y Webhooks estilo Zapier / Make.com**
> Desarrollado en Python moderno (3.11+) gestionado con [`uv`](https://github.com/astral-sh/uv).

[![CI Tests](https://img.shields.io/badge/tests-47%20passed-brightgreen.svg)](tests/)
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

### 5. Iniciar el Servidor de Webhooks de Alta Concurrencia
```powershell
# Registrar el flujo en la base de datos
uv run flow register examples/demo_zap.yaml

# Levantar el servidor
uv run flow serve --port 8000
```
Ahora puedes disparar webhooks directamente desde cualquier herramienta o script:
```bash
curl -X POST http://127.0.0.1:8000/webhook/order-processing-zap \
  -H "Content-Type: application/json" \
  -d '{"status": "confirmed", "total": 95.0, "customer": {"first_name": "Ana", "email": "ana@example.com"}}'
```

---

## 🛠️ Catálogo de Nodos Disponibles

| Tipo de Nodo | Categoría | Descripción |
| :--- | :--- | :--- |
| `trigger_webhook` | Trigger | Despierta el flujo al recibir una llamada HTTP POST/GET externa. |
| `trigger_manual` | Trigger | Disparo manual por CLI o scripts. |
| `trigger_schedule` | Trigger | Temporizador basado en intervalos o cron. |
| `action_http` | Acción | Llamadas HTTP asíncronas (GET, POST, PUT, DELETE) con autenticación Bearer/Basic. |
| `action_filter` | Acción | Filtro condicional (*Only continue if...*). |
| `action_transform` | Acción | Mapeo y formateo de estructuras de datos JSON. |
| `action_code` | Acción | Ejecución de funciones de transformación personalizadas en Python. |
| `action_log` | Acción | Registro de eventos y alertas formateadas en consola. |
| `action_iterator` | Lotes | Desempaqueta un array para ejecutar pasos posteriores por cada elemento (`{{item}}`). |
| `action_aggregator` | Lotes | Agrupa y consolida los resultados del bucle (sumatorias, listas, conteos). |

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

## 🧪 Pruebas Automatizadas y Calidad

Para ejecutar la suite de 47 pruebas automatizadas y validar el 100% de los componentes:

```powershell
# Ejecutar tests con pytest
uv run pytest -v

# Verificar linting y formato con ruff
uv run ruff check .
uv run ruff format --check .
```
