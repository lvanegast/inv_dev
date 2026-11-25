"""Catálogo preconfigurado de plantillas y blueprints arquitectónicos para FlowEngine."""

from flowengine.core.models import FlowTemplate

CRM_LEAD_SCORING_YAML = """id: crm-lead-scoring
name: Ingesta de Leads y Scoring Automático
description: >
  Normaliza prospectos con DataMapper, evalúa score comercial y notifica
  al equipo de ventas de {{vars.COMPANY_NAME}}.
trigger:
  id: webhook_lead
  type: trigger_webhook
  config:
    path: /webhook/leads
nodes:
  - id: map_lead
    name: Normalizar y Calificar Lead
    type: action_mapper
    config:
      fields:
        lead_id: "{{lead.id}}"
        full_name: "{{lead.name}}"
        email: "{{lead.email}}"
        company: "{{lead.company}}"
        score: "{{lead.score}}"
      defaults:
        score: {{vars.MIN_SCORE}}
        company: "Sin Empresa"
  - id: notify_sales
    name: Notificar Equipo Comercial
    type: action_notify
    config:
      channel: email
      recipient: "{{vars.SALES_EMAIL}}"
      subject: "Nuevo Lead Calificado para {{vars.COMPANY_NAME}}: {{lead.name}}"
      message: "Se ha registrado un prospecto con score {{map_lead.output.score}}."
      mock: true
edges:
  - source: webhook_lead
    target: map_lead
  - source: map_lead
    target: notify_sales
    condition: "{{map_lead.output.score}} >= {{vars.MIN_SCORE}}"
"""

INVOICE_OCR_ACCOUNTING_YAML = """id: invoice-ocr-accounting
name: Procesamiento Inteligente de Facturas con OCR
description: >
  Ingiere comprobantes PDF, extrae campos clave mediante OCR y almacena
  registros contables en la base de datos SQLite.
trigger:
  id: trigger_invoice_file
  type: trigger_webhook
  config:
    path: /webhook/invoices
nodes:
  - id: parse_invoice_ocr
    name: Extracción OCR de Factura
    type: action_ocr
    config:
      content: "{{file_content}}"
      mock_ocr: true
  - id: insert_accounting_record
    name: Persistir Registro Contable en SQLite
    type: action_sql
    config:
      query: >
        INSERT INTO {{vars.TABLE_NAME}} (invoice_number, total, subtotal, tax)
        VALUES (:invoice_no, :total, :subtotal, :tax);
      params:
        invoice_no: "{{parse_invoice_ocr.output.invoice_number}}"
        total: "{{parse_invoice_ocr.output.total_amount}}"
        subtotal: "{{parse_invoice_ocr.output.subtotal}}"
        tax: "{{parse_invoice_ocr.output.tax_amount}}"
  - id: broadcast_invoice_event
    name: Difusión de Evento Fan-Out
    type: action_webhook_broadcast
    config:
      topic: "{{vars.AUDIT_EVENT_TOPIC}}"
      payload:
        invoice_number: "{{parse_invoice_ocr.output.invoice_number}}"
        total_amount: "{{parse_invoice_ocr.output.total_amount}}"
        tax_rate: {{vars.DEFAULT_TAX_RATE}}
edges:
  - source: trigger_invoice_file
    target: parse_invoice_ocr
  - source: parse_invoice_ocr
    target: insert_accounting_record
  - source: insert_accounting_record
    target: broadcast_invoice_event
"""

EXCEL_BATCH_COMMISSIONS_YAML = """id: excel-batch-commissions
name: Procesador Batch de Ventas y Comisiones Excel
description: >
  Procesa planillas de ventas, liquida comisiones del {{vars.COMMISSION_PERCENT}}%
  y genera reporte consolidado en Excel.
trigger:
  id: trigger_batch_upload
  type: trigger_webhook
  config:
    path: /webhook/sales-batch
nodes:
  - id: read_sales_sheet
    name: Ingesta de Planilla Excel
    type: action_tabular
    config:
      operation: read
      content: "{{file_content}}"
  - id: process_sales_rows
    name: Iterador de Filas de Venta
    type: action_iterator
    config:
      items: "{{read_sales_sheet.output.rows}}"
  - id: calculate_commission
    name: Cálculo de Comisión
    type: action_code
    config:
      code: "commission = row.get('amount', 0) * ({{vars.COMMISSION_PERCENT}} / 100.0)"
  - id: export_final_sheet
    name: Exportación a Excel Consolidado
    type: action_tabular
    config:
      operation: write
      format: xlsx
      records: "{{process_sales_rows.output}}"
edges:
  - source: trigger_batch_upload
    target: read_sales_sheet
  - source: read_sales_sheet
    target: process_sales_rows
  - source: process_sales_rows
    target: calculate_commission
  - source: calculate_commission
    target: export_final_sheet
"""

WEBHOOK_CDC_FANOUT_YAML = """id: webhook-cdc-fanout
name: Bus de Eventos CDC y Fan-Out Criptográfico
description: >
  Punto central de retransmisión de eventos en abanico con firmas
  HMAC-SHA256 hacia microservicios suscriptores.
trigger:
  id: webhook_ingress
  type: trigger_webhook
  config:
    path: /webhook/events
nodes:
  - id: map_envelope
    name: Estandarizar Envoltorio de Evento
    type: action_mapper
    config:
      fields:
        event_id: "{{event.id}}"
        event_topic: "{{vars.EVENT_PREFIX}}.{{event.action}}"
        timestamp: "{{event.timestamp}}"
        data: "{{event.payload}}"
  - id: fanout_broadcast
    name: Retransmisión Fan-Out Criptográfica
    type: action_webhook_broadcast
    config:
      topic: "{{map_envelope.output.event_topic}}"
      payload: "{{map_envelope.output}}"
      timeout_seconds: {{vars.FANOUT_TIMEOUT}}
edges:
  - source: webhook_ingress
    target: map_envelope
  - source: map_envelope
    target: fanout_broadcast
"""

POLLING_ORDER_WATCHDOG_YAML = """id: polling-order-watchdog
name: Sondeo Incremental de Pedidos con Watermark
description: >
  Monitorea API de órdenes cada {{vars.POLL_INTERVAL}} segundos con cursor
  persistente y alerta sobre nuevos pedidos.
trigger:
  id: poll_orders_api
  type: trigger_polling
  config:
    source_type: http
    endpoint_url: "{{vars.ORDERS_ENDPOINT}}"
    cursor_field: id
    watermark_type: numeric
    interval_seconds: {{vars.POLL_INTERVAL}}
nodes:
  - id: alert_new_order
    name: Notificar Nuevo Pedido
    type: action_notify
    config:
      channel: webhook
      message: "Nuevo pedido detectado con ID {{poll_orders_api.output.id}}"
      mock: true
edges:
  - source: poll_orders_api
    target: alert_new_order
"""

SCHEDULED_DLQ_MONITOR_YAML = """id: scheduled-dlq-monitor
name: Auditoría Programada y Monitor de Salud DLQ
description: >
  Tarea Cron recurrente ({{vars.CRON_SCHEDULE}}) que audita la Dead Letter Queue
  y notifica ante anomalías.
trigger:
  id: cron_heartbeat
  type: trigger_schedule
  config:
    cron: "{{vars.CRON_SCHEDULE}}"
nodes:
  - id: check_dlq_health
    name: Diagnóstico de Fallas en DB
    type: action_sql
    config:
      query: "SELECT COUNT(*) as failed_runs FROM runs WHERE status = 'failed';"
      read_only: true
  - id: send_health_alert
    name: Despachar Alerta a Slack / Teams
    type: action_notify
    config:
      channel: webhook
      webhook_url: "{{vars.ALERT_WEBHOOK_URL}}"
      message: "Monitor DLQ: Se detectaron {{check_dlq_health.output.failed_runs}} ejecuciones fallidas."
      mock: true
edges:
  - source: cron_heartbeat
    target: check_dlq_health
  - source: check_dlq_health
    target: send_health_alert
    condition: "{{check_dlq_health.output.failed_runs}} > 0"
"""


def get_builtin_templates() -> list[FlowTemplate]:
    """Retorna la lista de planos preconfigurados oficiales para el catálogo."""
    return [
        FlowTemplate(
            id="crm-lead-scoring",
            name="Ingesta de Leads y Scoring Automático",
            description=(
                "Recepciona prospectos vía Webhook, estandariza datos con DataMapper, "
                "evalúa score comercial y notifica de inmediato al equipo de ventas."
            ),
            category="sales",
            tags=["sales", "crm", "webhook", "mapper", "notify"],
            icon="🎯",
            difficulty="beginner",
            definition_yaml=CRM_LEAD_SCORING_YAML.strip(),
            variables_schema={
                "COMPANY_NAME": {
                    "type": "string",
                    "label": "Nombre de la Empresa",
                    "description": "Nombre de la organización emisora",
                    "default": "Acme Corp",
                },
                "MIN_SCORE": {
                    "type": "integer",
                    "label": "Puntaje Mínimo de Lead",
                    "description": "Score mínimo para calificar un prospecto",
                    "default": 70,
                },
                "SALES_EMAIL": {
                    "type": "string",
                    "label": "Email de Ventas",
                    "description": "Correo del equipo comercial para avisos",
                    "default": "ventas@acme.com",
                },
            },
        ),
        FlowTemplate(
            id="invoice-ocr-accounting",
            name="Procesamiento Inteligente de Facturas con OCR",
            description=(
                "Extrae metadatos fiscales, número, subtotal, IVA y total de facturas PDF "
                "mediante OCR, almacena en SQLite y difunde evento Fan-Out."
            ),
            category="finance",
            tags=["finance", "ocr", "pdf", "sql", "accounting", "broadcast"],
            icon="🧾",
            difficulty="intermediate",
            definition_yaml=INVOICE_OCR_ACCOUNTING_YAML.strip(),
            variables_schema={
                "DEFAULT_TAX_RATE": {
                    "type": "number",
                    "label": "Tasa de Impuesto / IVA (%)",
                    "description": "Porcentaje impositivo de auditoría",
                    "default": 19.0,
                },
                "TABLE_NAME": {
                    "type": "string",
                    "label": "Tabla SQLite Contable",
                    "description": "Nombre de la tabla donde persistir la factura",
                    "default": "processed_invoices",
                },
                "AUDIT_EVENT_TOPIC": {
                    "type": "string",
                    "label": "Tópico de Evento CDC",
                    "description": "Canal de evento Fan-Out para notificar",
                    "default": "finance.invoice.processed",
                },
            },
        ),
        FlowTemplate(
            id="excel-batch-commissions",
            name="Procesador Batch de Ventas y Comisiones Excel",
            description=(
                "Lee archivo de ventas Excel (.xlsx) o CSV con Tabular, itera registros "
                "calculando comisiones y exporta planilla de liquidación consolidada."
            ),
            category="data",
            tags=["data", "excel", "tabular", "batch", "commissions"],
            icon="📊",
            difficulty="intermediate",
            definition_yaml=EXCEL_BATCH_COMMISSIONS_YAML.strip(),
            variables_schema={
                "COMMISSION_PERCENT": {
                    "type": "number",
                    "label": "Porcentaje de Comisión (%)",
                    "description": "Porcentaje a liquidar sobre cada venta",
                    "default": 5.0,
                },
                "MIN_SALE_THRESHOLD": {
                    "type": "number",
                    "label": "Monto Mínimo de Venta",
                    "description": "Umbral mínimo de venta computable",
                    "default": 100.0,
                },
            },
        ),
        FlowTemplate(
            id="webhook-cdc-fanout",
            name="Bus de Eventos CDC y Fan-Out Criptográfico",
            description=(
                "Punto central de retransmisión masiva de eventos en abanico con firmas "
                "HMAC-SHA256 hacia múltiples microservicios suscriptores."
            ),
            category="integration",
            tags=["integration", "cdc", "fanout", "security", "hmac"],
            icon="📡",
            difficulty="advanced",
            definition_yaml=WEBHOOK_CDC_FANOUT_YAML.strip(),
            variables_schema={
                "EVENT_PREFIX": {
                    "type": "string",
                    "label": "Prefijo de Evento Global",
                    "description": "Prefijo para los tópicos del bus de eventos",
                    "default": "orders",
                },
                "FANOUT_TIMEOUT": {
                    "type": "integer",
                    "label": "Timeout de Despacho (segundos)",
                    "description": "Tiempo límite de espera por suscriptor",
                    "default": 10,
                },
            },
        ),
        FlowTemplate(
            id="polling-order-watchdog",
            name="Sondeo Incremental de Pedidos con Watermark",
            description=(
                "Monitorea API externa o base de datos detectando pedidos nuevos mediante "
                "cursor persistente (Watermark) sin generar registros duplicados."
            ),
            category="monitoring",
            tags=["monitoring", "polling", "watermark", "orders"],
            icon="⏱️",
            difficulty="intermediate",
            definition_yaml=POLLING_ORDER_WATCHDOG_YAML.strip(),
            variables_schema={
                "POLL_INTERVAL": {
                    "type": "integer",
                    "label": "Intervalo de Sondeo (segundos)",
                    "description": "Frecuencia de revisión en segundos",
                    "default": 60,
                },
                "ORDERS_ENDPOINT": {
                    "type": "string",
                    "label": "URL Endpoint de Pedidos",
                    "description": "Endpoint REST para consultar nuevas órdenes",
                    "default": "https://api.acme.com/orders",
                },
            },
        ),
        FlowTemplate(
            id="scheduled-dlq-monitor",
            name="Auditoría Programada y Monitor de Salud DLQ",
            description=(
                "Tarea Cron recurrente que audita la Dead Letter Queue (DLQ) y dispara "
                "alertas de contingencia si detecta fallas críticas acumuladas."
            ),
            category="devops",
            tags=["devops", "cron", "dlq", "alerting", "health"],
            icon="🛡️",
            difficulty="beginner",
            definition_yaml=SCHEDULED_DLQ_MONITOR_YAML.strip(),
            variables_schema={
                "CRON_SCHEDULE": {
                    "type": "string",
                    "label": "Expresión Cron de Programación",
                    "description": "Frecuencia Cron de ejecución (5 campos)",
                    "default": "0 * * * *",
                },
                "ALERT_WEBHOOK_URL": {
                    "type": "string",
                    "label": "URL de Webhook para Alertas",
                    "description": "URL donde despachar la notificación de salud operativa",
                    "default": "https://hooks.slack.com/services/test",
                },
            },
        ),
    ]
