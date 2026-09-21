# Prueba Técnica — Ingeniero de Datos (Hospital Alma Máter)
 
Solución end-to-end de ingesta, procesamiento y visualización de datos para una IPS, a partir de 4 fuentes en Excel: `pacientes`, `citas`, `eventos_clinicos` y `facturacion`.
 
## Contenido del repositorio
 ```
notebooks/
  01_bronze_ingesta      -> Lectura cruda de los 4 Excel + trazabilidad
  02_silver_limpieza     -> Tipificación, validaciones de calidad, integridad referencial
  03_gold_agregaciones   -> KPIs de negocio listos para consumo
README.md                -> Este documento
```
Entorno: Databricks Free Edition (Serverless) + PySpark/SQL + Git + Dashboard nativo de Databricks (Lakeview).

---

## 1. Entendimiento de los datos
 
### Entidades y relaciones
```
pacientes (1) ──< (N) citas
citas (1) ──< (N) eventos_clinicos
citas (1) ──< (N) facturacion
```
### Granularidad confirmada
 
| Tabla | 1 fila = |
|---|---|
| `pacientes` | 1 paciente (223 registros únicos) |
| `citas` | 1 cita (1001 registros únicos) |
| `eventos_clinicos` | 1 evento dentro de una cita (1602 filas — una cita puede tener varios eventos) |
| `facturacion` | 1 factura (1202 filas — una cita puede generar 1 o 2 facturas) |

### Problemas de calidad detectados
 
- 1 registro corrupto en `pacientes.fecha_nacimiento` (paciente `P00200`): el valor venía como una hora suelta (`00:00:00`) en vez de una fecha completa, probablemente por un error de captura/formato en el Excel origen. Se resuelve en Silver.
- Nulos en `citas.motivo_cancelacion` (933 de 1001): **no son un error**, el campo solo aplica cuando `estado_cita = 'Cancelada'`. Validado: 0 casos de cita cancelada sin motivo.
- Nulos en `eventos_clinicos.valor_resultado`/`unidad_resultado`: lógicos, no todos los tipos de evento (ej. "Diagnóstico") tienen una medición numérica asociada.
- Integridad referencial: sin huérfanos, toda cita tiene un paciente válido, todo evento y factura apuntan a una cita existente (validado en Silver, resultado: 0 en los 3 casos).
- Sin duplicados exactos ni claves repetidas en ninguna de las 4 tablas.

---
 
## 2. Arquitectura de la solución
 
```
Excel (pacientes, citas, eventos_clinicos, facturacion)
        │
        ▼
  [Ingesta]  pandas (puente técnico) → Spark DataFrame
        │
        ▼
  BRONZE  (workspace.bronze)   — datos crudos, sin transformar + metadatos de trazabilidad
        │
        ▼
  SILVER  (workspace.silver)   — tipificación, limpieza, validaciones de calidad e integridad
        │
        ▼
  GOLD    (workspace.gold)     — KPIs agregados, listos para negocio
        │
        ▼
  Dashboard (Databricks Lakeview) — consumo visual
```

### Por qué Lakehouse (Bronze/Silver/Gold) y no un Data Warehouse tradicional
 
Los datos de origen son heterogéneos (texto, fechas con formatos inconsistentes, relaciones 1-a-N) y con calidad desconocida de antemano. Un Lakehouse permite guardar el dato crudo primero (sin perder información ni bloquear la ingesta por errores de formato) y decidir las reglas de limpieza en un paso separado y auditable en vez de forzar una transformación agresiva en el momento de cargar, como exigiría un Data Warehouse clásico (schema-on-write).

### Por qué ELT y no ETL
 
Se optó por **ELT**: se carga el dato tal cual llega (Bronze = "Load" crudo) y las transformaciones ocurren después, dentro de la plataforma (Silver = "Transform"). Esto permite reprocesar la lógica de limpieza sin volver a depender de los archivos fuente originales, y deja registro de exactamente qué llegó vs. qué se decidió hacer con eso.

---
 
## 3. Decisiones técnicas y alternativas consideradas
 
| Decisión | Alternativa considerada | Por qué se descartó |
|---|---|---|
| Lectura de Excel vía **pandas** dentro de la función de ingesta | Librería nativa `spark-excel` (crealytics) | Es una librería Maven/JVM, **Databricks Serverless no soporta instalación de librerías Maven**, solo paquetes Python vía `%pip`. Confirmado al intentar instalarla. |
| Todas las columnas `object` convertidas con `.astype("string")` antes de crear el DataFrame de Spark | Dejar que Spark infiera el tipo directamente desde pandas | Con datos mixtos (ej. el registro corrupto de fecha), Spark no logra unificar el tipo y falla. Convertir a string de forma explícita evita el conflicto sin perder el valor original. |
| Conversión de `fecha_nacimiento` con `F.try_to_date(...)` | `to_date()` normal | Se utilizó `to_date()` para evitar que valores no parseables detuvieran el pipeline. La función devuelve `NULL` cuando no puede convertir un valor, permitiendo continuar el procesamiento y documentar el registro afectado. |
| `mode("overwrite")` en la escritura de todas las capas | `append` / `merge` incremental | Es una carga única de datos históricos para esta prueba. En un escenario productivo con cargas recurrentes, se usaría `merge` (upsert) para no perder histórico ni duplicar datos. |
| Dashboard nativo de Databricks (Lakeview) | Power BI Desktop | El enunciado no exige la herramienta específica. Dado el tiempo disponible, se priorizó una solución sin instalación adicional, dentro de la misma plataforma. La conexión a Power BI vía SQL Warehouse (hostname + HTTP Path) es igualmente viable y quedó explorada como camino de evolución. |

---
 
## 4. Diseño técnico por capa
 
### Bronze (`01_bronze_ingesta`)
- Lectura de los 4 Excel con pandas - conversión a Spark DataFrame.
- Sin transformación de negocio: el dato corrupto de `fecha_nacimiento` se preserva tal cual.
- Metadatos de trazabilidad agregados a cada fila: `fuente_archivo`, `fecha_ingesta`, `lote_ingesta` (este último generado dinámicamente por fecha de ejecución).
- Manejo de errores: `try/except` alrededor de la lectura, si un archivo falla, se detiene la ejecución con un mensaje explícito en vez de continuar con datos parciales.

### Silver (`02_silver_limpieza`)
- Normalización de nombres de columnas.
- Diagnóstico reutilizable (nulos, duplicados exactos, claves repetidas) aplicado a las 4 tablas por igual.
- Conversión de `fecha_nacimiento` a fecha real (`try_to_date`), 1 registro resulta en `null`, documentado.
- Validaciones de reglas de negocio: citas canceladas sin motivo, consistencia `bruto - descuento = neto` en facturación.
- Integridad referencial entre las 4 tablas (citas=pacientes, eventos=citas, facturación=citas).

### Gold (`03_gold_agregaciones`)
Tres tablas construidas a partir de Silver:
 
| Tabla | KPI(s) que resuelve |
|---|---|
| `gold_citas_por_especialidad` | Volumen de citas y tasa de inasistencia, por especialidad |
| `gold_facturacion_mensual` | Total facturado y ticket promedio, por mes |
| `gold_eventos_por_tipo` | Distribución de la carga clínica por tipo de evento |

---
 
## 5. KPIs y justificación de negocio
 
1. **Volumen de citas por especialidad**: identifica dónde está la demanda real, útil para decisiones de personal y horarios. Medicina General y Oftalmología concentran la mayor demanda.
2. **Tasa de inasistencia por especialidad**: cada cita perdida es tiempo médico desperdiciado. Pediatría presenta la tasa más alta (~19%), casi el doble que Optometría.
3. **Facturación total y promedio por mes**: indicador financiero base para seguimiento de tendencia. Se observa una caída en el último mes registrado, que corresponde a un mes incompleto en los datos fuente, no a una caída real de facturación.
4. **Eventos clínicos por tipo**: muestra en qué se concentra la carga operativa real (más allá del número de citas), relevante para planear personal e insumos. Signos vitales (45%) y Diagnóstico (27%) concentran la mayoría de los eventos.
**Dashboard publicado:** _https://dbc-750e5da6-a43c.cloud.databricks.com/dashboardsv3/01f1b59ef2821d4ca4c61858d5aa7a4c/published?o=7474650182476180_

---
 
## 6. Seguridad y gobierno de datos
 
- Los datos incluyen información clínica y de facturación potencialmente sensible (PII). En un entorno productivo, el acceso a las capas Silver/Gold se restringiría vía Unity Catalog con principio de mínimo privilegio (permisos por schema/tabla, no acceso total a la cuenta de almacenamiento).
- Trazabilidad: cada fila en Bronze conserva su archivo y momento de ingesta, permitiendo auditar el origen de cualquier dato en capas posteriores.
---

## 7. Escenarios y cómo se manejarían
 
**Aumento de volumen**:
Bronze ya está desacoplado de Silver/Gold, escalar significaría mover el volumen de archivos crudos a Azure Data Lake Storage Gen2 (en vez del volumen interno de Databricks) y usar cómputo Job Compute dimensionado según carga, en vez de Serverless de entorno gratuito.

**Datos tardíos**:
Al usar `lote_ingesta` con fecha de ejecución, una carga tardía queda identificada como un lote distinto, el diseño soportaría cambiar de `overwrite` a `merge` por clave natural (`id_paciente`, `id_cita`, etc.) para incorporar registros tardíos sin duplicar ni perder los ya procesados.

**Fallo del pipeline**:
Cada función de lectura tiene manejo de errores (`try/except`) que detiene la ejecución con un mensaje claro. En producción, esto se combinaría con reintentos automáticos y alertas (Databricks Workflows), en vez de depender de revisión manual.

**Protección de datos personales (PII)**:
Columnas como `nombres`, `apellidos`, `numero_documento` se mantendrían con acceso restringido en Silver/Gold vía Unity Catalog; los KPIs de Gold están diseñados para no exponer identificadores individuales, solo agregaciones.

---
 
## 8. Evolución a producción (Azure/AWS - Databricks)
 
El mismo pipeline evolucionaría reemplazando el volumen interno de Databricks por **Azure Data Lake Storage Gen2**, con:
- Ingesta orquestada por **Azure Data Factory** (en vez de ejecución manual del notebook).
- Autenticación de Databricks al Data Lake vía **Service Principal** (Microsoft Entra ID) con permisos acotados por contenedor.
- **Unity Catalog** para gobierno de datos y control de acceso a nivel de schema/tabla.
- Capa de consumo vía **SQL Warehouse** conectado a Power BI como herramienta de BI corporativa.

---
 
## 9. Limitaciones conocidas
 
- `mode("overwrite")` no es incremental, válido para esta prueba (carga única), no para producción con cargas recurrentes.
- El dashboard se construyó en Databricks Lakeview en vez de Power BI, la conexión a Power BI vía SQL Warehouse quedó validada como alternativa.
- No se implementó CI/CD (reto opcional).
