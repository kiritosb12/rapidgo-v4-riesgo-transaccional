#!/usr/bin/env bash
# Crea las etiquetas, los hitos (milestones) y los 20 Issues del proyecto RapidGo V4 con GitHub CLI.
# Uso:
#   gh auth login
#   export REPO="usuario/rapidgo-v4-riesgo-transaccional"
#   export USER_A="usuario-github-esteban" USER_B="usuario-github-oscar" USER_C="usuario-github-karol"
#   bash scripts/crear_issues.sh
# Luego, en el tablero Kanban: botón "Add item" → agregar todos los Issues del repositorio (o activar la
# automatización "Auto-add to project" del tablero).
set -euo pipefail
: "${REPO:?Defina REPO=usuario/repositorio}"
# Si un integrante aún no acepta la invitación como colaborador, deje su variable sin definir:
# su Issue se crea sin asignar y con la línea "Responsable: <nombre>" para asignarlo después.
: "${USER_A:=@Esteban}"; : "${USER_B:=@Oscar}"; : "${USER_C:=@Karol}"

label() { gh label create "$1" --color "$2" --repo "$REPO" --force >/dev/null; }
label "semana-1" "0E8A16"; label "semana-2" "1D76DB"; label "semana-3" "5319E7"; label "semana-4" "D93F0B"
label "kafka" "F5A623"; label "spark" "1F4E79"; label "datalake" "16A085"; label "airflow" "006837"
label "integracion-continua" "FBCA04"; label "documentacion" "C5DEF5"; label "infraestructura" "BFD4F2"; label "version" "E74C5E"

for m in "Semana 1 - v0.1.0-foundation" "Semana 2 - v0.2.0-streaming" "Semana 3 - v0.3.0-orchestration" "Semana 4 - v1.0.0"; do
  gh api "repos/$REPO/milestones" -f title="$m" >/dev/null 2>&1 || true
done

issue() { # titulo | etiquetas | responsable | hito | descripcion
  if [[ "$3" == @* ]]; then
    gh issue create --repo "$REPO" --title "$1" --label "$2" --milestone "$4" --body "$5

**Responsable:** ${3#@} (pendiente de asignar)"
  else
    gh issue create --repo "$REPO" --title "$1" --label "$2" --assignee "$3" --milestone "$4" --body "$5"
  fi
}
S1="Semana 1 - v0.1.0-foundation"; S2="Semana 2 - v0.2.0-streaming"; S3="Semana 3 - v0.3.0-orchestration"; S4="Semana 4 - v1.0.0"

# ---------------- SEMANA 1 ----------------
issue "Estructura inicial del repositorio" "semana-1,infraestructura" "$USER_B" "$S1" "Crear las carpetas recomendadas (producer, spark, airflow/dags, tests, samples, datalake, docs), .gitignore y requirements.txt.
**Criterio de aceptación:** estructura igual a la guía, integrada mediante un Pull Request revisado."
issue "Contrato JSON del evento intento_pago y ejemplos" "semana-1,kafka,documentacion" "$USER_C" "$S1" "Definir los campos (valor, medio de pago, dispositivo, ubicación, intentos), el esquema JSON y 2 eventos de ejemplo (normal y anómalo). Incluir la prueba automática del contrato.
**Criterio de aceptación:** los ejemplos de samples/ son válidos frente al esquema."
issue "Definir indicadores y reglas de alerta con umbrales iniciales" "semana-1,documentacion" "$USER_A" "$S1" "Mínimo 3 indicadores (intentos por pedido, valor promedio, eventos de riesgo) y 2 alertas (múltiples intentos, señales combinadas) con la justificación de los umbrales.
**Criterio de aceptación:** docs/kpis_y_alertas.md revisado por otro integrante."
issue "Topic, clave y particiones de Kafka + ADR-001" "semana-1,kafka,documentacion" "$USER_A" "$S1" "Definir el topic rapidgo.v4.pagos.intentos, la clave id_pedido y 3 particiones, y justificarlo en docs/decisions/ADR-001.
**Criterio de aceptación:** ADR con opciones, decisión y consecuencias."
issue "Diagrama de arquitectura y README inicial" "semana-1,documentacion" "$USER_B" "$S1" "docs/arquitectura.png específico de la variante y README con problema, equipo, variante, arquitectura, contrato, indicadores y alertas."
issue "docker-compose preliminar (Kafka KRaft, topic y Kafka UI)" "semana-1,infraestructura,kafka" "$USER_C" "$S1" "Kafka en modo KRaft, servicio kafka-init que crea el topic con 3 particiones y Kafka UI en el puerto 8080.
**Criterio de aceptación:** docker compose config --quiet sin errores y topic creado."
issue "Punto de control de la Semana 1 y versión v0.1.0-foundation" "semana-1,version,documentacion" "$USER_B" "$S1" "docs/checkpoints/semana1.md (máximo 1 página: decisión clave, riesgo principal y plan de la Semana 2) y publicar el release."

# ---------------- SEMANA 2 ----------------
issue "Productor de intentos de pago con clave id_pedido" "semana-2,kafka" "$USER_A" "$S2" "Generador en Python con cerca de 5 % de anomalías (valores altos, IP extranjera, reintentos), confirmación total (acks=all), sin duplicados (idempotente), clave id_pedido y equipo=equipo9-v4."
issue "Spark Streaming: de Kafka a las capas raw y processed en Parquet" "semana-2,spark,datalake" "$USER_B" "$S2" "Lectura continua desde Kafka, interpretación con esquema, al menos 3 transformaciones (tipos y eliminación de duplicados, hora local, señales y puntaje de riesgo) y escritura en Parquet con checkpoint."
issue "Indicadores por ventana y alertas A1 y A2 en Spark" "semana-2,spark" "$USER_A" "$S2" "Ventana fija de 1 minuto para K1 a K4 con marca de agua; ventana de 5 minutos por id_pedido para MULTIPLES_INTENTOS; SENALES_COMBINADAS por evento. Salida en la capa curated."
issue "Pruebas automáticas de reglas y flujo ci.yml" "semana-2,integracion-continua" "$USER_C" "$S2" "Pruebas del contrato y de las funciones de reglas (puntaje de riesgo, umbrales). ci.yml que se ejecute en push, pull_request y workflow_dispatch: sintaxis, pruebas y validación de docker compose."
issue "ADR-002, punto de control de la Semana 2 y versión v0.2.0-streaming" "semana-2,documentacion,version" "$USER_C" "$S2" "ADR-002 basado en valores reales observados (por ejemplo, ajuste de un umbral o del intervalo de procesamiento). Punto de control con un valor real, una decisión técnica y un problema con el Pull Request que lo resolvió."

# ---------------- SEMANA 3 ----------------
issue "DAG de Airflow con al menos 5 tareas, reintentos y XCom" "semana-3,airflow" "$USER_A" "$S3" "verificar_fuentes → validar_raw → agregar_kpis → validar_curated → generar_reporte. 2 reintentos y XCom con los conteos de registros y alertas."
issue "Ampliar la integración continua: importar el DAG y nuevas pruebas" "semana-3,integracion-continua" "$USER_B" "$S3" "Paso en ci.yml que importa el DAG sin errores y ejecuta pruebas de calidad y de reglas."
issue "Integrar Airflow al docker-compose" "semana-3,infraestructura,airflow" "$USER_C" "$S3" "Servicios de PostgreSQL y Airflow (servidor web y planificador) con volúmenes hacia airflow/dags y datalake."
issue "Fallo controlado, recuperación, ADR-003 y punto de control de la Semana 3" "semana-3,airflow,documentacion" "$USER_C" "$S3" "Provocar un fallo (Kafka detenido o capa raw vacía), documentar el mensaje real del registro, las tareas afectadas y la recuperación. ADR-003 sobre la estrategia de recuperación. Versión v0.3.0-orchestration."

# ---------------- SEMANA 4 ----------------
issue "Integración y estabilización de extremo a extremo" "semana-4,infraestructura" "$USER_A" "$S4" "Flujo completo levantando con un solo docker compose up siguiendo el README."
issue "Control de calidad final y versión v1.0.0" "semana-4,integracion-continua,version" "$USER_B" "$S4" "Integración continua en verde antes de crear la etiqueta, release.yml (opcional), notas de la versión y etiqueta v1.0.0. Cada integrante ejecuta el flujo manualmente (workflow_dispatch)."
issue "README final, tabla de contribuciones y cierre del Kanban" "semana-4,documentacion" "$USER_C" "$S4" "README con instalación, ejecución, resultados, limitaciones, contribuciones y enlace al video. Cerrar o justificar los Issues pendientes."
issue "Video final (máximo 10 minutos) con los tres integrantes" "semana-4,documentacion" "$USER_A" "$S4" "Guion según la guía: problema, arquitectura, demostración, GitHub, resultados y aprendizajes. Enlazarlo desde el README y la versión."

echo "Listo: 20 Issues creados en $REPO"
