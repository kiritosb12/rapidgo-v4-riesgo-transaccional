#!/usr/bin/env bash
# Prueba de punta a punta de RapidGo V4: levanta todo, espera datos en el Data Lake, ejecuta el DAG y
# comprueba que termina en verde. Uso (Git Bash o Linux, desde la raíz del repositorio):
#   bash scripts/prueba_e2e.sh            # usa lo que ya esté corriendo o lo levanta
#   bash scripts/prueba_e2e.sh --limpio   # borra datos, checkpoints y la base de Airflow antes de empezar
set -euo pipefail
DAG=rapidgo_riesgo_transaccional
inicio=$(date +%s)
paso() { echo; echo "[$(( $(date +%s) - inicio )) s] $*"; }
if [ "${1:-}" = "--limpio" ]; then
  paso "Borrando contenedores, base de Airflow y datos del lago"
  docker compose down -v
  for d in raw processed curated; do find "datalake/$d" -mindepth 1 ! -name .gitkeep -exec rm -rf {} + 2>/dev/null || true; done
  rm -rf datalake/_checkpoints datalake/_metricas datalake/_cuarentena
fi
paso "Levantando servicios (la primera vez construye las imágenes)"
docker compose up -d --build
docker compose ps
paso "Esperando datos en processed (máx. 5 min)"
for i in $(seq 1 60); do
  n=$( (find datalake/processed/pagos -name '*.parquet' 2>/dev/null || true) | wc -l)
  [ "$n" -gt 0 ] && break
  sleep 5
done
[ "$n" -gt 0 ] || { echo "ERROR: Spark no escribió processed. Revise: docker compose logs spark"; exit 1; }
echo "processed tiene $n archivos"
paso "Esperando 3 min para que cierren las primeras ventanas (marca de agua de 2 min)"
sleep 180
paso "Activando y ejecutando el DAG"
docker compose exec -T airflow-scheduler airflow dags unpause "$DAG" > /dev/null
RUN=$(docker compose exec -T airflow-scheduler airflow dags trigger "$DAG" | grep -o 'manual__[^ |]*' | head -1)
echo "Ejecución: $RUN"
estado=""
for i in $(seq 1 60); do
  sleep 5
  estado=$(docker compose exec -T airflow-scheduler airflow dags list-runs "$DAG" -o plain 2>/dev/null | grep "$RUN" | awk '{print $3}' || true)
  [ "$estado" = "success" ] || [ "$estado" = "failed" ] && break
done
docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run "$DAG" "$RUN" -o plain 2>/dev/null | awk 'NR>1 {print "  ", $2, $3}'
if [ "$estado" != "success" ]; then
  echo "ERROR: el DAG terminó en '${estado:-sin terminar}'. Revise la tarea en rojo en http://localhost:8081"
  exit 1
fi
reporte=$(ls -t datalake/curated/reportes/*/reporte_*.md | head -1)
paso "OK: flujo completo en verde. Reporte: $reporte"
sed -n '1,25p' "$reporte"
