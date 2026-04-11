@echo off
setlocal

echo [INFO] Trigger ops_alerting_dag ...
docker exec crypto_airflow_scheduler airflow dags trigger ops_alerting_dag

if errorlevel 1 (
  echo [ERROR] Gagal trigger ops_alerting_dag.
  exit /b 1
)

echo [DONE] ops_alerting_dag berhasil di-trigger.
exit /b 0
