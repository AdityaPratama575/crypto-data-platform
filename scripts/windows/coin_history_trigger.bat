@echo off
setlocal

set "DAG_ID=coin_history_daily_dag"

echo [INFO] Unpausing %DAG_ID%...
docker exec crypto_airflow_scheduler airflow dags unpause %DAG_ID%
if errorlevel 1 goto :fail

echo [INFO] Triggering %DAG_ID%...
docker exec crypto_airflow_scheduler airflow dags trigger %DAG_ID%
if errorlevel 1 goto :fail

echo [DONE] DAG %DAG_ID% triggered.
exit /b 0

:fail
echo [ERROR] Failed to trigger %DAG_ID%. Ensure scheduler container is running.
exit /b 1
