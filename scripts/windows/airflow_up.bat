@echo off
setlocal

set "ROOT_DIR=%~dp0..\.."
pushd "%ROOT_DIR%"

echo [INFO] Starting Airflow services...
docker compose up -d airflow-postgres airflow-webserver airflow-scheduler
if errorlevel 1 goto :fail

docker compose ps
if errorlevel 1 goto :fail

echo [DONE] Airflow services are up.
echo [INFO] Open http://localhost:8080 (admin/admin)
popd
exit /b 0

:fail
echo [ERROR] Failed to start Airflow services.
popd
exit /b 1

