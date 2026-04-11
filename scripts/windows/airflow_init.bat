@echo off
setlocal

set "ROOT_DIR=%~dp0..\.."
pushd "%ROOT_DIR%"

echo [INFO] Starting airflow-postgres...
docker compose up airflow-postgres -d
if errorlevel 1 goto :fail

echo [INFO] Running airflow-init...
docker compose run --rm airflow-init
if errorlevel 1 goto :fail

echo [DONE] airflow-init completed.
popd
exit /b 0

:fail
echo [ERROR] airflow-init failed.
popd
exit /b 1

