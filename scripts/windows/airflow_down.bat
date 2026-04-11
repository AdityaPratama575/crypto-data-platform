@echo off
setlocal

set "ROOT_DIR=%~dp0..\.."
pushd "%ROOT_DIR%"

echo [INFO] Stopping Airflow stack...
docker compose down
if errorlevel 1 goto :fail

echo [DONE] Airflow stack stopped.
popd
exit /b 0

:fail
echo [ERROR] Failed to stop Airflow stack.
popd
exit /b 1

