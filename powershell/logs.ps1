Write-Host "===================================" -ForegroundColor Cyan
Write-Host "DST Airlines - Logs Viewer" -ForegroundColor Cyan
Write-Host "===================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Available services:" -ForegroundColor Yellow
Write-Host "1. All services"
Write-Host "2. Airflow Scheduler"
Write-Host "3. Airflow Worker"
Write-Host "4. FastAPI"
Write-Host "5. PostgreSQL"
Write-Host ""

$choice = Read-Host "Select service (1-5)"

switch ($choice) {
    "1" { docker-compose logs -f }
    "2" { docker-compose logs -f airflow-scheduler }
    "3" { docker-compose logs -f airflow-worker }
    "4" { docker-compose logs -f fastapi }
    "5" { docker-compose logs -f postgres }
    default { 
        Write-Host "Invalid choice. Showing all logs..." -ForegroundColor Red
        docker-compose logs -f 
    }
}
