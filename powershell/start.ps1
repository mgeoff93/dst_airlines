# Définit le répertoire du script comme répertoire courant
Set-Location -Path $PSScriptRoot

Write-Host "===================================" -ForegroundColor Cyan
Write-Host "DST Airlines - Quick Start" -ForegroundColor Cyan
Write-Host "===================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "1. Building Docker images..." -ForegroundColor Yellow
docker-compose build

Write-Host ""
Write-Host "2. Starting services..." -ForegroundColor Yellow
docker-compose up -d

Write-Host ""
Write-Host "3. Waiting for services to be ready..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

Write-Host ""
Write-Host "===================================" -ForegroundColor Cyan
Write-Host "Services Status:" -ForegroundColor Cyan
Write-Host "===================================" -ForegroundColor Cyan
docker-compose ps

Write-Host ""
Write-Host "===================================" -ForegroundColor Green
Write-Host "Access Points:" -ForegroundColor Green
Write-Host "===================================" -ForegroundColor Green
Write-Host "- Airflow UI:  http://localhost:8080 (airflow/airflow)" -ForegroundColor White
Write-Host "- MLFlowUI:    http://localhost:5000" -ForegroundColor White
Write-Host "- FastAPI:     http://localhost:8000/docs" -ForegroundColor White
Write-Host "- PostgreSQL:  localhost:5432 (airflow/airflow)" -ForegroundColor White
Write-Host ""

Write-Host "===================================" -ForegroundColor Cyan
Write-Host "Quick Commands:" -ForegroundColor Cyan
Write-Host "===================================" -ForegroundColor Cyan
Write-Host "View logs:     docker-compose logs -f" -ForegroundColor White
Write-Host "Stop all:      docker-compose down" -ForegroundColor White
Write-Host "Restart:       docker-compose restart" -ForegroundColor White
Write-Host ""

Write-Host "Press any key to open Airflow UI in browser..." -ForegroundColor Yellow
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
Start-Process "http://localhost:8080"