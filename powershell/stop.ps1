Write-Host "===================================" -ForegroundColor Red
Write-Host "DST Airlines - Shutdown" -ForegroundColor Red
Write-Host "===================================" -ForegroundColor Red
Write-Host ""

Write-Host "Stopping all services..." -ForegroundColor Yellow
docker-compose down

Write-Host ""
Write-Host "Services stopped successfully!" -ForegroundColor Green
Write-Host ""

$clean = Read-Host "Do you want to clean volumes? (y/N)"
if ($clean -eq "y" -or $clean -eq "Y") {
    Write-Host "Cleaning volumes..." -ForegroundColor Yellow
    docker-compose down -v
    Write-Host "Volumes cleaned!" -ForegroundColor Green
}

Write-Host ""
Write-Host "Shutdown complete." -ForegroundColor Cyan
