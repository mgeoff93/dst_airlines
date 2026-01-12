# Requêtes :
$response = Invoke-RestMethod -Uri "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token" `
    -Method Post `
    -Body @{
        grant_type    = "client_credentials"
        client_id     = "datascientest_airlines-api-client"
        client_secret = "MRekHJjkss0MYXYx70nSFEENfu4lDiYE"
    }

$token = $response.access_token

$apiResponse = Invoke-WebRequest -Uri "https://opensky-network.org/api/states/all" -Headers @{Authorization = "Bearer $token"}

# Affiche les crédits restants et le quota total
$apiResponse.Headers | Select-Object "X-Rate-Limit-Limit", "X-Rate-Limit-Remaining", "X-Rate-Limit-Reset"




# Supprime les fichiers générés
Remove-Item .env -ErrorAction SilentlyContinue
Remove-Item .\airflow\config\variables.json -ErrorAction SilentlyContinue

# Nettoie Terraform
Remove-Item -Recurse -Force .\terraform\.terraform\ -ErrorAction SilentlyContinue
Remove-Item .\terraform\.terraform.lock.hcl -ErrorAction SilentlyContinue
Remove-Item .\terraform\terraform.tfstate* -ErrorAction SilentlyContinue

# Nettoyage Docker
docker system prune -f