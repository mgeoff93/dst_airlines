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