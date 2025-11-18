param (
    [string]$NetworkName = "data-network"
)

Write-Host "
                   Chang
" -ForegroundColor Cyan

# Name of the Airflow logs
$Dir = ".\logs"

# Check if the network exists
$networkExists = docker network ls --filter "name=^$NetworkName$" --format "{{.Name}}"
if (-not $networkExists) {
    Write-Host "Creating Docker network: $NetworkName"
    docker network create -d bridge $NetworkName | Out-Null
    Write-Host "Network $NetworkName created successfully."
} else {
    Write-Host "The $NetworkName network already exists."
}

# Check if the directory exists
if (Test-Path $Dir) {
    Write-Host "$Dir exists. Checking permissions..."
    try {
        # Grant full permissions to everyone
        icacls $Dir /grant "Everyone:(OI)(CI)F" /T | Out-Null
        Write-Host "Permissions for $Dir have been set to full (777 equivalent)."
    } catch {
        Write-Host "Failed to set permissions for $Dir" -ForegroundColor Red
    }
} else {
    Write-Host "$Dir does not exist. Creating it..."
    New-Item -ItemType Directory -Path $Dir | Out-Null
    icacls $Dir /grant "Everyone:(OI)(CI)F" /T | Out-Null
    Write-Host "$Dir created and permissions set to full (777 equivalent)."
}
