####################################################################################################################
# Setup containers to run Trang
####################################################################################################################

NETWORK_NAME=data-network

SHELL := powershell.exe
.SHELLFLAGS := -NoProfile -Command

init:
	@echo "Run init project"
	@if (Test-Path "./scripts/init_project.ps1") { ./scripts/init_project.ps1 -NetworkName "$(NETWORK_NAME)"; Write-Host "Run init project complete"; } else { Write-Host "Error: ./scripts/init_project.ps1 not found!" -ForegroundColor Red; exit 1; }

up:
	@echo "Spin up Trang containers"
	docker compose up -d --build
	@echo "Spin up Trang complete"

ui:
	@echo "Open Trang Web UI"
	start http://localhost:8080

down:
	@echo "Shutdown Trang containers"
	docker compose down
	@echo "Shutdown Trang complete"

deploy: init up

restart: down up
