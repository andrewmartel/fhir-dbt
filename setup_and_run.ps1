# Full setup and run: create conda env, install deps, load DB, dbt, run assignment queries.
# Run from project root: .\setup_and_run.ps1
# If Postgres uses a different password, edit .env before running.

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

Write-Host "Creating conda env fhir-env (python 3.11)..."
conda create -n fhir-env python=3.11 -y

Write-Host "Installing Python packages in fhir-env..."
& conda run -n fhir-env pip install -r requirements.txt
& conda run -n fhir-env pip install "dbt-postgres"

Write-Host "Running full pipeline (load DB, dbt run/test, assignment queries)..."
& conda run -n fhir-env python run_everything.py

Write-Host "Done."
