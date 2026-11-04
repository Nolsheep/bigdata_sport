-- Runs once, the first time the Postgres container starts with an empty volume.
CREATE SCHEMA IF NOT EXISTS raw_data;     -- untouched copies of the Kaggle CSVs
CREATE SCHEMA IF NOT EXISTS production;   -- cleaned, modelled tables that feed the dashboard
CREATE SCHEMA IF NOT EXISTS audit;        -- pipeline run log and reconciliation results
