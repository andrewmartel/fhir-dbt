# FHIR dbt Take‑Home (Analytics Engineer)

This project loads a small synthetic FHIR bulk dataset into Postgres and uses dbt
to model it and answer the analytics questions from the assignment.

## Quick run (no database)

To **run the full assignment and get all 5 answers** without Postgres or dbt:

```bash
pip install -r requirements.txt   # only python-dotenv and psycopg2; psycopg2 optional for standalone
python run_assignment_standalone.py
```

Results are printed to the console. To save them:  
`python run_assignment_standalone.py > assignment_results.txt`

Pre-generated results are in `assignment_results.txt`.

## 1. Environment setup (for dbt + Postgres)

**Credentials:** No passwords are stored in the repo. After cloning, create a `.env` file with your Postgres password (see below). The loader and `run_everything.py` use `.env`; dbt gets the password via the `DBT_PASSWORD` env var, which `run_everything.py` sets from `PGPASSWORD`. `.env` is in `.gitignore` and must not be committed.

- **Python deps**
  - Create and activate a virtualenv (optional but recommended)
  - Install requirements:

    ```bash
    pip install -r requirements.txt
    ```

- **Postgres**
  - Use any Postgres instance (local Docker, cloud, etc.)
  - Create a database, e.g. `fhir`
  - Copy `.env.example` to `.env` in the project root and set your password:

    ```bash
    cp .env.example .env
    # Edit .env and set PGPASSWORD=your_real_postgres_password
    ```

## 2. Load FHIR NDJSON into Postgres

The dataset in `sample-bulk-fhir-datasets-10-patients` is a small SMART on FHIR
bulk export (10 patients). Each file is NDJSON with one resource per line.

I chose to:

- Keep **raw tables very close to FHIR** with a `jsonb` payload
- Extract just enough typed columns in dbt for the assignment questions

To create and populate the raw tables:

```bash
python load_fhir_to_postgres.py
```

This script:

- Creates `raw_patient`, `raw_immunization`, `raw_observation`,
  `raw_diagnostic_report`
- Loads the corresponding NDJSON files into those tables

## 3. dbt configuration

Create a dbt profile named `fhir_dbt` (e.g. in `~/.dbt/profiles.yml`) pointing
to the same Postgres database:

```yaml
fhir_dbt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: postgres
      password: your_password
      dbname: fhir
      schema: public
```

Then from this project directory:

```bash
dbt deps   # not strictly needed here, but safe
dbt run
dbt test
```

Key schemas / models:

- Staging models in `stg_fhir`:
  - `stg_patient`, `stg_immunization`, `stg_observation`,
    `stg_diagnostic_report`
- Mart models in `fhir_marts`:
  - `mart_immunization_stats`
  - `mart_vital_signs_per_patient`
  - `mart_recent_cholesterol_report_per_patient`
  - `mart_patient_profile`

## 4. Answering the assignment questions

Once `dbt run` has completed, you can answer the questions with SQL on the mart
models.

- **Q1: How many patients had the influenza vaccine?**

  ```sql
  select patients_with_influenza_vaccine
  from fhir_marts.mart_immunization_stats;
  ```

- **Q2: What are the top 5 most common vaccines?**

  ```sql
  select
      (jsonb_array_elements(top_5_vaccines)->>'vaccine_name') as vaccine_name,
      (jsonb_array_elements(top_5_vaccines)->>'administrations')::int as administrations
  from fhir_marts.mart_immunization_stats;
  ```

- **Q3: Average number of vital signs per patient**

  ```sql
  select avg_vital_signs_per_patient
  from fhir_marts.mart_vital_signs_per_patient;
  ```

- **Q4: Most recent report with a recorded cholesterol lab result for each patient**

  ```sql
  select *
  from fhir_marts.mart_recent_cholesterol_report_per_patient;
  ```

  This model:

  - Treats cholesterol‑related labs as Observations whose LOINC code is in
    `{2093‑3 (total cholesterol), 2089‑1 (LDL), 2571‑8 (triglycerides)}`
  - Joins DiagnosticReports to those observations on patient and
    `effectiveDateTime` date
  - Picks the most recent report per patient

- **Q5: Two interesting facts about a specific patient**

  You can explore the per‑patient mart:

  ```sql
  select *
  from fhir_marts.mart_patient_profile
  order by patient_id;
  ```

  and then drill into raw JSON as needed, e.g.:

  ```sql
  -- example: look at a specific patient in more detail
  select resource
  from raw_patient
  where patient_id = '<some-patient-id>';
  ```

## 5. Assumptions and trade‑offs

- **Dataset choice**: I used the published SMART on FHIR 10‑patient bulk export
  as the assignment dataset. It is small enough to inspect manually but still
  representative of realistic FHIR structure.
- **Raw vs. modeled**:
  - Raw tables keep the **entire FHIR resource as JSONB** so that future
    questions or models can project additional fields without re‑ingesting.
  - dbt models focus only on columns needed for:
    - Patient identity and demographics
    - Immunization CVX codes and displays
    - Observation categories and codes (for vital signs and cholesterol)
    - DiagnosticReport timing
- **Cholesterol logic**: the “cholesterol lab result” definition is approximated
  as Observations with common lipid‑panel LOINC codes (total cholesterol, LDL,
  triglycerides). In a production setting I would confirm exact code lists with
  clinical stakeholders.
- **Vital signs**: “vital signs” are identified by Observation category code
  `vital-signs`, which aligns with the FHIR specification.

If I were to model *all* FHIR resources at scale, I would:

- Introduce a proper **FHIR ingestion pipeline** (e.g. Kafka + a landing store)
  with validation and schema evolution handling
- Use a **canonical data warehouse model**:
  - Resource‑specific tables with important attributes pulled out of JSON
  - Shared dimensions (patients, providers, facilities, code systems)
  - Fact tables for encounters, measurements, and events
- Add:
  - **Data quality checks** (uniqueness, referential integrity, clinical ranges)
  - **Incremental dbt models** to handle ongoing loads
  - **Metadata / lineage tracking** so downstream teams can trust and discover
    models

## 6. AI usage

I used an AI coding assistant (Cursor / GPT‑based) to:

- Scaffold the loader script and dbt models
- Look up example LOINC codes and FHIR field conventions

All SQL, Python, and modeling decisions have been reviewed and understood by me,
and I’m prepared to walk through them in a live session.
