# FHIR Patient Data API - Quick Start Guide

## Installation

1. Install dependencies:

```bash
pip install -r requirements.txt
```

Or using uv:

```bash
uv pip install -r requirements.txt
```

## Running the API

Start the FastAPI server:

```bash
uvicorn api.main:app --reload
```

The API will be available at: `http://localhost:8000`

## API Documentation

Interactive API documentation is available at:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## API Endpoints

### 1. GET /patients

Fetch all patients with basic information

**Query Parameters:**

- `limit` (optional): Maximum number of patients to return (default: 100, max: 1000)
- `skip` (optional): Number of patients to skip for pagination (default: 0)

**Example:**

```bash
curl http://localhost:8000/patients?limit=10
```

### 2. GET /patients/{mrn}

Get complete patient details by Medical Record Number (MRN)

**Path Parameters:**

- `mrn`: Medical Record Number (patient ID)

**Example:**

```bash
curl http://localhost:8000/patients/4877dc14-609c-a22b-0a94-e15707167428
```

### 3. GET /patients/{mrn}/encounters

Get patient visit/encounter history by MRN

**Path Parameters:**

- `mrn`: Medical Record Number (patient ID)

**Example:**

```bash
curl http://localhost:8000/patients/4877dc14-609c-a22b-0a94-e15707167428/encounters
```

## Health Check

Check API and database health:

```bash
curl http://localhost:8000/health
```

## Configuration

The API uses the Neo4j connection settings from `config.py`:

- URI: `bolt://localhost:7687`
- Username: `neo4j`
- Password: (configured in config.py)

Make sure your Neo4j database is running and populated with FHIR data before starting the API.
