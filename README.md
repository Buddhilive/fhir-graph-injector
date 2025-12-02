# FHIR Graph Injector

A Python application to inject FHIR (Fast Healthcare Interoperability Resources) data into a Neo4j graph database. This tool processes FHIR Bundles containing Patient, Encounter, Condition, and Observation resources and creates a knowledge graph representation.

## Features

- **FHIR Resource Processing**: Supports Patient, Encounter, Condition, and Observation resources
- **Graph Database**: Creates a knowledge graph in Neo4j with proper relationships
- **Batch Processing**: Can process multiple FHIR Bundle files from a directory
- **Comprehensive Data Extraction**: Extracts detailed information including demographics, clinical data, and temporal information
- **Logging**: Comprehensive logging for monitoring the injection process
- **Database Management**: Built-in database clearing and constraint creation

## Prerequisites

- Python 3.11 or higher
- Neo4j Database running on localhost:7687 (default)
- UV package manager (for dependency management)

## Installation

1. Clone or download this repository
2. Install dependencies using UV:
   ```bash
   uv add neo4j
   ```

## Neo4j Setup

1. Install Neo4j Desktop or Neo4j Community Server
2. Create a new database or use the default `neo4j` database
3. Start the database service
4. Update the Neo4j password in `config.py`

## Install Neo4j on Docker

You can also run Neo4j using Docker. Use the following command to pull and run the Neo4j image:

```bash
docker run -d --name neo4j --restart always --publish=7474:7474 --publish=7687:7687 --env NEO4J_AUTH=neo4j/Qwaszx12 neo4j:latest
```

## Configuration

Edit `config.py` to match your Neo4j setup:

```python
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "your_password_here"  # Change this!
```

## Usage

### V2 Implementation (Recommended - Following Reference Tutorial)

**Processing All FHIR Files:**
```bash
python main_v2.py
```

**Testing with a Single File:**
```bash
python test_single_file_v2.py
```

**Running Sample Queries:**
```bash
python sample_queries.py
```

### V1 Implementation (Original)

**Processing All FHIR Files:**
```bash
python main.py
```

**Testing with a Single File:**
```bash
python test_single_file.py
```

### Using as a Library

```python
from fhir_neo4j_injector import FHIRNeo4jInjector

# Initialize injector
with FHIRNeo4jInjector("bolt://localhost:7687", "neo4j", "password") as injector:
    # Create database constraints
    injector.create_constraints()

    # Process a single file
    injector.inject_from_file("path/to/fhir_bundle.json")

    # Or process all files in a directory
    injector.inject_from_directory("data")

    # Get summary
    summary = injector.get_database_summary()
    print(summary)
```

## Graph Schema

### V2 Implementation (Following Reference Tutorial)

**Node Types:**
- **Patient**: Demographics, address, race, ethnicity
- **Practitioner**: Healthcare providers
- **Organization**: Healthcare facilities
- **Encounter**: Medical visits and interactions
- **Condition**: Medical conditions and diagnoses
- **Observation**: Clinical observations and measurements
- **MedicationRequest**: Prescribed medications
- **Procedure**: Medical procedures performed

**Relationships:**
- `Patient -[HASENCOUNTER]-> Encounter`
- `Patient -[HASCONDITION]-> Condition`
- `Patient -[HASMEDICATION]-> MedicationRequest`
- `Patient -[HASPROCEDURE]-> Procedure`
- `Encounter -[HASOBSERVATION]-> Observation`
- `Encounter -[HASCONDITION]-> Condition`
- `MedicationRequest -[TREATMENTFOR]-> Condition`

**Temporal Relationships:**
- `Patient -[FIRSTCONDITION]-> Condition` (first diagnosed condition)
- `Patient -[LATESTCONDITION]-> Condition` (most recent condition)
- `Condition -[NEXTCONDITION]-> Condition` (temporal sequence)

### V1 Implementation (Original)

**Node Types:** Patient, Encounter, Condition, Observation

**Relationships:**
- `Patient -[HAS_ENCOUNTER]-> Encounter`
- `Patient -[HAS_CONDITION]-> Condition`
- `Patient -[HAS_OBSERVATION]-> Observation`
- `Encounter -[HAS_CONDITION]-> Condition`
- `Encounter -[HAS_OBSERVATION]-> Observation`

### Sample Queries

Once data is loaded, you can query the graph using Cypher queries:

```cypher
// Find all patients
MATCH (p:Patient) RETURN p

// Find patients with specific conditions
MATCH (p:Patient)-[:HAS_CONDITION]->(c:Condition)
WHERE c.display CONTAINS "Hypertension"
RETURN p.name, c.display

// Find all observations for a patient
MATCH (p:Patient {name: "John Doe"})-[:HAS_OBSERVATION]->(o:Observation)
RETURN o.display, o.value, o.unit

// Find encounters and their related conditions
MATCH (e:Encounter)-[:HAS_CONDITION]->(c:Condition)
RETURN e.type, c.display, e.start
```

## Data Structure

The application processes FHIR Bundles and extracts:

### Patient Information
- Name, gender, birth date
- Address and contact information
- Race, ethnicity, birth place
- Medical record numbers and identifiers

### Encounter Information
- Status, class, type
- Start and end dates
- Links to patient

### Condition Information
- Clinical and verification status
- Condition codes and descriptions
- Onset and recording dates
- Links to patient and encounter

### Observation Information
- Status and codes
- Values (quantity, string, or coded values)
- Effective dates
- Links to patient and encounter

## Logging

The application provides detailed logging of the injection process:
- Resource counts by type
- Individual resource processing
- Error handling for malformed data
- Database operation summaries

## Error Handling

- Graceful handling of missing or malformed FHIR data
- Continues processing other resources if one fails
- Detailed error logging for troubleshooting
- Database connection error handling

## Performance

- Uses Neo4j constraints for better performance
- Batch processing of multiple files
- Efficient relationship creation
- Memory-efficient JSON processing

## Troubleshooting

### Common Issues

1. **Neo4j Connection Error**: Verify Neo4j is running and credentials are correct
2. **File Not Found**: Check that FHIR JSON files exist in the data directory
3. **Memory Issues**: For large files, consider processing files individually

### Debugging

Enable verbose logging by setting `LOG_LEVEL = "DEBUG"` in `config.py`.

## Contributing

When adding support for additional FHIR resource types:

1. Add a new injection method (e.g., `inject_procedure`)
2. Update the main injection loop in `inject_fhir_bundle`
3. Add appropriate relationships to existing resources
4. Update the database summary method

## License

This project is provided as-is for educational and development purposes.