"""
Sample Neo4j Cypher queries for the FHIR Graph Database (V2 Implementation)

This script demonstrates various queries you can run against the FHIR knowledge graph
following the reference tutorial pattern.
"""

import logging
from neo4j import GraphDatabase
import config

class FHIRQueryExamples:
    """Sample queries for the FHIR Neo4j graph database."""

    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def run_query(self, query: str, description: str):
        """Run a query and display results."""
        print(f"\n{'='*60}")
        print(f"Query: {description}")
        print(f"{'='*60}")
        print(f"Cypher: {query}")
        print("-" * 60)

        with self.driver.session() as session:
            try:
                result = session.run(query)
                records = list(result)

                if records:
                    for i, record in enumerate(records[:10]):  # Limit to first 10 results
                        print(f"  {i+1}. {dict(record)}")
                    if len(records) > 10:
                        print(f"  ... and {len(records) - 10} more records")
                    print(f"\nTotal records: {len(records)}")
                else:
                    print("  No records found.")

            except Exception as e:
                print(f"  Error: {e}")

    def basic_queries(self):
        """Run basic data exploration queries."""
        print("\n" + "="*80)
        print("BASIC DATA EXPLORATION QUERIES")
        print("="*80)

        # Count all node types
        self.run_query(
            """
            MATCH (n)
            RETURN labels(n)[0] as NodeType, count(n) as Count
            ORDER BY Count DESC
            """,
            "Count nodes by type"
        )

        # Count all relationship types
        self.run_query(
            """
            MATCH ()-[r]->()
            RETURN type(r) as RelationshipType, count(r) as Count
            ORDER BY Count DESC
            """,
            "Count relationships by type"
        )

        # Show all patients
        self.run_query(
            """
            MATCH (p:Patient)
            RETURN p.fname + ' ' + p.lname as FullName, p.sex, p.birthDate, p.city, p.state
            ORDER BY FullName
            LIMIT 10
            """,
            "List first 10 patients"
        )

    def patient_queries(self):
        """Patient-focused queries."""
        print("\n" + "="*80)
        print("PATIENT-FOCUSED QUERIES")
        print("="*80)

        # Patients by gender
        self.run_query(
            """
            MATCH (p:Patient)
            RETURN p.sex as Gender, count(p) as Count
            ORDER BY Count DESC
            """,
            "Patient count by gender"
        )

        # Patients by state
        self.run_query(
            """
            MATCH (p:Patient)
            WHERE p.state IS NOT NULL AND p.state <> ""
            RETURN p.state as State, count(p) as Count
            ORDER BY Count DESC
            LIMIT 10
            """,
            "Top 10 states by patient count"
        )

        # Patients with most encounters
        self.run_query(
            """
            MATCH (p:Patient)-[:HASENCOUNTER]->(e:Encounter)
            RETURN p.fname + ' ' + p.lname as PatientName, count(e) as EncounterCount
            ORDER BY EncounterCount DESC
            LIMIT 10
            """,
            "Patients with most encounters"
        )

    def condition_queries(self):
        """Condition and diagnosis queries."""
        print("\n" + "="*80)
        print("CONDITION AND DIAGNOSIS QUERIES")
        print("="*80)

        # Most common conditions
        self.run_query(
            """
            MATCH (c:Condition)
            WHERE c.type IS NOT NULL AND c.type <> ""
            RETURN c.type as Condition, count(c) as Count
            ORDER BY Count DESC
            LIMIT 15
            """,
            "Most common conditions"
        )

        # Patients with specific conditions (example: diabetes)
        self.run_query(
            """
            MATCH (p:Patient)-[:HASCONDITION]->(c:Condition)
            WHERE toLower(c.type) CONTAINS 'diabetes'
            RETURN p.fname + ' ' + p.lname as PatientName, c.type as Condition, c.onsetdate as OnsetDate
            ORDER BY c.onsetdate
            """,
            "Patients with diabetes"
        )

        # Condition progression (using temporal relationships)
        self.run_query(
            """
            MATCH (p:Patient)-[:FIRSTCONDITION]->(first:Condition)
            MATCH (p)-[:LATESTCONDITION]->(latest:Condition)
            WHERE first.id <> latest.id
            RETURN p.fname + ' ' + p.lname as PatientName,
                   first.type as FirstCondition, first.onsetdate as FirstOnset,
                   latest.type as LatestCondition, latest.onsetdate as LatestOnset
            LIMIT 10
            """,
            "Condition progression over time"
        )

        # Condition chains
        self.run_query(
            """
            MATCH path = (c1:Condition)-[:NEXTCONDITION*1..3]->(c2:Condition)
            MATCH (p:Patient)-[:HASCONDITION]->(c1)
            RETURN p.fname + ' ' + p.lname as PatientName,
                   [c in nodes(path) | c.type] as ConditionSequence
            LIMIT 5
            """,
            "Condition progression chains"
        )

    def clinical_queries(self):
        """Clinical observations and measurements."""
        print("\n" + "="*80)
        print("CLINICAL OBSERVATIONS QUERIES")
        print("="*80)

        # Most common observation types
        self.run_query(
            """
            MATCH (obs:Observation)
            WHERE obs.type IS NOT NULL AND obs.type <> ""
            RETURN obs.type as ObservationType, count(obs) as Count
            ORDER BY Count DESC
            LIMIT 15
            """,
            "Most common observation types"
        )

        # Vital signs for patients
        self.run_query(
            """
            MATCH (p:Patient)-[:HASOBSERVATION]->(obs:Observation)
            WHERE toLower(obs.type) CONTAINS 'blood pressure' OR
                  toLower(obs.type) CONTAINS 'heart rate' OR
                  toLower(obs.type) CONTAINS 'temperature'
            RETURN p.fname + ' ' + p.lname as PatientName,
                   obs.type as VitalSign, obs.value + ' ' + obs.unit as Value,
                   obs.effectivedate as Date
            ORDER BY obs.effectivedate DESC
            LIMIT 20
            """,
            "Recent vital signs"
        )

    def medication_queries(self):
        """Medication and treatment queries."""
        print("\n" + "="*80)
        print("MEDICATION AND TREATMENT QUERIES")
        print("="*80)

        # Most prescribed medications
        self.run_query(
            """
            MATCH (mr:MedicationRequest)
            WHERE mr.medication IS NOT NULL AND mr.medication <> ""
            RETURN mr.medication as Medication, count(mr) as PrescriptionCount
            ORDER BY PrescriptionCount DESC
            LIMIT 15
            """,
            "Most prescribed medications"
        )

        # Medications treating specific conditions
        self.run_query(
            """
            MATCH (mr:MedicationRequest)-[:TREATMENTFOR]->(c:Condition)
            RETURN c.type as Condition, mr.medication as Medication, count(*) as TreatmentCount
            ORDER BY TreatmentCount DESC
            LIMIT 15
            """,
            "Medications by condition treated"
        )

        # Patient medication profiles
        self.run_query(
            """
            MATCH (p:Patient)-[:HASMEDICATION]->(mr:MedicationRequest)
            WITH p, collect(mr.medication) as Medications, count(mr) as MedCount
            WHERE MedCount >= 2
            RETURN p.fname + ' ' + p.lname as PatientName, MedCount, Medications[0..5] as SampleMedications
            ORDER BY MedCount DESC
            LIMIT 10
            """,
            "Patients with multiple medications"
        )

    def encounter_queries(self):
        """Healthcare encounter queries."""
        print("\n" + "="*80)
        print("HEALTHCARE ENCOUNTER QUERIES")
        print("="*80)

        # Most common encounter types
        self.run_query(
            """
            MATCH (e:Encounter)
            WHERE e.type IS NOT NULL AND e.type <> ""
            RETURN e.type as EncounterType, count(e) as Count
            ORDER BY Count DESC
            """,
            "Most common encounter types"
        )

        # Patient encounter patterns
        self.run_query(
            """
            MATCH (p:Patient)-[:HASENCOUNTER]->(e:Encounter)
            WHERE e.encstart IS NOT NULL AND e.encstart <> ""
            WITH p, e ORDER BY e.encstart
            WITH p, collect(e.type)[0..3] as EncounterPattern, count(e) as TotalEncounters
            WHERE TotalEncounters >= 2
            RETURN p.fname + ' ' + p.lname as PatientName, TotalEncounters, EncounterPattern
            ORDER BY TotalEncounters DESC
            LIMIT 10
            """,
            "Patient encounter patterns"
        )

    def complex_queries(self):
        """Complex analytical queries."""
        print("\n" + "="*80)
        print("COMPLEX ANALYTICAL QUERIES")
        print("="*80)

        # Patient care complexity score
        self.run_query(
            """
            MATCH (p:Patient)
            OPTIONAL MATCH (p)-[:HASCONDITION]->(c:Condition)
            OPTIONAL MATCH (p)-[:HASMEDICATION]->(mr:MedicationRequest)
            OPTIONAL MATCH (p)-[:HASENCOUNTER]->(e:Encounter)
            WITH p, count(DISTINCT c) as ConditionCount,
                    count(DISTINCT mr) as MedicationCount,
                    count(DISTINCT e) as EncounterCount
            WITH p, (ConditionCount * 2 + MedicationCount + EncounterCount * 0.5) as ComplexityScore
            WHERE ComplexityScore > 0
            RETURN p.fname + ' ' + p.lname as PatientName,
                   ConditionCount, MedicationCount, EncounterCount,
                   round(ComplexityScore, 2) as ComplexityScore
            ORDER BY ComplexityScore DESC
            LIMIT 15
            """,
            "Patient care complexity analysis"
        )

        # Comorbidity patterns
        self.run_query(
            """
            MATCH (p:Patient)-[:HASCONDITION]->(c1:Condition)
            MATCH (p)-[:HASCONDITION]->(c2:Condition)
            WHERE c1.id < c2.id AND c1.type IS NOT NULL AND c2.type IS NOT NULL
            RETURN c1.type as Condition1, c2.type as Condition2, count(p) as CooccurrenceCount
            ORDER BY CooccurrenceCount DESC
            LIMIT 15
            """,
            "Common comorbidity patterns"
        )

    def run_all_samples(self):
        """Run all sample queries."""
        print("FHIR NEO4J KNOWLEDGE GRAPH - SAMPLE QUERIES")
        print("Following the reference tutorial pattern")
        print("=" * 80)

        try:
            self.basic_queries()
            self.patient_queries()
            self.condition_queries()
            self.clinical_queries()
            self.medication_queries()
            self.encounter_queries()
            self.complex_queries()

            print("\n" + "="*80)
            print("QUERY EXECUTION COMPLETED")
            print("="*80)
            print("You can copy any of these queries and run them in the Neo4j Browser")
            print("or use them as templates for your own analyses.")

        except Exception as e:
            print(f"Error running queries: {e}")

def main():
    """Main function to run sample queries."""
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    try:
        with FHIRQueryExamples(config.NEO4J_URI, config.NEO4J_USER, config.NEO4J_PASSWORD) as query_runner:
            query_runner.run_all_samples()

    except Exception as e:
        logging.error(f"Application error: {e}")
        print(f"Error: {e}")
        print("Make sure Neo4j is running and you have data in the database.")

if __name__ == "__main__":
    main()