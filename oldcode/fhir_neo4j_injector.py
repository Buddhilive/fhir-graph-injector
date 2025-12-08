"""
FHIR to Neo4j Graph Injector

This module provides functionality to inject FHIR resources into a Neo4j graph database.
It processes FHIR Bundles containing Patient, Encounter, Condition, and Observation resources.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from neo4j import GraphDatabase, Session
import uuid


class FHIRNeo4jInjector:
    """
    Injects FHIR resources into Neo4j database as a knowledge graph.
    """

    def __init__(self, uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = "password"):
        """
        Initialize the injector with Neo4j connection parameters.

        Args:
            uri: Neo4j database URI
            user: Database username
            password: Database password
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.logger = logging.getLogger(__name__)

    def close(self):
        """Close the database connection."""
        self.driver.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def clear_database(self):
        """Clear all nodes and relationships from the database."""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            self.logger.info("Database cleared")

    def create_constraints(self):
        """Create uniqueness constraints for better performance."""
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Patient) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (e:Encounter) REQUIRE e.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Condition) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (o:Observation) REQUIRE o.id IS UNIQUE"
        ]

        with self.driver.session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                    self.logger.info(f"Created constraint: {constraint}")
                except Exception as e:
                    self.logger.warning(f"Constraint already exists or failed: {e}")

    def extract_name(self, name_list: List[Dict]) -> str:
        """Extract human-readable name from FHIR name structure."""
        if not name_list:
            return "Unknown"

        name = name_list[0]  # Use first official name
        given_names = " ".join(name.get("given", []))
        family_name = name.get("family", "")

        return f"{given_names} {family_name}".strip()

    def extract_address(self, address_list: List[Dict]) -> Dict[str, str]:
        """Extract address information from FHIR address structure."""
        if not address_list:
            return {}

        addr = address_list[0]  # Use first address
        return {
            "line": ", ".join(addr.get("line", [])),
            "city": addr.get("city", ""),
            "state": addr.get("state", ""),
            "postalCode": addr.get("postalCode", ""),
            "country": addr.get("country", "")
        }

    def extract_telecom(self, telecom_list: List[Dict]) -> Dict[str, str]:
        """Extract telecom information from FHIR telecom structure."""
        result = {}
        for telecom in telecom_list:
            system = telecom.get("system", "")
            value = telecom.get("value", "")
            if system and value:
                result[system] = value
        return result

    def extract_coding_display(self, coding_list: List[Dict]) -> str:
        """Extract display text from FHIR coding structure."""
        if not coding_list:
            return ""
        return coding_list[0].get("display", "")

    def inject_patient(self, session: Session, patient: Dict[str, Any]):
        """Inject a Patient resource into Neo4j."""
        patient_id = patient.get("id")
        if not patient_id:
            self.logger.warning("Patient missing ID, skipping")
            return

        # Extract basic patient information
        name = self.extract_name(patient.get("name", []))
        gender = patient.get("gender", "")
        birth_date = patient.get("birthDate", "")

        # Extract address
        address = self.extract_address(patient.get("address", []))

        # Extract telecom
        telecom = self.extract_telecom(patient.get("telecom", []))

        # Extract extensions for additional data
        extensions = patient.get("extension", [])
        race = ""
        ethnicity = ""
        birth_place = {}

        for ext in extensions:
            url = ext.get("url", "")
            if "us-core-race" in url:
                for sub_ext in ext.get("extension", []):
                    if sub_ext.get("url") == "text":
                        race = sub_ext.get("valueString", "")
            elif "us-core-ethnicity" in url:
                for sub_ext in ext.get("extension", []):
                    if sub_ext.get("url") == "text":
                        ethnicity = sub_ext.get("valueString", "")
            elif "patient-birthPlace" in url:
                birth_place = ext.get("valueAddress", {})

        # Create patient node
        query = """
        MERGE (p:Patient {id: $patient_id})
        SET p.name = $name,
            p.gender = $gender,
            p.birthDate = $birth_date,
            p.race = $race,
            p.ethnicity = $ethnicity,
            p.addressLine = $address_line,
            p.city = $city,
            p.state = $state,
            p.postalCode = $postal_code,
            p.country = $country,
            p.phone = $phone,
            p.email = $email,
            p.birthPlaceCity = $birth_place_city,
            p.birthPlaceState = $birth_place_state,
            p.birthPlaceCountry = $birth_place_country
        """

        session.run(query, {
            "patient_id": patient_id,
            "name": name,
            "gender": gender,
            "birth_date": birth_date,
            "race": race,
            "ethnicity": ethnicity,
            "address_line": address.get("line", ""),
            "city": address.get("city", ""),
            "state": address.get("state", ""),
            "postal_code": address.get("postalCode", ""),
            "country": address.get("country", ""),
            "phone": telecom.get("phone", ""),
            "email": telecom.get("email", ""),
            "birth_place_city": birth_place.get("city", ""),
            "birth_place_state": birth_place.get("state", ""),
            "birth_place_country": birth_place.get("country", "")
        })

        self.logger.info(f"Injected Patient: {name} (ID: {patient_id})")

    def inject_encounter(self, session: Session, encounter: Dict[str, Any]):
        """Inject an Encounter resource into Neo4j."""
        encounter_id = encounter.get("id")
        if not encounter_id:
            return

        # Extract encounter information
        status = encounter.get("status", "")
        encounter_class = encounter.get("class", {}).get("display", "")
        encounter_type = self.extract_coding_display(encounter.get("type", [{}])[0].get("coding", []))

        # Extract period
        period = encounter.get("period", {})
        start = period.get("start", "")
        end = period.get("end", "")

        # Extract patient reference
        subject = encounter.get("subject", {})
        patient_id = subject.get("reference", "").replace("urn:uuid:", "")

        # Create encounter node
        query = """
        MERGE (e:Encounter {id: $encounter_id})
        SET e.status = $status,
            e.class = $encounter_class,
            e.type = $encounter_type,
            e.start = $start,
            e.end = $end
        """

        session.run(query, {
            "encounter_id": encounter_id,
            "status": status,
            "encounter_class": encounter_class,
            "encounter_type": encounter_type,
            "start": start,
            "end": end
        })

        # Link to patient
        if patient_id:
            link_query = """
            MATCH (p:Patient {id: $patient_id})
            MATCH (e:Encounter {id: $encounter_id})
            MERGE (p)-[:HAS_ENCOUNTER]->(e)
            """
            session.run(link_query, {
                "patient_id": patient_id,
                "encounter_id": encounter_id
            })

        self.logger.info(f"Injected Encounter: {encounter_type} (ID: {encounter_id})")

    def inject_condition(self, session: Session, condition: Dict[str, Any]):
        """Inject a Condition resource into Neo4j."""
        condition_id = condition.get("id")
        if not condition_id:
            return

        # Extract condition information
        clinical_status = condition.get("clinicalStatus", {}).get("coding", [{}])[0].get("display", "")
        verification_status = condition.get("verificationStatus", {}).get("coding", [{}])[0].get("display", "")

        # Extract condition code
        code_info = condition.get("code", {})
        condition_code = ""
        condition_display = ""
        if code_info.get("coding"):
            coding = code_info["coding"][0]
            condition_code = coding.get("code", "")
            condition_display = coding.get("display", "")

        # Extract dates
        onset_datetime = condition.get("onsetDateTime", "")
        recorded_date = condition.get("recordedDate", "")

        # Extract references
        subject = condition.get("subject", {})
        patient_id = subject.get("reference", "").replace("urn:uuid:", "")

        encounter = condition.get("encounter", {})
        encounter_id = encounter.get("reference", "").replace("urn:uuid:", "")

        # Create condition node
        query = """
        MERGE (c:Condition {id: $condition_id})
        SET c.clinicalStatus = $clinical_status,
            c.verificationStatus = $verification_status,
            c.code = $condition_code,
            c.display = $condition_display,
            c.onsetDateTime = $onset_datetime,
            c.recordedDate = $recorded_date
        """

        session.run(query, {
            "condition_id": condition_id,
            "clinical_status": clinical_status,
            "verification_status": verification_status,
            "condition_code": condition_code,
            "condition_display": condition_display,
            "onset_datetime": onset_datetime,
            "recorded_date": recorded_date
        })

        # Link to patient
        if patient_id:
            link_query = """
            MATCH (p:Patient {id: $patient_id})
            MATCH (c:Condition {id: $condition_id})
            MERGE (p)-[:HAS_CONDITION]->(c)
            """
            session.run(link_query, {
                "patient_id": patient_id,
                "condition_id": condition_id
            })

        # Link to encounter
        if encounter_id:
            link_query = """
            MATCH (e:Encounter {id: $encounter_id})
            MATCH (c:Condition {id: $condition_id})
            MERGE (e)-[:HAS_CONDITION]->(c)
            """
            session.run(link_query, {
                "encounter_id": encounter_id,
                "condition_id": condition_id
            })

        self.logger.info(f"Injected Condition: {condition_display} (ID: {condition_id})")

    def inject_observation(self, session: Session, observation: Dict[str, Any]):
        """Inject an Observation resource into Neo4j."""
        observation_id = observation.get("id")
        if not observation_id:
            return

        # Extract observation information
        status = observation.get("status", "")

        # Extract observation code
        code_info = observation.get("code", {})
        observation_code = ""
        observation_display = ""
        if code_info.get("coding"):
            coding = code_info["coding"][0]
            observation_code = coding.get("code", "")
            observation_display = coding.get("display", "")

        # Extract value - can be various types
        value = ""
        value_unit = ""
        if "valueQuantity" in observation:
            value_qty = observation["valueQuantity"]
            value = str(value_qty.get("value", ""))
            value_unit = value_qty.get("unit", "")
        elif "valueString" in observation:
            value = observation["valueString"]
        elif "valueCodeableConcept" in observation:
            value_concept = observation["valueCodeableConcept"]
            if value_concept.get("coding"):
                value = value_concept["coding"][0].get("display", "")

        # Extract dates
        effective_datetime = observation.get("effectiveDateTime", "")
        issued = observation.get("issued", "")

        # Extract references
        subject = observation.get("subject", {})
        patient_id = subject.get("reference", "").replace("urn:uuid:", "")

        encounter = observation.get("encounter", {})
        encounter_id = encounter.get("reference", "").replace("urn:uuid:", "")

        # Create observation node
        query = """
        MERGE (o:Observation {id: $observation_id})
        SET o.status = $status,
            o.code = $observation_code,
            o.display = $observation_display,
            o.value = $value,
            o.unit = $value_unit,
            o.effectiveDateTime = $effective_datetime,
            o.issued = $issued
        """

        session.run(query, {
            "observation_id": observation_id,
            "status": status,
            "observation_code": observation_code,
            "observation_display": observation_display,
            "value": value,
            "value_unit": value_unit,
            "effective_datetime": effective_datetime,
            "issued": issued
        })

        # Link to patient
        if patient_id:
            link_query = """
            MATCH (p:Patient {id: $patient_id})
            MATCH (o:Observation {id: $observation_id})
            MERGE (p)-[:HAS_OBSERVATION]->(o)
            """
            session.run(link_query, {
                "patient_id": patient_id,
                "observation_id": observation_id
            })

        # Link to encounter
        if encounter_id:
            link_query = """
            MATCH (e:Encounter {id: $encounter_id})
            MATCH (o:Observation {id: $observation_id})
            MERGE (e)-[:HAS_OBSERVATION]->(o)
            """
            session.run(link_query, {
                "encounter_id": encounter_id,
                "observation_id": observation_id
            })

        self.logger.info(f"Injected Observation: {observation_display} (ID: {observation_id})")

    def inject_fhir_bundle(self, bundle_data: Dict[str, Any]):
        """
        Inject a complete FHIR Bundle into Neo4j.

        Args:
            bundle_data: Parsed FHIR Bundle JSON
        """
        if bundle_data.get("resourceType") != "Bundle":
            raise ValueError("Input data is not a FHIR Bundle")

        entries = bundle_data.get("entry", [])

        # Count resources by type
        resource_counts = {}

        with self.driver.session() as session:
            # First pass: inject all resources
            for entry in entries:
                resource = entry.get("resource", {})
                resource_type = resource.get("resourceType")

                if not resource_type:
                    continue

                resource_counts[resource_type] = resource_counts.get(resource_type, 0) + 1

                try:
                    if resource_type == "Patient":
                        self.inject_patient(session, resource)
                    elif resource_type == "Encounter":
                        self.inject_encounter(session, resource)
                    elif resource_type == "Condition":
                        self.inject_condition(session, resource)
                    elif resource_type == "Observation":
                        self.inject_observation(session, resource)
                    else:
                        self.logger.info(f"Skipping unsupported resource type: {resource_type}")

                except Exception as e:
                    self.logger.error(f"Error injecting {resource_type} resource: {e}")

        # Log summary
        self.logger.info("FHIR Bundle injection completed:")
        for resource_type, count in resource_counts.items():
            self.logger.info(f"  {resource_type}: {count} resources")

    def inject_from_file(self, file_path: str):
        """
        Load and inject FHIR Bundle from a JSON file.

        Args:
            file_path: Path to the FHIR Bundle JSON file
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                bundle_data = json.load(f)

            self.inject_fhir_bundle(bundle_data)
            self.logger.info(f"Successfully processed file: {file_path}")

        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {e}")
            raise

    def inject_from_directory(self, directory_path: str):
        """
        Load and inject all FHIR Bundle JSON files from a directory.

        Args:
            directory_path: Path to directory containing FHIR Bundle JSON files
        """
        data_dir = Path(directory_path)

        if not data_dir.exists():
            raise FileNotFoundError(f"Directory not found: {directory_path}")

        json_files = list(data_dir.glob("*.json"))

        if not json_files:
            self.logger.warning(f"No JSON files found in {directory_path}")
            return

        self.logger.info(f"Found {len(json_files)} JSON files to process")

        for json_file in json_files:
            try:
                self.inject_from_file(str(json_file))
            except Exception as e:
                self.logger.error(f"Failed to process {json_file}: {e}")
                continue

    def get_database_summary(self) -> Dict[str, int]:
        """Get a summary of nodes and relationships in the database."""
        with self.driver.session() as session:
            # Count nodes by label
            node_counts = {}
            labels = ["Patient", "Encounter", "Condition", "Observation"]

            for label in labels:
                result = session.run(f"MATCH (n:{label}) RETURN count(n) as count")
                count = result.single()["count"]
                node_counts[label] = count

            # Count relationships
            relationship_counts = {}
            rel_types = ["HAS_ENCOUNTER", "HAS_CONDITION", "HAS_OBSERVATION"]

            for rel_type in rel_types:
                result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count")
                count = result.single()["count"]
                relationship_counts[rel_type] = count

            return {
                "nodes": node_counts,
                "relationships": relationship_counts
            }


def main():
    """Main function to run the FHIR injection process."""
    import config

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    try:
        with FHIRNeo4jInjector(config.NEO4J_URI, config.NEO4J_USER, config.NEO4J_PASSWORD) as injector:
            # Create constraints for better performance
            if config.CREATE_CONSTRAINTS:
                injector.create_constraints()

            # Clear database if requested
            if config.CLEAR_DATABASE_ON_START:
                injector.clear_database()

            # Inject all FHIR files from the data directory
            injector.inject_from_directory(config.DATA_DIRECTORY)

            # Print summary
            summary = injector.get_database_summary()
            print("\n=== Database Summary ===")
            print("Nodes:")
            for label, count in summary["nodes"].items():
                print(f"  {label}: {count}")
            print("\nRelationships:")
            for rel_type, count in summary["relationships"].items():
                print(f"  {rel_type}: {count}")

    except Exception as e:
        logging.error(f"Application error: {e}")
        raise


if __name__ == "__main__":
    main()