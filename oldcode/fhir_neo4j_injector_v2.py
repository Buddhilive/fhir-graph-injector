"""
FHIR to Neo4j Graph Injector (Version 2)

Updated implementation following the reference tutorial pattern with:
- Two-phase approach: nodes first, then relationships
- Support for more FHIR resource types
- Temporal relationship creation
- Improved data extraction patterns
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from neo4j import GraphDatabase, Session
from datetime import datetime


class FHIRNeo4jInjectorV2:
    """
    Enhanced FHIR to Neo4j injector following the reference tutorial pattern.
    """

    def __init__(self, uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = "password"):
        """Initialize the injector with Neo4j connection parameters."""
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
        """Create uniqueness constraints for all resource types."""
        constraints = [
            "CREATE CONSTRAINT pid FOR (p:Patient) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT prid FOR (pr:Practitioner) REQUIRE pr.id IS UNIQUE",
            "CREATE CONSTRAINT oid FOR (o:Organization) REQUIRE o.id IS UNIQUE",
            "CREATE CONSTRAINT eid FOR (e:Encounter) REQUIRE e.id IS UNIQUE",
            "CREATE CONSTRAINT cid FOR (c:Condition) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT obsid FOR (obs:Observation) REQUIRE obs.id IS UNIQUE",
            "CREATE CONSTRAINT mrid FOR (mr:MedicationRequest) REQUIRE mr.id IS UNIQUE",
            "CREATE CONSTRAINT procid FOR (proc:Procedure) REQUIRE proc.id IS UNIQUE"
        ]

        with self.driver.session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                    self.logger.info(f"Created constraint: {constraint}")
                except Exception as e:
                    self.logger.warning(f"Constraint already exists or failed: {e}")

    def extract_reference_id(self, reference: str) -> str:
        """Extract ID from FHIR reference string."""
        if not reference:
            return ""
        # Handle both "urn:uuid:id" and "ResourceType/id" formats
        if "urn:uuid:" in reference:
            return reference.replace("urn:uuid:", "")
        elif "/" in reference:
            return reference.split("/")[-1]
        return reference

    def safe_get(self, data: Dict, path: str, default: str = ""):
        """Safely get nested dictionary values using dot notation."""
        keys = path.split('.')
        current = data
        try:
            for key in keys:
                if '[' in key and ']' in key:
                    # Handle array access like "name[0]"
                    array_key = key.split('[')[0]
                    index = int(key.split('[')[1].split(']')[0])
                    current = current[array_key][index]
                else:
                    current = current[key]
            return current if current is not None else default
        except (KeyError, IndexError, TypeError):
            return default

    def create_patient_nodes(self, session: Session, bundle_data: Dict[str, Any]):
        """Create Patient nodes from FHIR Bundle."""
        entries = bundle_data.get("entry", [])
        patient_count = 0

        for entry in entries:
            resource = entry.get("resource", {})
            if resource.get("resourceType") != "Patient":
                continue

            try:
                # Extract patient data following the reference pattern
                patient_id = resource.get("id", "")

                # Name extraction
                names = resource.get("name", [{}])
                fname = ""
                lname = ""
                if names:
                    name = names[0]
                    given = name.get("given", [])
                    fname = " ".join(given) if given else ""
                    lname = name.get("family", "")

                # Basic demographics
                sex = resource.get("gender", "")
                birth_date = resource.get("birthDate", "")

                # Address extraction
                addresses = resource.get("address", [{}])
                city = ""
                state = ""
                zip_code = ""
                if addresses:
                    addr = addresses[0]
                    city = addr.get("city", "")
                    state = addr.get("state", "")
                    zip_code = addr.get("postalCode", "")

                # Extensions for race/ethnicity
                race = ""
                ethnicity = ""
                for ext in resource.get("extension", []):
                    if "us-core-race" in ext.get("url", ""):
                        for sub_ext in ext.get("extension", []):
                            if sub_ext.get("url") == "text":
                                race = sub_ext.get("valueString", "")
                    elif "us-core-ethnicity" in ext.get("url", ""):
                        for sub_ext in ext.get("extension", []):
                            if sub_ext.get("url") == "text":
                                ethnicity = sub_ext.get("valueString", "")

                # Create patient node
                query = """
                CREATE (p:Patient {
                    id: $id,
                    fname: $fname,
                    lname: $lname,
                    sex: $sex,
                    birthDate: $birthDate,
                    city: $city,
                    state: $state,
                    zip: $zip,
                    race: $race,
                    ethnicity: $ethnicity
                })
                """

                session.run(query, {
                    "id": patient_id,
                    "fname": fname,
                    "lname": lname,
                    "sex": sex,
                    "birthDate": birth_date,
                    "city": city,
                    "state": state,
                    "zip": zip_code,
                    "race": race,
                    "ethnicity": ethnicity
                })

                patient_count += 1

            except Exception as e:
                self.logger.error(f"Error creating patient node: {e}")

        self.logger.info(f"Created {patient_count} Patient nodes")

    def create_practitioner_nodes(self, session: Session, bundle_data: Dict[str, Any]):
        """Create Practitioner nodes from FHIR Bundle."""
        entries = bundle_data.get("entry", [])
        practitioner_count = 0

        for entry in entries:
            resource = entry.get("resource", {})
            if resource.get("resourceType") != "Practitioner":
                continue

            try:
                practitioner_id = resource.get("id", "")

                # Name extraction
                names = resource.get("name", [{}])
                fname = ""
                name_full = ""
                if names:
                    name = names[0]
                    given = name.get("given", [])
                    family = name.get("family", "")
                    fname = " ".join(given) if given else ""
                    name_full = f"{fname} {family}".strip()

                gender = resource.get("gender", "")

                query = """
                CREATE (pr:Practitioner {
                    id: $id,
                    fname: $fname,
                    name: $name,
                    gender: $gender
                })
                """

                session.run(query, {
                    "id": practitioner_id,
                    "fname": fname,
                    "name": name_full,
                    "gender": gender
                })

                practitioner_count += 1

            except Exception as e:
                self.logger.error(f"Error creating practitioner node: {e}")

        self.logger.info(f"Created {practitioner_count} Practitioner nodes")

    def create_organization_nodes(self, session: Session, bundle_data: Dict[str, Any]):
        """Create Organization nodes from FHIR Bundle."""
        entries = bundle_data.get("entry", [])
        org_count = 0

        for entry in entries:
            resource = entry.get("resource", {})
            if resource.get("resourceType") != "Organization":
                continue

            try:
                org_id = resource.get("id", "")
                name = resource.get("name", "")

                # Type extraction
                org_type = ""
                types = resource.get("type", [])
                if types and types[0].get("coding"):
                    org_type = types[0]["coding"][0].get("display", "")

                # Address extraction
                addresses = resource.get("address", [{}])
                address_city = ""
                address_state = ""
                address_line = ""
                if addresses:
                    addr = addresses[0]
                    address_city = addr.get("city", "")
                    address_state = addr.get("state", "")
                    lines = addr.get("line", [])
                    address_line = ", ".join(lines) if lines else ""

                query = """
                CREATE (o:Organization {
                    id: $id,
                    name: $name,
                    orgtype: $orgtype,
                    addressCity: $addressCity,
                    addressState: $addressState,
                    addressLine: $addressLine
                })
                """

                session.run(query, {
                    "id": org_id,
                    "name": name,
                    "orgtype": org_type,
                    "addressCity": address_city,
                    "addressState": address_state,
                    "addressLine": address_line
                })

                org_count += 1

            except Exception as e:
                self.logger.error(f"Error creating organization node: {e}")

        self.logger.info(f"Created {org_count} Organization nodes")

    def create_encounter_nodes(self, session: Session, bundle_data: Dict[str, Any]):
        """Create Encounter nodes from FHIR Bundle."""
        entries = bundle_data.get("entry", [])
        encounter_count = 0

        for entry in entries:
            resource = entry.get("resource", {})
            if resource.get("resourceType") != "Encounter":
                continue

            try:
                enc_id = resource.get("id", "")
                status = resource.get("status", "")

                # Type extraction
                enc_type = ""
                types = resource.get("type", [])
                if types and types[0].get("coding"):
                    enc_type = types[0]["coding"][0].get("display", "")

                # Period extraction
                period = resource.get("period", {})
                enc_start = period.get("start", "")
                enc_end = period.get("end", "")

                # Patient reference
                subject = resource.get("subject", {})
                pid = self.extract_reference_id(subject.get("reference", ""))

                # Organization reference
                service_provider = resource.get("serviceProvider", {})
                org_id = self.extract_reference_id(service_provider.get("reference", ""))

                # Provider reference (if available)
                provider = ""
                participants = resource.get("participant", [])
                for participant in participants:
                    individual = participant.get("individual", {})
                    if individual:
                        provider = self.extract_reference_id(individual.get("reference", ""))
                        break

                query = """
                CREATE (e:Encounter {
                    id: $id,
                    type: $type,
                    status: $status,
                    encstart: $encstart,
                    encend: $encend,
                    pid: $pid,
                    orgid: $orgid,
                    provider: $provider
                })
                """

                session.run(query, {
                    "id": enc_id,
                    "type": enc_type,
                    "status": status,
                    "encstart": enc_start,
                    "encend": enc_end,
                    "pid": pid,
                    "orgid": org_id,
                    "provider": provider
                })

                encounter_count += 1

            except Exception as e:
                self.logger.error(f"Error creating encounter node: {e}")

        self.logger.info(f"Created {encounter_count} Encounter nodes")

    def create_condition_nodes(self, session: Session, bundle_data: Dict[str, Any]):
        """Create Condition nodes from FHIR Bundle."""
        entries = bundle_data.get("entry", [])
        condition_count = 0

        for entry in entries:
            resource = entry.get("resource", {})
            if resource.get("resourceType") != "Condition":
                continue

            try:
                condition_id = resource.get("id", "")

                # Status extraction
                clinical_status = ""
                verification_status = ""

                clin_stat = resource.get("clinicalStatus", {})
                if clin_stat.get("coding"):
                    clinical_status = clin_stat["coding"][0].get("display", "")

                verif_stat = resource.get("verificationStatus", {})
                if verif_stat.get("coding"):
                    verification_status = verif_stat["coding"][0].get("display", "")

                # Condition code
                condition_code = ""
                condition_type = ""
                code_info = resource.get("code", {})
                if code_info.get("coding"):
                    coding = code_info["coding"][0]
                    condition_code = coding.get("code", "")
                    condition_type = coding.get("display", "")

                # Dates
                onset_date = resource.get("onsetDateTime", "")
                recorded_date = resource.get("recordedDate", "")

                # References
                subject = resource.get("subject", {})
                pid = self.extract_reference_id(subject.get("reference", ""))

                encounter = resource.get("encounter", {})
                enc_ref = self.extract_reference_id(encounter.get("reference", ""))

                query = """
                CREATE (c:Condition {
                    id: $id,
                    type: $type,
                    clinicalstatus: $clinicalstatus,
                    verificationstatus: $verificationstatus,
                    conditioncode: $conditioncode,
                    pid: $pid,
                    encref: $encref,
                    onsetdate: $onsetdate,
                    recordeddata: $recordeddate
                })
                """

                session.run(query, {
                    "id": condition_id,
                    "type": condition_type,
                    "clinicalstatus": clinical_status,
                    "verificationstatus": verification_status,
                    "conditioncode": condition_code,
                    "pid": pid,
                    "encref": enc_ref,
                    "onsetdate": onset_date,
                    "recordeddate": recorded_date
                })

                condition_count += 1

            except Exception as e:
                self.logger.error(f"Error creating condition node: {e}")

        self.logger.info(f"Created {condition_count} Condition nodes")

    def create_observation_nodes(self, session: Session, bundle_data: Dict[str, Any]):
        """Create Observation nodes from FHIR Bundle."""
        entries = bundle_data.get("entry", [])
        observation_count = 0

        for entry in entries:
            resource = entry.get("resource", {})
            if resource.get("resourceType") != "Observation":
                continue

            try:
                obs_id = resource.get("id", "")
                status = resource.get("status", "")

                # Code extraction
                obs_code = ""
                obs_type = ""
                code_info = resource.get("code", {})
                if code_info.get("coding"):
                    coding = code_info["coding"][0]
                    obs_code = coding.get("code", "")
                    obs_type = coding.get("display", "")

                # Value extraction
                value = ""
                unit = ""
                if "valueQuantity" in resource:
                    value_qty = resource["valueQuantity"]
                    value = str(value_qty.get("value", ""))
                    unit = value_qty.get("unit", "")
                elif "valueString" in resource:
                    value = resource["valueString"]
                elif "valueCodeableConcept" in resource:
                    value_concept = resource["valueCodeableConcept"]
                    if value_concept.get("coding"):
                        value = value_concept["coding"][0].get("display", "")

                # Dates
                effective_date = resource.get("effectiveDateTime", "")
                issued = resource.get("issued", "")

                # References
                subject = resource.get("subject", {})
                pid = self.extract_reference_id(subject.get("reference", ""))

                encounter = resource.get("encounter", {})
                enc_id = self.extract_reference_id(encounter.get("reference", ""))

                query = """
                CREATE (obs:Observation {
                    id: $id,
                    type: $type,
                    status: $status,
                    code: $code,
                    value: $value,
                    unit: $unit,
                    effectivedate: $effectivedate,
                    issued: $issued,
                    pid: $pid,
                    encid: $encid
                })
                """

                session.run(query, {
                    "id": obs_id,
                    "type": obs_type,
                    "status": status,
                    "code": obs_code,
                    "value": value,
                    "unit": unit,
                    "effectivedate": effective_date,
                    "issued": issued,
                    "pid": pid,
                    "encid": enc_id
                })

                observation_count += 1

            except Exception as e:
                self.logger.error(f"Error creating observation node: {e}")

        self.logger.info(f"Created {observation_count} Observation nodes")

    def create_medication_request_nodes(self, session: Session, bundle_data: Dict[str, Any]):
        """Create MedicationRequest nodes from FHIR Bundle."""
        entries = bundle_data.get("entry", [])
        med_count = 0

        for entry in entries:
            resource = entry.get("resource", {})
            if resource.get("resourceType") != "MedicationRequest":
                continue

            try:
                med_id = resource.get("id", "")
                status = resource.get("status", "")
                intent = resource.get("intent", "")

                # Medication extraction
                medication = ""
                med_info = resource.get("medicationCodeableConcept", {})
                if med_info.get("coding"):
                    medication = med_info["coding"][0].get("display", "")

                # Dates
                authored_on = resource.get("authoredOn", "")

                # References
                subject = resource.get("subject", {})
                pid = self.extract_reference_id(subject.get("reference", ""))

                encounter = resource.get("encounter", {})
                enc_id = self.extract_reference_id(encounter.get("reference", ""))

                # Reason reference for treatment relationships
                reason_id = ""
                reason_refs = resource.get("reasonReference", [])
                if reason_refs:
                    reason_id = self.extract_reference_id(reason_refs[0].get("reference", ""))

                query = """
                CREATE (mr:MedicationRequest {
                    id: $id,
                    status: $status,
                    intent: $intent,
                    medication: $medication,
                    authoredOn: $authoredOn,
                    pid: $pid,
                    encid: $encid,
                    reasonid: $reasonid
                })
                """

                session.run(query, {
                    "id": med_id,
                    "status": status,
                    "intent": intent,
                    "medication": medication,
                    "authoredOn": authored_on,
                    "pid": pid,
                    "encid": enc_id,
                    "reasonid": reason_id
                })

                med_count += 1

            except Exception as e:
                self.logger.error(f"Error creating medication request node: {e}")

        self.logger.info(f"Created {med_count} MedicationRequest nodes")

    def create_procedure_nodes(self, session: Session, bundle_data: Dict[str, Any]):
        """Create Procedure nodes from FHIR Bundle."""
        entries = bundle_data.get("entry", [])
        proc_count = 0

        for entry in entries:
            resource = entry.get("resource", {})
            if resource.get("resourceType") != "Procedure":
                continue

            try:
                proc_id = resource.get("id", "")
                status = resource.get("status", "")

                # Procedure code
                proc_type = ""
                code_info = resource.get("code", {})
                if code_info.get("coding"):
                    proc_type = code_info["coding"][0].get("display", "")

                # Date
                performed_date = ""
                if "performedDateTime" in resource:
                    performed_date = resource["performedDateTime"]
                elif "performedPeriod" in resource:
                    period = resource["performedPeriod"]
                    performed_date = period.get("start", "")

                # References
                subject = resource.get("subject", {})
                pid = self.extract_reference_id(subject.get("reference", ""))

                encounter = resource.get("encounter", {})
                enc_id = self.extract_reference_id(encounter.get("reference", ""))

                query = """
                CREATE (proc:Procedure {
                    id: $id,
                    type: $type,
                    status: $status,
                    performedDate: $performedDate,
                    pid: $pid,
                    encid: $encid
                })
                """

                session.run(query, {
                    "id": proc_id,
                    "type": proc_type,
                    "status": status,
                    "performedDate": performed_date,
                    "pid": pid,
                    "encid": enc_id
                })

                proc_count += 1

            except Exception as e:
                self.logger.error(f"Error creating procedure node: {e}")

        self.logger.info(f"Created {proc_count} Procedure nodes")

    def create_basic_relationships(self, session: Session):
        """Create basic relationships between nodes."""
        relationships = [
            # Patient to Encounter
            {
                "query": """
                MATCH (p:Patient), (e:Encounter)
                WHERE e.pid = p.id
                CREATE (p)-[r:HASENCOUNTER]->(e)
                """,
                "name": "HASENCOUNTER"
            },
            # Patient to Condition
            {
                "query": """
                MATCH (p:Patient), (c:Condition)
                WHERE c.pid = p.id
                CREATE (p)-[r:HASCONDITION]->(c)
                """,
                "name": "HASCONDITION"
            },
            # Encounter to Observation
            {
                "query": """
                MATCH (e:Encounter), (obs:Observation)
                WHERE obs.encid = e.id
                CREATE (e)-[r:HASOBSERVATION]->(obs)
                """,
                "name": "HASOBSERVATION"
            },
            # Encounter to Condition
            {
                "query": """
                MATCH (e:Encounter), (c:Condition)
                WHERE c.encref = e.id
                CREATE (e)-[r:HASCONDITION]->(c)
                """,
                "name": "ENCOUNTER_HASCONDITION"
            },
            # MedicationRequest treatment for Condition
            {
                "query": """
                MATCH (mr:MedicationRequest), (c:Condition)
                WHERE mr.reasonid = c.id
                CREATE (mr)-[r:TREATMENTFOR]->(c)
                """,
                "name": "TREATMENTFOR"
            },
            # Patient to MedicationRequest
            {
                "query": """
                MATCH (p:Patient), (mr:MedicationRequest)
                WHERE mr.pid = p.id
                CREATE (p)-[r:HASMEDICATION]->(mr)
                """,
                "name": "HASMEDICATION"
            },
            # Patient to Procedure
            {
                "query": """
                MATCH (p:Patient), (proc:Procedure)
                WHERE proc.pid = p.id
                CREATE (p)-[r:HASPROCEDURE]->(proc)
                """,
                "name": "HASPROCEDURE"
            }
        ]

        for rel in relationships:
            try:
                result = session.run(rel["query"])
                count = result.consume().counters.relationships_created
                self.logger.info(f"Created {count} {rel['name']} relationships")
            except Exception as e:
                self.logger.error(f"Error creating {rel['name']} relationships: {e}")

    def create_temporal_relationships(self, session: Session):
        """Create temporal relationships for conditions."""
        try:
            # First condition for each patient
            first_condition_query = """
            MATCH (p:Patient)-[:HASCONDITION]->(c:Condition)
            WHERE c.onsetdate IS NOT NULL AND c.onsetdate <> ""
            WITH p, c ORDER BY c.onsetdate ASC
            WITH p, COLLECT(c)[0] as firstCondition
            CREATE (p)-[r:FIRSTCONDITION]->(firstCondition)
            """

            result = session.run(first_condition_query)
            count = result.consume().counters.relationships_created
            self.logger.info(f"Created {count} FIRSTCONDITION relationships")

            # Latest condition for each patient
            latest_condition_query = """
            MATCH (p:Patient)-[:HASCONDITION]->(c:Condition)
            WHERE c.onsetdate IS NOT NULL AND c.onsetdate <> ""
            WITH p, c ORDER BY c.onsetdate DESC
            WITH p, COLLECT(c)[0] as latestCondition
            CREATE (p)-[r:LATESTCONDITION]->(latestCondition)
            """

            result = session.run(latest_condition_query)
            count = result.consume().counters.relationships_created
            self.logger.info(f"Created {count} LATESTCONDITION relationships")

            # Next condition relationships
            next_condition_query = """
            MATCH (p:Patient)-[:HASCONDITION]->(c:Condition)
            WHERE c.onsetdate IS NOT NULL AND c.onsetdate <> ""
            WITH p, c ORDER BY c.onsetdate ASC
            WITH p, COLLECT(c) as conditions
            UNWIND range(0, size(conditions)-2) as i
            WITH conditions[i] as current, conditions[i+1] as next
            CREATE (current)-[r:NEXTCONDITION]->(next)
            """

            result = session.run(next_condition_query)
            count = result.consume().counters.relationships_created
            self.logger.info(f"Created {count} NEXTCONDITION relationships")

        except Exception as e:
            self.logger.error(f"Error creating temporal relationships: {e}")

    def inject_fhir_bundle_v2(self, bundle_data: Dict[str, Any]):
        """
        Enhanced FHIR Bundle injection with two-phase approach.
        """
        if bundle_data.get("resourceType") != "Bundle":
            raise ValueError("Input data is not a FHIR Bundle")

        with self.driver.session() as session:
            # Phase 1: Create all nodes
            self.logger.info("Phase 1: Creating nodes...")

            self.create_patient_nodes(session, bundle_data)
            self.create_practitioner_nodes(session, bundle_data)
            self.create_organization_nodes(session, bundle_data)
            self.create_encounter_nodes(session, bundle_data)
            self.create_condition_nodes(session, bundle_data)
            self.create_observation_nodes(session, bundle_data)
            self.create_medication_request_nodes(session, bundle_data)
            self.create_procedure_nodes(session, bundle_data)

            # Phase 2: Create relationships
            self.logger.info("Phase 2: Creating relationships...")

            self.create_basic_relationships(session)
            self.create_temporal_relationships(session)

    def inject_from_file_v2(self, file_path: str):
        """Load and inject FHIR Bundle from a JSON file using v2 approach."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                bundle_data = json.load(f)

            self.inject_fhir_bundle_v2(bundle_data)
            self.logger.info(f"Successfully processed file: {file_path}")

        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {e}")
            raise

    def inject_from_directory_v2(self, directory_path: str):
        """Load and inject all FHIR Bundle JSON files from a directory using v2 approach."""
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
                self.inject_from_file_v2(str(json_file))
            except Exception as e:
                self.logger.error(f"Failed to process {json_file}: {e}")
                continue

    def get_database_summary_v2(self) -> Dict[str, int]:
        """Get enhanced summary of nodes and relationships in the database."""
        with self.driver.session() as session:
            # Count nodes by label
            node_counts = {}
            labels = ["Patient", "Practitioner", "Organization", "Encounter",
                     "Condition", "Observation", "MedicationRequest", "Procedure"]

            for label in labels:
                result = session.run(f"MATCH (n:{label}) RETURN count(n) as count")
                count = result.single()["count"]
                node_counts[label] = count

            # Count relationships
            relationship_counts = {}
            rel_types = ["HASENCOUNTER", "HASCONDITION", "HASOBSERVATION",
                        "TREATMENTFOR", "HASMEDICATION", "HASPROCEDURE",
                        "FIRSTCONDITION", "LATESTCONDITION", "NEXTCONDITION"]

            for rel_type in rel_types:
                result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count")
                count = result.single()["count"]
                relationship_counts[rel_type] = count

            return {
                "nodes": node_counts,
                "relationships": relationship_counts
            }


def main_v2():
    """Main function for v2 implementation."""
    import config

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    try:
        with FHIRNeo4jInjectorV2(config.NEO4J_URI, config.NEO4J_USER, config.NEO4J_PASSWORD) as injector:
            # Create constraints for better performance
            if config.CREATE_CONSTRAINTS:
                injector.create_constraints()

            # Clear database if requested
            if config.CLEAR_DATABASE_ON_START:
                injector.clear_database()

            # Inject all FHIR files from the data directory
            injector.inject_from_directory_v2(config.DATA_DIRECTORY)

            # Print summary
            summary = injector.get_database_summary_v2()
            print("\n=== Database Summary (V2) ===")
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
    main_v2()