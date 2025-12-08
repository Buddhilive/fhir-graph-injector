"""
FHIR Graph Builder for Neo4j
Processes FHIR Bundle JSON files and creates a graph database representation
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable


class FHIRGraphBuilder:
    """Build Neo4j graph from FHIR patient data"""
    
    def __init__(self, uri: str, user: str, password: str):
        """
        Initialize the FHIR Graph Builder
        
        Args:
            uri: Neo4j connection URI
            user: Neo4j username
            password: Neo4j password
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.logger = logging.getLogger(__name__)
        
    def close(self):
        """Close the Neo4j driver connection"""
        if self.driver:
            self.driver.close()
            
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        
    def create_constraints(self):
        """Create unique constraints for all node types"""
        constraints = [
            "CREATE CONSTRAINT patient_id IF NOT EXISTS FOR (p:Patient) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT practitioner_id IF NOT EXISTS FOR (p:Practitioner) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT organization_id IF NOT EXISTS FOR (o:Organization) REQUIRE o.id IS UNIQUE",
            "CREATE CONSTRAINT encounter_id IF NOT EXISTS FOR (e:Encounter) REQUIRE e.id IS UNIQUE",
            "CREATE CONSTRAINT condition_id IF NOT EXISTS FOR (c:Condition) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT observation_id IF NOT EXISTS FOR (o:Observation) REQUIRE o.id IS UNIQUE",
            "CREATE CONSTRAINT medication_request_id IF NOT EXISTS FOR (m:MedicationRequest) REQUIRE m.id IS UNIQUE",
            "CREATE CONSTRAINT procedure_id IF NOT EXISTS FOR (pr:Procedure) REQUIRE pr.id IS UNIQUE",
        ]
        
        with self.driver.session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                    self.logger.info(f"Created constraint: {constraint.split('FOR')[1].split('REQUIRE')[0].strip()}")
                except Exception as e:
                    self.logger.warning(f"Constraint may already exist: {e}")
                    
    def clear_database(self):
        """Clear all nodes and relationships from the database"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            self.logger.info("Database cleared")
            
    def _safe_get(self, data: Dict, *keys, default=None):
        """Safely navigate nested dictionary structure"""
        for key in keys:
            if isinstance(data, dict):
                data = data.get(key, {})
            elif isinstance(data, list) and len(data) > 0:
                data = data[0] if isinstance(key, int) or key == 0 else data
            else:
                return default
        return data if data != {} else default
        
    def create_patient_nodes(self, bundle: Dict):
        """Create Patient nodes from FHIR bundle"""
        entries = bundle.get("entry", [])
        
        with self.driver.session() as session:
            for entry in entries:
                resource = entry.get("resource", {})
                if resource.get("resourceType") != "Patient":
                    continue
                    
                # Extract patient data
                patient_data = {
                    "id": resource.get("id"),
                    "fname": self._safe_get(resource, "name", 0, "given", 0),
                    "lname": self._safe_get(resource, "name", 0, "family"),
                    "gender": resource.get("gender"),
                    "birthDate": resource.get("birthDate"),
                    "city": self._safe_get(resource, "address", 0, "city"),
                    "state": self._safe_get(resource, "address", 0, "state"),
                    "postalCode": self._safe_get(resource, "address", 0, "postalCode"),
                    "country": self._safe_get(resource, "address", 0, "country"),
                    "addressLine": self._safe_get(resource, "address", 0, "line", 0),
                    "maritalStatus": self._safe_get(resource, "maritalStatus", "text"),
                    "race": None,
                    "ethnicity": None,
                }
                
                # Extract race and ethnicity from extensions
                extensions = resource.get("extension", [])
                for ext in extensions:
                    if "us-core-race" in ext.get("url", ""):
                        patient_data["race"] = self._safe_get(ext, "extension", 0, "valueCoding", "display")
                    elif "us-core-ethnicity" in ext.get("url", ""):
                        patient_data["ethnicity"] = self._safe_get(ext, "extension", 0, "valueCoding", "display")
                
                # Create node
                query = """
                MERGE (p:Patient {id: $id})
                SET p.fname = $fname,
                    p.lname = $lname,
                    p.gender = $gender,
                    p.birthDate = $birthDate,
                    p.city = $city,
                    p.state = $state,
                    p.postalCode = $postalCode,
                    p.country = $country,
                    p.addressLine = $addressLine,
                    p.maritalStatus = $maritalStatus,
                    p.race = $race,
                    p.ethnicity = $ethnicity
                """
                session.run(query, **patient_data)
                self.logger.debug(f"Created Patient node: {patient_data['id']}")
                
    def create_practitioner_nodes(self, bundle: Dict):
        """Create Practitioner nodes from FHIR bundle"""
        entries = bundle.get("entry", [])
        
        with self.driver.session() as session:
            for entry in entries:
                resource = entry.get("resource", {})
                if resource.get("resourceType") != "Practitioner":
                    continue
                    
                practitioner_data = {
                    "id": resource.get("id"),
                    "fname": self._safe_get(resource, "name", 0, "given", 0),
                    "lname": self._safe_get(resource, "name", 0, "family"),
                    "gender": resource.get("gender"),
                    "prefix": self._safe_get(resource, "name", 0, "prefix", 0),
                }
                
                query = """
                MERGE (pr:Practitioner {id: $id})
                SET pr.fname = $fname,
                    pr.lname = $lname,
                    pr.gender = $gender,
                    pr.prefix = $prefix
                """
                session.run(query, **practitioner_data)
                self.logger.debug(f"Created Practitioner node: {practitioner_data['id']}")
                
    def create_organization_nodes(self, bundle: Dict):
        """Create Organization nodes from FHIR bundle"""
        entries = bundle.get("entry", [])
        
        with self.driver.session() as session:
            for entry in entries:
                resource = entry.get("resource", {})
                if resource.get("resourceType") != "Organization":
                    continue
                    
                org_data = {
                    "id": resource.get("id"),
                    "name": resource.get("name"),
                    "orgtype": self._safe_get(resource, "type", 0, "coding", 0, "display"),
                    "addressCity": self._safe_get(resource, "address", 0, "city"),
                    "addressState": self._safe_get(resource, "address", 0, "state"),
                    "addressLine": self._safe_get(resource, "address", 0, "line", 0),
                }
                
                query = """
                MERGE (o:Organization {id: $id})
                SET o.name = $name,
                    o.orgtype = $orgtype,
                    o.addressCity = $addressCity,
                    o.addressState = $addressState,
                    o.addressLine = $addressLine
                """
                session.run(query, **org_data)
                self.logger.debug(f"Created Organization node: {org_data['id']}")
                
    def create_encounter_nodes(self, bundle: Dict):
        """Create Encounter nodes from FHIR bundle"""
        entries = bundle.get("entry", [])
        
        with self.driver.session() as session:
            for entry in entries:
                resource = entry.get("resource", {})
                if resource.get("resourceType") != "Encounter":
                    continue
                    
                encounter_data = {
                    "id": resource.get("id"),
                    "status": resource.get("status"),
                    "class": self._safe_get(resource, "class", "code"),
                    "type": self._safe_get(resource, "type", 0, "text"),
                    "encstart": self._safe_get(resource, "period", "start"),
                    "encend": self._safe_get(resource, "period", "end"),
                    "patient_ref": self._safe_get(resource, "subject", "reference"),
                    "provider_ref": self._safe_get(resource, "participant", 0, "individual", "reference"),
                    "org_ref": self._safe_get(resource, "serviceProvider", "reference"),
                }
                
                query = """
                MERGE (e:Encounter {id: $id})
                SET e.status = $status,
                    e.class = $class,
                    e.type = $type,
                    e.encstart = $encstart,
                    e.encend = $encend,
                    e.patient_ref = $patient_ref,
                    e.provider_ref = $provider_ref,
                    e.org_ref = $org_ref
                """
                session.run(query, **encounter_data)
                self.logger.debug(f"Created Encounter node: {encounter_data['id']}")
                
    def create_condition_nodes(self, bundle: Dict):
        """Create Condition nodes from FHIR bundle"""
        entries = bundle.get("entry", [])
        
        with self.driver.session() as session:
            for entry in entries:
                resource = entry.get("resource", {})
                if resource.get("resourceType") != "Condition":
                    continue
                    
                condition_data = {
                    "id": resource.get("id"),
                    "clinicalStatus": self._safe_get(resource, "clinicalStatus", "coding", 0, "code"),
                    "verificationStatus": self._safe_get(resource, "verificationStatus", "coding", 0, "code"),
                    "code": self._safe_get(resource, "code", "coding", 0, "code"),
                    "display": self._safe_get(resource, "code", "text"),
                    "onsetDateTime": resource.get("onsetDateTime"),
                    "recordedDate": resource.get("recordedDate"),
                    "patient_ref": self._safe_get(resource, "subject", "reference"),
                    "encounter_ref": self._safe_get(resource, "encounter", "reference"),
                }
                
                query = """
                MERGE (c:Condition {id: $id})
                SET c.clinicalStatus = $clinicalStatus,
                    c.verificationStatus = $verificationStatus,
                    c.code = $code,
                    c.display = $display,
                    c.onsetDateTime = $onsetDateTime,
                    c.recordedDate = $recordedDate,
                    c.patient_ref = $patient_ref,
                    c.encounter_ref = $encounter_ref
                """
                session.run(query, **condition_data)
                self.logger.debug(f"Created Condition node: {condition_data['id']}")
                
    def create_observation_nodes(self, bundle: Dict):
        """Create Observation nodes from FHIR bundle"""
        entries = bundle.get("entry", [])
        
        with self.driver.session() as session:
            for entry in entries:
                resource = entry.get("resource", {})
                if resource.get("resourceType") != "Observation":
                    continue
                    
                observation_data = {
                    "id": resource.get("id"),
                    "status": resource.get("status"),
                    "category": self._safe_get(resource, "category", 0, "coding", 0, "display"),
                    "code": self._safe_get(resource, "code", "coding", 0, "code"),
                    "display": self._safe_get(resource, "code", "text"),
                    "effectiveDateTime": resource.get("effectiveDateTime"),
                    "value": self._safe_get(resource, "valueQuantity", "value"),
                    "unit": self._safe_get(resource, "valueQuantity", "unit"),
                    "valueString": resource.get("valueString"),
                    "valueCode": self._safe_get(resource, "valueCodeableConcept", "text"),
                    "patient_ref": self._safe_get(resource, "subject", "reference"),
                    "encounter_ref": self._safe_get(resource, "encounter", "reference"),
                }
                
                query = """
                MERGE (o:Observation {id: $id})
                SET o.status = $status,
                    o.category = $category,
                    o.code = $code,
                    o.display = $display,
                    o.effectiveDateTime = $effectiveDateTime,
                    o.value = $value,
                    o.unit = $unit,
                    o.valueString = $valueString,
                    o.valueCode = $valueCode,
                    o.patient_ref = $patient_ref,
                    o.encounter_ref = $encounter_ref
                """
                session.run(query, **observation_data)
                self.logger.debug(f"Created Observation node: {observation_data['id']}")
                
    def create_medication_request_nodes(self, bundle: Dict):
        """Create MedicationRequest nodes from FHIR bundle"""
        entries = bundle.get("entry", [])
        
        with self.driver.session() as session:
            for entry in entries:
                resource = entry.get("resource", {})
                if resource.get("resourceType") != "MedicationRequest":
                    continue
                    
                med_data = {
                    "id": resource.get("id"),
                    "status": resource.get("status"),
                    "intent": resource.get("intent"),
                    "medicationCode": self._safe_get(resource, "medicationCodeableConcept", "coding", 0, "code"),
                    "medicationDisplay": self._safe_get(resource, "medicationCodeableConcept", "text"),
                    "authoredOn": resource.get("authoredOn"),
                    "patient_ref": self._safe_get(resource, "subject", "reference"),
                    "encounter_ref": self._safe_get(resource, "encounter", "reference"),
                    "requester_ref": self._safe_get(resource, "requester", "reference"),
                    "reason_ref": self._safe_get(resource, "reasonReference", 0, "reference"),
                }
                
                query = """
                MERGE (m:MedicationRequest {id: $id})
                SET m.status = $status,
                    m.intent = $intent,
                    m.medicationCode = $medicationCode,
                    m.medicationDisplay = $medicationDisplay,
                    m.authoredOn = $authoredOn,
                    m.patient_ref = $patient_ref,
                    m.encounter_ref = $encounter_ref,
                    m.requester_ref = $requester_ref,
                    m.reason_ref = $reason_ref
                """
                session.run(query, **med_data)
                self.logger.debug(f"Created MedicationRequest node: {med_data['id']}")
                
    def create_procedure_nodes(self, bundle: Dict):
        """Create Procedure nodes from FHIR bundle"""
        entries = bundle.get("entry", [])
        
        with self.driver.session() as session:
            for entry in entries:
                resource = entry.get("resource", {})
                if resource.get("resourceType") != "Procedure":
                    continue
                    
                proc_data = {
                    "id": resource.get("id"),
                    "status": resource.get("status"),
                    "code": self._safe_get(resource, "code", "coding", 0, "code"),
                    "display": self._safe_get(resource, "code", "text"),
                    "performedStart": self._safe_get(resource, "performedPeriod", "start"),
                    "performedEnd": self._safe_get(resource, "performedPeriod", "end"),
                    "patient_ref": self._safe_get(resource, "subject", "reference"),
                    "encounter_ref": self._safe_get(resource, "encounter", "reference"),
                    "reason_ref": self._safe_get(resource, "reasonReference", 0, "reference"),
                }
                
                query = """
                MERGE (pr:Procedure {id: $id})
                SET pr.status = $status,
                    pr.code = $code,
                    pr.display = $display,
                    pr.performedStart = $performedStart,
                    pr.performedEnd = $performedEnd,
                    pr.patient_ref = $patient_ref,
                    pr.encounter_ref = $encounter_ref,
                    pr.reason_ref = $reason_ref
                """
                session.run(query, **proc_data)
                self.logger.debug(f"Created Procedure node: {proc_data['id']}")
                
    def create_relationships(self):
        """Create all relationships between nodes"""
        with self.driver.session() as session:
            # Patient to Encounter
            query = """
            MATCH (p:Patient), (e:Encounter)
            WHERE e.patient_ref CONTAINS p.id
            MERGE (p)-[:HASENCOUNTER]->(e)
            """
            result = session.run(query)
            self.logger.info("Created Patient-Encounter relationships")
            
            # Encounter to Observation
            query = """
            MATCH (e:Encounter), (o:Observation)
            WHERE o.encounter_ref CONTAINS e.id
            MERGE (e)-[:HASOBSERVATION]->(o)
            """
            session.run(query)
            self.logger.info("Created Encounter-Observation relationships")
            
            # Encounter to Condition
            query = """
            MATCH (e:Encounter), (c:Condition)
            WHERE c.encounter_ref CONTAINS e.id
            MERGE (e)-[:REVEALEDCONDITION]->(c)
            """
            session.run(query)
            self.logger.info("Created Encounter-Condition relationships")
            
            # Patient to Condition
            query = """
            MATCH (p:Patient), (c:Condition)
            WHERE c.patient_ref CONTAINS p.id
            MERGE (p)-[:HASCONDITION {date: c.onsetDateTime}]->(c)
            """
            session.run(query)
            self.logger.info("Created Patient-Condition relationships")
            
            # MedicationRequest to Condition
            query = """
            MATCH (m:MedicationRequest), (c:Condition)
            WHERE m.reason_ref CONTAINS c.id
            MERGE (m)-[:TREATMENTFOR]->(c)
            """
            session.run(query)
            self.logger.info("Created MedicationRequest-Condition relationships")
            
            # Patient to MedicationRequest
            query = """
            MATCH (p:Patient), (m:MedicationRequest)
            WHERE m.patient_ref CONTAINS p.id
            MERGE (p)-[:HASMEDICATION]->(m)
            """
            session.run(query)
            self.logger.info("Created Patient-MedicationRequest relationships")
            
            # Procedure to Condition
            query = """
            MATCH (pr:Procedure), (c:Condition)
            WHERE pr.reason_ref CONTAINS c.id
            MERGE (pr)-[:PROCEDUREFORTREATMENT]->(c)
            """
            session.run(query)
            self.logger.info("Created Procedure-Condition relationships")
            
            # Procedure to Encounter
            query = """
            MATCH (e:Encounter), (pr:Procedure)
            WHERE pr.encounter_ref CONTAINS e.id
            MERGE (e)-[:HASPROCEDURE]->(pr)
            """
            session.run(query)
            self.logger.info("Created Encounter-Procedure relationships")
            
            # Patient to Procedure
            query = """
            MATCH (p:Patient), (pr:Procedure)
            WHERE pr.patient_ref CONTAINS p.id
            MERGE (p)-[:HASPROCEDURE]->(pr)
            """
            session.run(query)
            self.logger.info("Created Patient-Procedure relationships")
            
    def create_temporal_relationships(self):
        """Create temporal relationships for conditions"""
        with self.driver.session() as session:
            # First condition for each patient
            query = """
            MATCH (p:Patient)-[:HASCONDITION]->(c:Condition)
            WITH p, c
            ORDER BY c.onsetDateTime ASC
            WITH p, collect(c)[0] as firstCondition
            MERGE (p)-[:FIRSTCONDITION {date: firstCondition.onsetDateTime}]->(firstCondition)
            """
            session.run(query)
            self.logger.info("Created FIRSTCONDITION relationships")
            
            # Latest condition for each patient
            query = """
            MATCH (p:Patient)-[:HASCONDITION]->(c:Condition)
            WITH p, c
            ORDER BY c.onsetDateTime DESC
            WITH p, collect(c)[0] as latestCondition
            MERGE (p)-[:LATESTCONDITION {date: latestCondition.onsetDateTime}]->(latestCondition)
            """
            session.run(query)
            self.logger.info("Created LATESTCONDITION relationships")
            
            # Next condition temporal chain for each patient
            query = """
            MATCH (p:Patient)-[:HASCONDITION]->(c:Condition)
            WITH p, c
            ORDER BY c.onsetDateTime ASC
            WITH p, collect(c) AS conditions
            UNWIND range(0, size(conditions) - 2) AS i
            WITH conditions[i] AS current, conditions[i + 1] AS next
            MERGE (current)-[:NEXTCONDITION {date: next.onsetDateTime}]->(next)
            """
            session.run(query)
            self.logger.info("Created NEXTCONDITION relationships")
            
    def process_fhir_bundle(self, file_path: Path):
        """
        Process a single FHIR bundle JSON file
        
        Args:
            file_path: Path to the FHIR bundle JSON file
        """
        self.logger.info(f"Processing file: {file_path.name}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                bundle = json.load(f)
                
            # Create all node types
            self.create_patient_nodes(bundle)
            self.create_practitioner_nodes(bundle)
            self.create_organization_nodes(bundle)
            self.create_encounter_nodes(bundle)
            self.create_condition_nodes(bundle)
            self.create_observation_nodes(bundle)
            self.create_medication_request_nodes(bundle)
            self.create_procedure_nodes(bundle)
            
            self.logger.info(f"Successfully processed: {file_path.name}")
            
        except Exception as e:
            self.logger.error(f"Error processing {file_path.name}: {e}")
            
    def process_directory(self, directory: Path):
        """
        Process all FHIR bundle JSON files in a directory
        
        Args:
            directory: Path to directory containing FHIR bundle JSON files
        """
        json_files = list(directory.glob("*.json"))
        total_files = len(json_files)
        
        self.logger.info(f"Found {total_files} JSON files to process")
        
        for idx, file_path in enumerate(json_files, 1):
            self.logger.info(f"Processing file {idx}/{total_files}")
            self.process_fhir_bundle(file_path)
            
        self.logger.info("All files processed. Creating relationships...")
        self.create_relationships()
        
        self.logger.info("Creating temporal relationships...")
        self.create_temporal_relationships()
        
        self.logger.info("Processing complete!")
        
    def get_summary(self) -> Dict[str, int]:
        """
        Get summary statistics of the database
        
        Returns:
            Dictionary with counts of each node type
        """
        with self.driver.session() as session:
            summary = {}
            
            node_types = ["Patient", "Practitioner", "Organization", "Encounter", 
                         "Condition", "Observation", "MedicationRequest", "Procedure"]
            
            for node_type in node_types:
                result = session.run(f"MATCH (n:{node_type}) RETURN count(n) as count")
                summary[node_type] = result.single()["count"]
                
            return summary
