"""
Neo4j Database Service Layer
Handles all database operations for patient data retrieval
"""

import logging
from typing import List, Optional, Dict, Any
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import config


logger = logging.getLogger(__name__)


class Neo4jService:
    """Service class for Neo4j database operations"""
    
    def __init__(self, uri: str = None, user: str = None, password: str = None):
        """
        Initialize Neo4j service
        
        Args:
            uri: Neo4j connection URI (defaults to config.NEO4J_URI)
            user: Neo4j username (defaults to config.NEO4J_USER)
            password: Neo4j password (defaults to config.NEO4J_PASSWORD)
        """
        self.uri = uri or config.NEO4J_URI
        self.user = user or config.NEO4J_USER
        self.password = password or config.NEO4J_PASSWORD
        self.driver = None
        
    def connect(self):
        """Establish connection to Neo4j database"""
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
            logger.info("Successfully connected to Neo4j database")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise
            
    def close(self):
        """Close the Neo4j driver connection"""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")
            
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        
    def get_all_patients(self, limit: int = 100, skip: int = 0) -> List[Dict[str, Any]]:
        """
        Retrieve all patients with basic information
        
        Args:
            limit: Maximum number of patients to return
            skip: Number of patients to skip (for pagination)
            
        Returns:
            List of patient dictionaries with basic info
        """
        query = """
        MATCH (p:Patient)
        RETURN p.id as id, 
               p.fname as fname, 
               p.lname as lname, 
               p.gender as gender, 
               p.birthDate as birthDate
        ORDER BY p.lname, p.fname
        SKIP $skip
        LIMIT $limit
        """
        
        with self.driver.session() as session:
            result = session.run(query, skip=skip, limit=limit)
            patients = [dict(record) for record in result]
            logger.info(f"Retrieved {len(patients)} patients")
            return patients
            
    def get_patient_by_mrn(self, mrn: str) -> Optional[Dict[str, Any]]:
        """
        Get complete patient details by Medical Record Number (MRN/ID)
        
        Args:
            mrn: Medical Record Number (patient ID)
            
        Returns:
            Patient details dictionary or None if not found
        """
        query = """
        MATCH (p:Patient {id: $mrn})
        RETURN p.id as id,
               p.fname as fname,
               p.lname as lname,
               p.gender as gender,
               p.birthDate as birthDate,
               p.city as city,
               p.state as state,
               p.postalCode as postalCode,
               p.country as country,
               p.addressLine as addressLine,
               p.maritalStatus as maritalStatus,
               p.race as race,
               p.ethnicity as ethnicity
        """
        
        with self.driver.session() as session:
            result = session.run(query, mrn=mrn)
            record = result.single()
            
            if record:
                patient = dict(record)
                logger.info(f"Retrieved patient details for MRN: {mrn}")
                return patient
            else:
                logger.warning(f"Patient not found with MRN: {mrn}")
                return None
                
    def get_patient_encounters(self, mrn: str) -> Dict[str, Any]:
        """
        Get patient encounter/visit history by MRN
        
        Args:
            mrn: Medical Record Number (patient ID)
            
        Returns:
            Dictionary with patient info and list of encounters
        """
        # First, get patient basic info
        patient_query = """
        MATCH (p:Patient {id: $mrn})
        RETURN p.fname as fname, p.lname as lname
        """
        
        # Then get encounters
        encounters_query = """
        MATCH (p:Patient {id: $mrn})-[:HASENCOUNTER]->(e:Encounter)
        RETURN e.id as id,
               e.status as status,
               e.class as class,
               e.type as type,
               e.encstart as encstart,
               e.encend as encend
        ORDER BY e.encstart DESC
        """
        
        with self.driver.session() as session:
            # Get patient info
            patient_result = session.run(patient_query, mrn=mrn)
            patient_record = patient_result.single()
            
            if not patient_record:
                logger.warning(f"Patient not found with MRN: {mrn}")
                return None
                
            patient_name = f"{patient_record['fname']} {patient_record['lname']}" if patient_record['fname'] and patient_record['lname'] else None
            
            # Get encounters
            encounters_result = session.run(encounters_query, mrn=mrn)
            encounters = [dict(record) for record in encounters_result]
            
            logger.info(f"Retrieved {len(encounters)} encounters for patient MRN: {mrn}")
            
            return {
                "patient_id": mrn,
                "patient_name": patient_name,
                "total_encounters": len(encounters),
                "encounters": encounters
            }
            
    def health_check(self) -> bool:
        """
        Check if database connection is healthy
        
        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            with self.driver.session() as session:
                result = session.run("RETURN 1")
                result.single()
                return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False


# Global service instance
db_service = Neo4jService()
