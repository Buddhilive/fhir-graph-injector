"""
Configuration settings for FHIR Neo4j Injector
"""

# Neo4j connection settings
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"  # Change this to your actual Neo4j password

# Application settings
DATA_DIRECTORY = "data"
LOG_LEVEL = "INFO"

# Database settings
CLEAR_DATABASE_ON_START = False  # Set to True if you want to clear database before injection
CREATE_CONSTRAINTS = True