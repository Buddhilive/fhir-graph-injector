"""
Test script to inject a single FHIR file into Neo4j
"""

import logging
from fhir_neo4j_injector import FHIRNeo4jInjector
import config

def test_single_file():
    """Test injection of a single FHIR file."""
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Test file
    test_file = "data/Benton624_Tremblay80_20d0dd2a-d69c-37f1-6da1-10a5df43cbe9.json"

    try:
        with FHIRNeo4jInjector(config.NEO4J_URI, config.NEO4J_USER, config.NEO4J_PASSWORD) as injector:
            # Create constraints
            if config.CREATE_CONSTRAINTS:
                injector.create_constraints()

            # Clear database if requested
            if config.CLEAR_DATABASE_ON_START:
                injector.clear_database()

            print(f"Testing injection of: {test_file}")

            # Inject the single file
            injector.inject_from_file(test_file)

            # Print summary
            summary = injector.get_database_summary()
            print("\n=== Database Summary ===")
            print("Nodes:")
            for label, count in summary["nodes"].items():
                print(f"  {label}: {count}")
            print("\nRelationships:")
            for rel_type, count in summary["relationships"].items():
                print(f"  {rel_type}: {count}")

            print(f"\nSuccessfully injected {test_file} into Neo4j!")

    except Exception as e:
        print(f"Error: {e}")
        logging.error(f"Test failed: {e}")

if __name__ == "__main__":
    test_single_file()