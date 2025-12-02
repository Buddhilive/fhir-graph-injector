"""
Test script for V2 implementation following the reference tutorial pattern
"""

import logging
from fhir_neo4j_injector_v2 import FHIRNeo4jInjectorV2
import config

def test_single_file_v2():
    """Test V2 injection of a single FHIR file."""
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Test file
    test_file = "data/Benton624_Tremblay80_20d0dd2a-d69c-37f1-6da1-10a5df43cbe9.json"

    try:
        with FHIRNeo4jInjectorV2(config.NEO4J_URI, config.NEO4J_USER, config.NEO4J_PASSWORD) as injector:
            # Create constraints
            if config.CREATE_CONSTRAINTS:
                injector.create_constraints()

            # Clear database if requested
            if config.CLEAR_DATABASE_ON_START:
                injector.clear_database()

            print(f"Testing V2 injection of: {test_file}")

            # Inject the single file using v2 method
            injector.inject_from_file_v2(test_file)

            # Print enhanced summary
            summary = injector.get_database_summary_v2()
            print("\n=== Database Summary (V2 Implementation) ===")
            print("Nodes:")
            total_nodes = 0
            for label, count in summary["nodes"].items():
                print(f"  {label}: {count}")
                total_nodes += count
            print(f"  Total Nodes: {total_nodes}")

            print("\nRelationships:")
            total_relationships = 0
            for rel_type, count in summary["relationships"].items():
                if count > 0:  # Only show relationships that exist
                    print(f"  {rel_type}: {count}")
                    total_relationships += count
            print(f"  Total Relationships: {total_relationships}")

            print(f"\nSuccessfully injected {test_file} into Neo4j using V2 implementation!")
            print("Graph follows the reference tutorial pattern with enhanced relationship modeling.")

    except Exception as e:
        print(f"Error: {e}")
        logging.error(f"Test failed: {e}")

if __name__ == "__main__":
    test_single_file_v2()