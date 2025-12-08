"""
Main entry point for FHIR Graph Injector
Processes all FHIR bundle JSON files from the data directory and builds a Neo4j graph
"""

import logging
from pathlib import Path
from fhir_graph_builder import FHIRGraphBuilder
import config


def setup_logging():
    """Configure logging"""
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def main():
    """Main execution function"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("FHIR Graph Injector - Starting")
    logger.info("=" * 60)
    
    # Initialize the graph builder
    with FHIRGraphBuilder(config.NEO4J_URI, config.NEO4J_USER, config.NEO4J_PASSWORD) as builder:
        # Create constraints
        logger.info("Creating database constraints...")
        builder.create_constraints()
        
        # Optional: Clear existing data (uncomment if needed)
        # logger.info("Clearing existing database...")
        # builder.clear_database()
        
        # Process all files in the data directory
        data_dir = Path(config.DATA_DIR)
        if not data_dir.exists():
            logger.error(f"Data directory not found: {data_dir}")
            return
            
        builder.process_directory(data_dir)
        
        # Display summary
        logger.info("=" * 60)
        logger.info("Database Summary:")
        logger.info("=" * 60)
        summary = builder.get_summary()
        for node_type, count in summary.items():
            logger.info(f"{node_type}: {count}")
        logger.info("=" * 60)
        logger.info("FHIR Graph Injector - Complete!")
        logger.info("=" * 60)


if __name__ == "__main__":
    main()
