"""
FastAPI Application for FHIR Patient Data API
Provides endpoints to retrieve patient data from Neo4j graph database
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import logging

from api.models import (
    PatientSummary, 
    PatientDetails, 
    PatientEncountersResponse,
    ErrorResponse
)
from api.database import db_service


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Create FastAPI app
app = FastAPI(
    title="FHIR Patient Data API",
    description="API for retrieving patient data from Neo4j graph database",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)


# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    """Initialize database connection on startup"""
    try:
        db_service.connect()
        logger.info("FastAPI application started successfully")
    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Close database connection on shutdown"""
    db_service.close()
    logger.info("FastAPI application shut down")


@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with API information"""
    return {
        "message": "FHIR Patient Data API",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": {
            "patients": "/patients",
            "patient_details": "/patients/{mrn}",
            "patient_encounters": "/patients/{mrn}/encounters"
        }
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint"""
    db_healthy = db_service.health_check()
    
    if db_healthy:
        return {
            "status": "healthy",
            "database": "connected"
        }
    else:
        raise HTTPException(
            status_code=503,
            detail="Database connection unhealthy"
        )


@app.get(
    "/patients",
    response_model=List[PatientSummary],
    tags=["Patients"],
    summary="Fetch all patients",
    description="Retrieve a list of all patients with basic information (MRN, name, gender, DOB)"
)
async def get_all_patients(
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of patients to return"),
    skip: int = Query(0, ge=0, description="Number of patients to skip (for pagination)")
):
    """
    Fetch all patients with basic information
    
    - **limit**: Maximum number of patients to return (default: 100, max: 1000)
    - **skip**: Number of patients to skip for pagination (default: 0)
    """
    try:
        patients = db_service.get_all_patients(limit=limit, skip=skip)
        logger.info(f"Retrieved {len(patients)} patients (limit={limit}, skip={skip})")
        return patients
    except Exception as e:
        logger.error(f"Error retrieving patients: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve patients: {str(e)}"
        )


@app.get(
    "/patients/{mrn}",
    response_model=PatientDetails,
    tags=["Patients"],
    summary="Get patient details by MRN",
    description="Retrieve complete patient demographics and details by Medical Record Number (MRN/ID)",
    responses={
        200: {"description": "Patient details retrieved successfully"},
        404: {"model": ErrorResponse, "description": "Patient not found"}
    }
)
async def get_patient_details(mrn: str):
    """
    Get complete patient details by Medical Record Number (MRN)
    
    - **mrn**: Medical Record Number (patient ID)
    
    Returns patient demographics including:
    - Name, gender, date of birth
    - Address information
    - Marital status, race, ethnicity
    """
    try:
        patient = db_service.get_patient_by_mrn(mrn)
        
        if patient is None:
            raise HTTPException(
                status_code=404,
                detail=f"Patient not found with MRN: {mrn}"
            )
            
        logger.info(f"Retrieved patient details for MRN: {mrn}")
        return patient
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving patient {mrn}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve patient details: {str(e)}"
        )


@app.get(
    "/patients/{mrn}/encounters",
    response_model=PatientEncountersResponse,
    tags=["Patients"],
    summary="Get patient visit history by MRN",
    description="Retrieve patient encounter/visit history by Medical Record Number (MRN/ID)",
    responses={
        200: {"description": "Patient encounters retrieved successfully"},
        404: {"model": ErrorResponse, "description": "Patient not found"}
    }
)
async def get_patient_encounters(mrn: str):
    """
    Get patient encounter/visit history by Medical Record Number (MRN)
    
    - **mrn**: Medical Record Number (patient ID)
    
    Returns:
    - Patient basic information
    - Total number of encounters
    - List of encounters with dates, types, and status
    """
    try:
        result = db_service.get_patient_encounters(mrn)
        
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"Patient not found with MRN: {mrn}"
            )
            
        logger.info(f"Retrieved {result['total_encounters']} encounters for patient MRN: {mrn}")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving encounters for patient {mrn}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve patient encounters: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
