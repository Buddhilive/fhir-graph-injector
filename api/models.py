"""
Pydantic models for FastAPI endpoints
Defines request and response schemas for patient data API
"""

from typing import Optional, List
from pydantic import BaseModel, Field
from datetime import date


class PatientSummary(BaseModel):
    """Brief patient information for list endpoints"""
    id: str = Field(..., description="Medical Record Number (MRN)")
    fname: Optional[str] = Field(None, description="First name")
    lname: Optional[str] = Field(None, description="Last name")
    gender: Optional[str] = Field(None, description="Gender")
    birthDate: Optional[str] = Field(None, description="Date of birth")
    
    class Config:
        json_schema_extra = {
            "example": {
                "id": "4877dc14-609c-a22b-0a94-e15707167428",
                "fname": "Abe604",
                "lname": "Sipes176",
                "gender": "male",
                "birthDate": "1970-03-12"
            }
        }


class PatientDetails(BaseModel):
    """Complete patient demographics and details"""
    id: str = Field(..., description="Medical Record Number (MRN)")
    fname: Optional[str] = Field(None, description="First name")
    lname: Optional[str] = Field(None, description="Last name")
    gender: Optional[str] = Field(None, description="Gender")
    birthDate: Optional[str] = Field(None, description="Date of birth")
    city: Optional[str] = Field(None, description="City")
    state: Optional[str] = Field(None, description="State")
    postalCode: Optional[str] = Field(None, description="Postal code")
    country: Optional[str] = Field(None, description="Country")
    addressLine: Optional[str] = Field(None, description="Street address")
    maritalStatus: Optional[str] = Field(None, description="Marital status")
    race: Optional[str] = Field(None, description="Race")
    ethnicity: Optional[str] = Field(None, description="Ethnicity")
    
    class Config:
        json_schema_extra = {
            "example": {
                "id": "4877dc14-609c-a22b-0a94-e15707167428",
                "fname": "Abe604",
                "lname": "Sipes176",
                "gender": "male",
                "birthDate": "1970-03-12",
                "city": "Worcester",
                "state": "MA",
                "postalCode": "01605",
                "country": "US",
                "addressLine": "734 Oberbrunner Parade Unit 28",
                "maritalStatus": "Never Married",
                "race": "White",
                "ethnicity": "Not Hispanic or Latino"
            }
        }


class Encounter(BaseModel):
    """Patient encounter/visit information"""
    id: str = Field(..., description="Encounter ID")
    status: Optional[str] = Field(None, description="Encounter status")
    encounterClass: Optional[str] = Field(None, alias="class", description="Encounter class (e.g., AMB for ambulatory)")
    type: Optional[str] = Field(None, description="Encounter type")
    encstart: Optional[str] = Field(None, description="Encounter start date/time")
    encend: Optional[str] = Field(None, description="Encounter end date/time")
    
    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "id": "4877dc14-609c-a22b-6df5-86312997308a",
                "status": "finished",
                "class": "AMB",
                "type": "Well child visit (procedure)",
                "encstart": "1981-03-26T16:41:18+00:00",
                "encend": "1981-03-26T16:56:18+00:00"
            }
        }


class PatientEncountersResponse(BaseModel):
    """Response model for patient encounters endpoint"""
    patient_id: str = Field(..., description="Medical Record Number (MRN)")
    patient_name: Optional[str] = Field(None, description="Patient full name")
    total_encounters: int = Field(..., description="Total number of encounters")
    encounters: List[Encounter] = Field(..., description="List of encounters")
    
    class Config:
        json_schema_extra = {
            "example": {
                "patient_id": "4877dc14-609c-a22b-0a94-e15707167428",
                "patient_name": "Abe604 Sipes176",
                "total_encounters": 2,
                "encounters": [
                    {
                        "id": "4877dc14-609c-a22b-6df5-86312997308a",
                        "status": "finished",
                        "class": "AMB",
                        "type": "Well child visit (procedure)",
                        "encstart": "1981-03-26T16:41:18+00:00",
                        "encend": "1981-03-26T16:56:18+00:00"
                    }
                ]
            }
        }


class ErrorResponse(BaseModel):
    """Error response model"""
    detail: str = Field(..., description="Error message")
    
    class Config:
        json_schema_extra = {
            "example": {
                "detail": "Patient not found"
            }
        }
