CREATE CONSTRAINT pid
FOR (p:Patient)
REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT prid
FOR (p:Practitioner)
REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT orgid
FOR (o:Organization)
REQUIRE o.id IS UNIQUE;
CREATE CONSTRAINT encid
FOR (e:Encounter)
REQUIRE e.id_name IS UNIQUE;
CREATE CONSTRAINT cond
FOR (c:Condition)
REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT obs
FOR (o:Observation)
REQUIRE o.id IS UNIQUE;
CREATE CONSTRAINT mr
FOR (m:MedicationRequest)
REQUIRE m.id IS UNIQUE;
CREATE CONSTRAINT pr
FOR (pr:Procedure)
REQUIRE pr.id IS UNIQUE;

// Patient Node

CALL
  apoc.load.json(
    "fhir/Clara183_Carbajal274_c698c2cc-6766-c4dd-f15a-e4b8e023c660.json"
  )
  YIELD value
UNWIND value.entry AS ent
WITH ent
WHERE ent.resource.resourceType = "Patient"
CREATE
  (p:Patient
    {
      lname: ent.resource.name[0].family,
      fname: ent.resource.name[0].given[0],
      sex: ent.resource.gender,
      birthDate: ent.resource.birthDate,
      state: ent.resource.address[0].state,
      city: ent.resource.address[0].city,
      zip: ent.resource.address[0].postalCode,
      id: ent.resource.id
    })

// Practitioner Node

CALL
  apoc.load.json(
    "fhir/Clara183_Carbajal274_c698c2cc-6766-c4dd-f15a-e4b8e023c660.json"
  )
  YIELD value
UNWIND value.entry AS ent
WITH ent
WHERE ent.resource.resourceType = "Practitioner"
CREATE
  (prov:Practitioner
    {
      fname: ent.resource.name[0].given[0],
      name: ent.resource.name[0].family,
      gender: ent.resource.gender,
      id: ent.resource.id
    })

// Organization Node

CALL
  apoc.load.json(
    "fhir/Clara183_Carbajal274_c698c2cc-6766-c4dd-f15a-e4b8e023c660.json"
  )
  YIELD value
UNWIND value.entry AS ent
WITH ent
WHERE ent.resource.resourceType = "Organization"
CREATE
  (org:Organization
    {
      name: ent.resource.name,
      orgtype: ent.resource.type[0].coding[0].display,
      id: ent.resource.id,
      addressCity: ent.resource.address[0].city,
      addressState: ent.resource.address[0].state,
      addressLine: ent.resource.address[0].line[0]
    })

// Encounter Node

CALL
  apoc.load.json(
    "fhir/Clara183_Carbajal274_c698c2cc-6766-c4dd-f15a-e4b8e023c660.json"
  )
  YIELD value
UNWIND value.entry AS ent
WITH ent
WHERE ent.resource.resourceType = "Encounter"
CREATE
  (enc:Encounter
    {
      type: ent.resource.class.code,
      id: ent.resource.id,
      provider: ent.resource.participant[0].individual.reference,
      encstart: ent.resource.period.start,
      encend: ent.resource.period.end,
      id: ent.resource.id,
      pid: ent.resource.subject.reference,
      status: ent.resource.status,
      orgid: ent.resource.serviceProvider.reference
    })

// Condition

CALL
  apoc.load.json(
    "fhir/Clara183_Carbajal274_c698c2cc-6766-c4dd-f15a-e4b8e023c660.json"
  )
  YIELD value
UNWIND value.entry AS ent
WITH ent
WHERE ent.resource.resourceType = "Condition"
CREATE
  (c:Condition
    {
      type: ent.resource.resourceType,
      id: ent.resource.id,
      clinicalstatus: ent.resource.clinicalstatus.coding[0].code,
      verificationstatus: ent.resource.verificationStatus.coding[0].code,
      conditioncode: ent.resource.code.text,
      pid: ent.resource.subject.reference,
      encref: ent.resource.encounter.reference,
      onsetdate: ent.resource.onsetDateTime,
      recordeddata: ent.resource.recordedDate
    })

// Observation

CALL
  apoc.load.json(
    "fhir/Clara183_Carbajal274_c698c2cc-6766-c4dd-f15a-e4b8e023c660.json"
  )
  YIELD value
UNWIND value.entry AS ent
WITH ent
WHERE ent.resource.resourceType = "Observation"
CREATE
  (obs:Observation
    {
      id: ent.resource.id,
      obscategory: ent.resource.category[0].coding[0].display,
      obstext: ent.resource.code.text,
      encid: ent.resource.encounter.reference,
      obstimedate: ent.resource.effectiveDateTime,
      obsvalue: ent.resource.valueQuantity.value,
      obsunit: ent.resource.valueQuantity.unit,
      obscode: ent.resource.valueQuantity.code,
      pid: ent.resource.subject.reference
    })

// MedicationRequest

CALL
  apoc.load.json(
    "fhir/Clara183_Carbajal274_c698c2cc-6766-c4dd-f15a-e4b8e023c660.json"
  )
  YIELD value
UNWIND value.entry AS ent
WITH ent
WHERE ent.resource.resourceType = "MedicationRequest"
CREATE
  (medrequest:MedicationRequest
    {
      id: ent.resource.id,
      status: ent.resource.status,
      intent: ent.resource.intent,
      codingdisplaytext: ent.resource.medicationCodeableConcept.text,
      codingdisplaycode: ent.resource.medicationCodeableConcept.coding[0].code,
      pid: ent.resource.subject.reference,
      encid: ent.resource.encounter.reference,
      reasonid: ent.resource.reasonReference[0].reference,
      requesterid: ent.resource.requester.reference,
      authoredon: ent.resource.authoredOn
    })

// Procedure

CALL
  apoc.load.json(
    "fhir/Clara183_Carbajal274_c698c2cc-6766-c4dd-f15a-e4b8e023c660.json"
  )
  YIELD value
UNWIND value.entry AS ent
WITH ent
WHERE ent.resource.resourceType = "Procedure"
CREATE
  (pr:Procedure
    {
      id: ent.resource.id,
      status: ent.resource.status,
      intent: ent.resource.intent,
      codingdisplaytext: ent.resource.code.text,
      pid: ent.resource.subject.reference,
      encid: ent.resource.encounter.reference,
      reasonid: ent.resource.reasonReference[0].reference,
      startdate: ent.resource.performedPeriod.start,
      enddate: ent.resource.performedPeriod.stop
    })

// Build relationships

// Creating relationship patient to encounter

MATCH (p:Patient), (e:Encounter)
WHERE e.pid CONTAINS p.id
CREATE (p)-[r:HASENCOUNTER]->(e)
RETURN type(r)

// Encounter to Observation

MATCH (o:Observation), (e:Encounter)
WHERE o.encid CONTAINS e.id
CREATE (e)-[r:HASOBSERVATION]->(o)
RETURN type(r)

// Condition to Encounter

MATCH (c:Condition), (e:Encounter)
WHERE c.encref CONTAINS e.id
CREATE (e)-[r:REVEALEDCONDITION]->(c)
RETURN type(r)

// Condition to Patient

MATCH (c:Condition), (p:Patient)
WHERE c.pid CONTAINS p.id
CREATE (p)-[r:CONDITION {date: c.onsetdate}]->(c)
RETURN type(r)

// Temporal relationship of Conditions

MATCH (c:Condition)
WITH c
ORDER BY c.onsetdate ASC
LIMIT 50
WITH collect(c) AS conditions
FOREACH (i IN range(0, size(conditions) - 2) |
  FOREACH (node1 IN [conditions[i]] |
    FOREACH (node2 IN [conditions[i + 1]] |
      CREATE (node1)-[:NEXTCONDITION {date: node2.onsetdate}]->(node2)
    )
  )
)

MATCH (c:Condition), (p:Patient)
WHERE c.pid CONTAINS p.id
WITH c, p
ORDER BY c.onsetdate ASC
LIMIT 1
CREATE (p)-[r:FIRSTCONDITION {date: c.onsetdate}]->(c)
RETURN type(r)

MATCH (c:Condition), (p:Patient)
WHERE c.pid CONTAINS p.id
WITH c, p
ORDER BY c.onsetdate DESC
LIMIT 1
CREATE (p)-[r:LATESTCONDITION {date: c.onsetdate}]->(c)
RETURN type(r)

// medicationrequest and condition

MATCH (c:Condition), (mr:MedicationRequest)
WHERE mr.reasonid CONTAINS c.id
CREATE (mr)-[r:TREATMENTFOR]->(c)
RETURN type(r)

// Procedure and Condition
// Procedure and Encounter

MATCH (c:Condition), (pr:Procedure)
WHERE pr.reasonid CONTAINS c.id
CREATE (pr)-[r:PROCEDUREFORTREATMENT]->(c)
RETURN type(r)

MATCH (e:Encounter), (pr:Procedure)
WHERE pr.encid CONTAINS e.id
CREATE (pr)-[r:PROCEDUREINENCOUNTER]->(c)
RETURN type(r)