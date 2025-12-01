# src/config-service/main.py
import json
import os
import psycopg2
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor

app = FastAPI()

# DB Config (Match your docker-compose env)
DB_CONFIG = {
    "dbname": "fraud_detection_db",
    "user": "admin",
    "password": "password",
    "host": "localhost", # or 'postgres' if running inside docker network
    "port": "5432"
}

class SchemaField(BaseModel):
    name: str
    type: str  # "string", "integer", "float", "timestamp"
    nullable: bool = True

class Pipeline(BaseModel):
    name: str
    kafka_topic: str
    schema: list[SchemaField]

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

@app.post("/api/pipelines")
def create_pipeline(pipeline: Pipeline):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 1. Insert Pipeline
        cursor.execute(
            "INSERT INTO pipelines (name, kafka_topic, status) VALUES (%s, %s, 'active') RETURNING id",
            (pipeline.name, pipeline.kafka_topic)
        )
        pipeline_id = cursor.fetchone()[0]

        # 2. Insert Schema (Stored as JSONB)
        # We store the list of fields directly as JSON
        schema_json = json.dumps([field.model_dump() for field in pipeline.schema])
        
        cursor.execute(
            "INSERT INTO schemas (pipeline_id, schema_json) VALUES (%s, %s)",
            (pipeline_id, schema_json)
        )
        
        conn.commit()
        return {"id": pipeline_id, "message": "Pipeline created successfully"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/api/pipelines/{id}/schema")
def get_schema(id: int):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    cursor.execute("SELECT schema_json FROM schemas WHERE pipeline_id = %s", (id,))
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        raise HTTPException(status_code=404, detail="Pipeline or Schema not found")
        
    return result['schema_json']