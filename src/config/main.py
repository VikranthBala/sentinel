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

class Rule(BaseModel):
    rule_expression: str
    severity: str = "warning"
    description: str

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

@app.post("/api/pipelines/{id}/rules")
def add_rule(id: int, rule: Rule):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO rules (pipeline_id, rule_expression, severity, description) VALUES (%s, %s, %s, %s) RETURNING id",
            (id, rule.rule_expression, rule.severity, rule.description)
        )
        rule_id = cursor.fetchone()[0]
        conn.commit()
        return {"id": rule_id, "status": "Rule added"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/api/pipelines")
def list_pipelines():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        SELECT p.id, p.name, p.kafka_topic, p.status, p.created_at,
               COUNT(fa.id) as alert_count
        FROM pipelines p
        LEFT JOIN fraud_alerts fa ON fa.pipeline_id = p.id
        GROUP BY p.id
        ORDER BY p.created_at DESC
    """)
    pipelines = cursor.fetchall()
    conn.close()
    return pipelines

@app.get("/api/pipelines/{id}")
def get_pipeline(id: int):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM pipelines WHERE id = %s", (id,))
    pipeline = cursor.fetchone()
    conn.close()
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return pipeline

@app.put("/api/pipelines/{id}")
def update_pipeline(id: int, status: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE pipelines SET status = %s WHERE id = %s",
        (status, id)
    )
    conn.commit()
    conn.close()
    return {"status": "updated"}

@app.get("/api/alerts")
def list_alerts(pipeline_id: int = None, limit: int = 20):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    if pipeline_id:
        cursor.execute("""
            SELECT * FROM fraud_alerts 
            WHERE pipeline_id = %s 
            ORDER BY alert_timestamp DESC 
            LIMIT %s
        """, (pipeline_id, limit))
    else:
        cursor.execute("""
            SELECT * FROM fraud_alerts 
            ORDER BY alert_timestamp DESC 
            LIMIT %s
        """, (limit,))
    
    alerts = cursor.fetchall()
    conn.close()
    return alerts

@app.get("/api/rules/{pipeline_id}")
def get_rules(pipeline_id: int):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        "SELECT * FROM rules WHERE pipeline_id = %s",
        (pipeline_id,)
    )
    rules = cursor.fetchall()
    conn.close()
    return rules

@app.delete("/api/rules/{rule_id}")
def delete_rule(rule_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM rules WHERE id = %s", (rule_id,))
    conn.commit()
    conn.close()
    return {"status": "deleted"}