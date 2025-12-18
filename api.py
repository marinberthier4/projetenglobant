"""
API minimale pour exposer les observations stockées en base MySQL.
Endpoints:
  - GET /observations  (filtres: maladie, indicateur, annee, region, limit)
  - GET /stats         (moyenne/min/max par maladie/indicateur/annee)

Démarrage:
  & ".\.venv\Scripts\python.exe" -m uvicorn api:app --reload --port 8000

"""
from fastapi import FastAPI, Query, HTTPException
from sqlalchemy import text
import pandas as pd
from db_config import get_engine

app = FastAPI(title="Maladies API")
engine = get_engine()


@app.get("/observations")
def get_observations(
    maladie=None,
    indicateur=None,
    annee=None,
    region=None,
    limit=100
):
    clauses = []
    params = {}
    if maladie:
        clauses.append("maladie = :maladie")
        params['maladie'] = maladie
    if indicateur:
        clauses.append("indicateur = :indicateur")
        params['indicateur'] = indicateur
    if annee:
        clauses.append("annee = :annee")
        params['annee'] = annee
    if region:
        clauses.append("region = :region")
        params['region'] = region

    where = ''
    if clauses:
        where = 'WHERE ' + ' AND '.join(clauses)

    sql = f"SELECT maladie, annee, region, indicateur, valeur, unite FROM observations {where} ORDER BY annee DESC LIMIT :limit"
    params['limit'] = limit

    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), params)
            rows = result.fetchall()
            df = pd.DataFrame(rows, columns=result.keys())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return df.to_dict(orient='records')


@app.get("/stats")
def get_stats(maladie=None, indicateur=None, annee=None):
    clauses = []
    params = {}
    if maladie:
        clauses.append("maladie = :maladie")
        params['maladie'] = maladie
    if indicateur:
        clauses.append("indicateur = :indicateur")
        params['indicateur'] = indicateur
    if annee:
        clauses.append("annee = :annee")
        params['annee'] = annee

    where = ''
    if clauses:
        where = 'WHERE ' + ' AND '.join(clauses)

    sql = f"SELECT indicateur, annee, AVG(valeur) AS moyenne, MIN(valeur) AS min_val, MAX(valeur) AS max_val FROM observations {where} GROUP BY indicateur, annee"
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), params)
            rows = result.fetchall()
            df = pd.DataFrame(rows, columns=result.keys())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return df.to_dict(orient='records')
