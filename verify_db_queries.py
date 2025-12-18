from db_config import get_engine
import pandas as pd

engine = get_engine()
print('Connected to', engine.url)

sql_obs = "SELECT maladie, annee, region, indicateur, valeur, unite FROM observations WHERE maladie='cancer' AND indicateur='prevalence' AND annee=2018 LIMIT 10"
sql_stats = "SELECT indicateur, annee, AVG(valeur) AS moyenne, MIN(valeur) AS min_val, MAX(valeur) AS max_val FROM observations WHERE maladie='cancer' AND indicateur='prevalence' GROUP BY indicateur, annee"

try:
    df_obs = pd.read_sql_query(sql_obs, engine)
    print('\nOBSERVATIONS (sample):')
    print(df_obs.to_string(index=False))
except Exception as e:
    print('Erreur lecture observations:', e)

try:
    df_stats = pd.read_sql_query(sql_stats, engine)
    print('\nSTATS (par année):')
    print(df_stats.to_string(index=False))
except Exception as e:
    print('Erreur lecture stats:', e)
