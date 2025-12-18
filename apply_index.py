from db_config import get_engine
from sqlalchemy import text

SQL = """
ALTER TABLE `observations`
  ADD INDEX `idx_obs_mia` (`maladie`(100), `indicateur`(100), `annee`);
"""

engine = get_engine()
print("Connexion à :", engine.url)
try:
    with engine.begin() as conn:
        conn.execute(text(SQL))
    print("Index créé avec succès (idx_obs_mia).")
except Exception as e:
    print("Erreur lors de la création de l'index :", e)
    # Afficher SHOW INDEX pour vérifier l'état
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SHOW INDEX FROM `observations`"))
            rows = res.fetchall()
            print("Indexes existants:")
            for r in rows:
                print(r)
    except Exception as e2:
        print("Impossible de lister les index:", e2)
