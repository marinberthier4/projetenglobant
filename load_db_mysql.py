"""
Script pour charger `donnees_sante/maladies_combine.csv` dans une base MySQL (XAMPP)
Usage:
  - Configurez vos identifiants MySQL via variables d'environnement:
      DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME
  - Puis exécutez:
      & ".\.venv\Scripts\python.exe" load_db_mysql.py

Remarques:
  - Crée/écrase la table `observations`.
  - Crée un index sur (maladie, indicateur, annee, region) si possible.
"""
import os
import sys
import pandas as pd
from sqlalchemy import text
from db_config import get_engine

CSV_PATH = os.path.join('donnees_sante', 'maladies_combine.csv')
TABLE_NAME = 'observations'


def main():
    if not os.path.exists(CSV_PATH):
        print(f"Erreur : fichier CSV introuvable : {CSV_PATH}")
        sys.exit(1)

    print(f"Lecture du CSV : {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, encoding='utf-8-sig')

    # Vérifier colonnes attendues
    expected = ['maladie', 'annee', 'region', 'indicateur', 'valeur']
    missing = [c for c in expected if c not in df.columns]
    if missing:
        print(f"Colonnes manquantes dans le CSV : {missing}")
        sys.exit(1)

    # Si colonne 'unite' absente, la créer avec valeurs par indicateur
    if 'unite' not in df.columns:
        print("Colonne 'unite' manquante — création automatique selon indicateur")
        df['unite'] = df['indicateur'].map({
            'prevalence': '%',
            'incidence': 'pour 100 000 hab',
            'mortalite': 'pour 100 000 hab'
        }).fillna('')

    # Nettoyage minimum
    df = df[['maladie', 'annee', 'region', 'indicateur', 'valeur', 'unite']]

    engine = get_engine()

    print(f"Connexion à la base : {engine.url}")
    print(f"Écriture dans la table `{TABLE_NAME}` (if_exists='replace')...")

    # to_sql peut être lent ; pour des fichiers modestes c'est acceptable
    df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)

    # Créer index (essaie, ignore erreur si existe)
    idx_sql = f"ALTER TABLE `{TABLE_NAME}` ADD INDEX idx_obs_miay (maladie, indicateur, annee, region)"
    try:
        with engine.begin() as conn:
            conn.execute(text(idx_sql))
            print("Index `idx_obs_miay` créé.")
    except Exception as e:
        print(f"Remarque : création de l'index a échoué (peut-être déjà existant): {e}")

    print("Import terminé.")


if __name__ == '__main__':
    main()
