import urllib.request
import json
import pandas as pd
import os

from visualisation import (
    creer_graphique_evolution_temporelle,
    creer_graphique_barres_regions,
    creer_graphique_comparaison_maladies,
    creer_heatmap_region_annee,
    creer_carte_france,
    calculer_statistiques,
)


def load_data():
    api_url = os.environ.get("API_URL", "http://127.0.0.1:8000/observations?limit=1000000")
    try:
        with urllib.request.urlopen(api_url, timeout=5) as resp:
            data = json.load(resp)
            df = pd.DataFrame(data)
            if not df.empty:
                df['annee'] = pd.to_numeric(df['annee'], errors='coerce')
                df['valeur'] = pd.to_numeric(df['valeur'], errors='coerce')
                df = df.dropna(subset=['maladie', 'annee', 'region', 'indicateur', 'valeur'])
                print(f"Loaded {len(df)} rows from API")
                return df
    except Exception as e:
        print(f"API load failed: {e}")

    # Fallback to CSV
    possible = [
        "donnees_sante/maladies_combine.csv",
        "donnees_sante/maladies_clean.csv",
    ]
    for p in possible:
        if os.path.exists(p):
            df = pd.read_csv(p, encoding='utf-8-sig')
            df['annee'] = pd.to_numeric(df['annee'], errors='coerce')
            df['valeur'] = pd.to_numeric(df['valeur'], errors='coerce')
            df = df.dropna(subset=['maladie', 'annee', 'region', 'indicateur', 'valeur'])
            print(f"Loaded {len(df)} rows from {p}")
            return df

    raise RuntimeError("No data available")


def main():
    df = load_data()

    # pick samples
    maladie = df['maladie'].unique()[0]
    indicateur = df['indicateur'].unique()[0]
    annee = int(df['annee'].dropna().unique()[0])

    print(f"Sample selection: maladie={maladie}, indicateur={indicateur}, annee={annee}")

    funcs = [
        (creer_graphique_evolution_temporelle, (df, maladie, indicateur)),
        (creer_graphique_barres_regions, (df, maladie, indicateur, annee)),
        (creer_graphique_comparaison_maladies, (df, indicateur, annee)),
        (creer_heatmap_region_annee, (df, maladie, indicateur)),
        (creer_carte_france, (df, maladie, indicateur, annee)),
    ]

    for fn, args in funcs:
        try:
            fig = fn(*args)
            ok = fig is not None
            t = type(fig)
            print(f"{fn.__name__}: returned {t}, ok={ok}")
        except Exception as e:
            print(f"{fn.__name__}: raised exception: {e}")

    # Stats
    try:
        stats = calculer_statistiques(df, maladie, indicateur, annee)
        print(f"calculer_statistiques: {stats}")
    except Exception as e:
        print(f"calculer_statistiques: raised {e}")


if __name__ == '__main__':
    main()
