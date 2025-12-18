import pandas as pd
import requests
import os
import zipfile
import tempfile
import re
try:
    from bs4 import BeautifulSoup
    HAVE_BS4 = True
except Exception:
    HAVE_BS4 = False
import time
import sqlalchemy
from sqlalchemy import text
import db_config

OUTPUT_FOLDER = "donnees_sante"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

print(" Démarrage de la collecte de données santé...")
print(f" Dossier de sortie : {OUTPUT_FOLDER}\n")


def telecharger_fichier(url, nom_fichier):
    try:
        print(f"Téléchargement : {nom_fichier}...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        chemin = os.path.join(OUTPUT_FOLDER, nom_fichier)
        with open(chemin, 'wb') as f:
            f.write(response.content)
        print(f" Téléchargé : {nom_fichier}\n")
        return True
    except Exception as e:
        print(f" Erreur lors du téléchargement de {nom_fichier}: {e}\n")
        return False


def read_csv_flexible(path_or_buf):
    try:
        return pd.read_csv(path_or_buf)
    except Exception:
        for sep in [';', ',', '\t']:
            try:
                return pd.read_csv(path_or_buf, sep=sep)
            except Exception:
                continue
    raise


def read_any_file(path_or_buf):
    path = str(path_or_buf)
    lower = path.lower()
    if lower.endswith('.csv'):
        return read_csv_flexible(path)
    if lower.endswith(('.xls', '.xlsx')):
        return pd.read_excel(path, sheet_name=0)
    if lower.endswith('.zip'):
        with zipfile.ZipFile(path, 'r') as z:
            candidates = [n for n in z.namelist() if n.lower().endswith(('.csv', '.xlsx', '.xls'))]
            if not candidates:
                raise ValueError('No CSV/XLSX inside ZIP')
            first = candidates[0]
            with z.open(first) as f:
                suffix = os.path.splitext(first)[1]
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                    tmp.write(f.read())
                    tmp_path = tmp.name
            try:
                if tmp_path.lower().endswith('.csv'):
                    return read_csv_flexible(tmp_path)
                else:
                    return pd.read_excel(tmp_path, sheet_name=0)
            finally:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass
    if lower.endswith(('.html', '.htm')):
        # try tables
        try:
            tables = pd.read_html(path)
            if tables:
                return tables[0]
        except Exception:
            pass
        # try to extract links to static.data.gouv and download first csv/xlsx
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as fh:
                txt = fh.read()
        except Exception:
            raise ValueError('Unable to parse HTML and no downloadable CSV/XLSX found')
        links = []
        if HAVE_BS4:
            soup = BeautifulSoup(txt, 'html.parser')
            for a in soup.find_all('a', href=True):
                href = a['href']
                if href and ('static.data.gouv' in href or href.lower().endswith(('.csv', '.xlsx', '.xls'))):
                    links.append(href)
        else:
            links = re.findall(r'href=["\']([^"\']+\.(?:csv|xlsx|xls))["\']', txt, flags=re.I)

        for link in links:
            if link.startswith('/'):
                link = 'https://www.data.gouv.fr' + link
            if not link.startswith('http'):
                continue
            try:
                r = requests.get(link, timeout=30)
                r.raise_for_status()
                suffix = os.path.splitext(link)[1]
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                    tmp.write(r.content)
                    tmp_path = tmp.name
                try:
                    if tmp_path.lower().endswith('.csv'):
                        return read_csv_flexible(tmp_path)
                    else:
                        return pd.read_excel(tmp_path, sheet_name=0)
                finally:
                    try:
                        os.unlink(tmp_path)
                    except Exception:
                        pass
            except Exception:
                continue
        raise ValueError('Unable to parse HTML and no downloadable CSV/XLSX found')


def normalize_downloaded_dataframe(df, source_name=None):
    cols = {c.lower().strip(): c for c in df.columns}
    def find_col(possible):
        for p in possible:
            if p in cols:
                return cols[p]
        return None

    col_annee = find_col(['annee', 'année', 'year', 'an'])
    col_region = find_col(['region', 'région', 'departement', 'dept', 'region_name', 'libelle', 'code_reg', 'reg'])
    col_indicateur = find_col(['indicateur', 'indicator', 'type', 'measure', 'mesure'])
    col_valeur = find_col(['valeur', 'value', 'val', 'nombre', 'count', 'valeur_tot'])
    col_unite = find_col(['unite', 'unité', 'unit'])
    col_maladie = find_col(['maladie', 'disease', 'diagnosis', 'pathology'])

    # If we find a long-format table with the required columns, build output
    if col_valeur is not None and col_annee is not None and col_region is not None:
        out = pd.DataFrame()
        out['annee'] = pd.to_numeric(df[col_annee], errors='coerce').astype('Int64')
        out['region'] = df[col_region].astype(str).str.strip()
        out['indicateur'] = df[col_indicateur] if col_indicateur in df.columns else 'valeur'
        out['valeur'] = pd.to_numeric(df[col_valeur], errors='coerce')
        out['unite'] = df[col_unite] if col_unite in df.columns else ''
        if col_maladie in df.columns:
            out['maladie'] = df[col_maladie].astype(str)
        else:
            out['maladie'] = source_name or 'inconnue'
        out = out.dropna(subset=['annee', 'region', 'valeur'])
        out['indicateur'] = out['indicateur'].astype(str)
        out['unite'] = out['unite'].astype(str)
        out['maladie'] = out['maladie'].astype(str)
        return out[['maladie', 'annee', 'region', 'indicateur', 'valeur', 'unite']]

    # Fallback: wide tables where years are columns (e.g., columns '2018','2019')
    cols_stripped = [str(c).strip() for c in df.columns]
    year_cols = [c for c in cols_stripped if c.isdigit() and len(c) == 4]
    if year_cols and col_region:
        try:
            # map back to original column names for exact selection
            year_cols_exact = [c for c in df.columns if str(c).strip() in year_cols]
            id_vars = [col_region]
            if col_indicateur in df.columns:
                id_vars.append(col_indicateur)
            melted = df.melt(id_vars=id_vars, value_vars=year_cols_exact, var_name='annee', value_name='valeur')
            melted['annee'] = pd.to_numeric(melted['annee'], errors='coerce').astype('Int64')
            melted['region'] = melted[col_region].astype(str).str.strip()
            melted['indicateur'] = melted[col_indicateur] if col_indicateur in df.columns else 'valeur'
            melted['unite'] = df[col_unite].iloc[0] if col_unite in df.columns else ''
            melted['maladie'] = source_name or 'inconnue'
            out = melted[['maladie', 'annee', 'region', 'indicateur', 'valeur', 'unite']]
            out['valeur'] = pd.to_numeric(out['valeur'], errors='coerce')
            out = out.dropna(subset=['annee', 'region', 'valeur'])
            return out
        except Exception:
            pass

    print(f"  Normalisation impossible pour {source_name} — colonnes requises manquantes ({col_valeur=}, {col_annee=}, {col_region=})")
    return pd.DataFrame()


def process_inca_xlsx(path, source_name=None):
    """Try to read all sheets in an INCa xlsx and extract any tables that match
    the canonical schema. Returns a concatenated DataFrame or empty DF."""
    try:
        sheets = pd.read_excel(path, sheet_name=None)
    except Exception as e:
        print(f" Lecture XLSX failed for {path}: {e}")
        return pd.DataFrame()

    extracted = []
    for sname, df in sheets.items():
        try:
            # simple heuristic: sheet must have at least 3 columns
            if df.shape[1] < 2:
                continue
            df_cols = [str(c).strip().lower() for c in df.columns]
            if any(k in 'annee year an' for k in ' '.join(df_cols)) or any('region' in c for c in df_cols):
                cand = normalize_downloaded_dataframe(df, source_name=source_name)
                if not cand.empty:
                    extracted.append(cand)
        except Exception:
            continue

    if extracted:
        return pd.concat(extracted, ignore_index=True)
    return pd.DataFrame()


def process_zip_and_normalize(path, source_name=None):
    """Extract CSV/XLSX files from a ZIP and attempt normalization on each."""
    try:
        with zipfile.ZipFile(path, 'r') as z:
            names = z.namelist()
            candidates = [n for n in names if n.lower().endswith(('.csv', '.xlsx', '.xls'))]
            results = []
            for n in candidates:
                try:
                    with z.open(n) as f:
                        # write to temp
                        suffix = os.path.splitext(n)[1]
                        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                            tmp.write(f.read())
                            tmp_path = tmp.name
                        try:
                            df_raw = None
                            if tmp_path.lower().endswith('.csv'):
                                df_raw = read_csv_flexible(tmp_path)
                            else:
                                df_raw = pd.read_excel(tmp_path, sheet_name=0)
                            df_norm = normalize_downloaded_dataframe(df_raw, source_name=source_name)
                            if not df_norm.empty:
                                results.append(df_norm)
                        finally:
                            try:
                                os.unlink(tmp_path)
                            except Exception:
                                pass
                except Exception:
                    continue
            if results:
                return pd.concat(results, ignore_index=True)
    except Exception as e:
        print(f" Erreur lors de l'ouverture du zip {path}: {e}")
    return pd.DataFrame()


def ensure_observations_table(engine):
    create_sql = """
    CREATE TABLE IF NOT EXISTS observations (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        maladie VARCHAR(255),
        annee INT,
        region VARCHAR(255),
        indicateur VARCHAR(255),
        valeur DOUBLE,
        unite VARCHAR(255),
        UNIQUE KEY ux_obs_unique (maladie(100), annee, region(100), indicateur(100))
    ) CHARACTER SET utf8mb4;
    """
    with db_config.get_engine().begin() as conn:
        conn.execute(text(create_sql))


def upsert_observations(engine, df):
    if df.empty:
        print("Aucun enregistrement à insérer dans la base (dataframe vide)")
        return
    temp_table = 'temp_import'
    df.to_sql(temp_table, con=engine, if_exists='replace', index=False)
    insert_sql = f"""
    INSERT INTO observations (maladie, annee, region, indicateur, valeur, unite)
    SELECT maladie, annee, region, indicateur, valeur, unite FROM {temp_table}
    ON DUPLICATE KEY UPDATE
      valeur = VALUES(valeur),
      unite = VALUES(unite)
    """
    with engine.begin() as conn:
        conn.execute(text(insert_sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
    print(f" {len(df)} lignes synchronisées dans la table `observations` (UPSERT)")


def dump_existing_for_maladie(engine, maladie):
    try:
        q = text("SELECT * FROM observations WHERE maladie = :m")
        df_old = pd.read_sql_query(q, con=engine, params={'m': maladie})
        if not df_old.empty:
            outp = os.path.join(OUTPUT_FOLDER, f'backup_observations_{maladie}.csv')
            df_old.to_csv(outp, index=False, encoding='utf-8-sig')
            print(f"Dump sauvegarde existante -> {outp} ({len(df_old)} lignes)")
        else:
            print(f"  Aucune ligne existante à sauvegarder pour '{maladie}'")
    except Exception as e:
        print(f" Erreur lors du dump de sauvegarde pour {maladie}: {e}")


DATASETS = {
    'inca_mco': {
        'url': 'https://static.data.gouv.fr/resources/donnees-dactivite-de-cancerologie-des-etablissements-de-sante-en-france/20251022-142956/inca-donnees-mco-10.2025.xlsx',
        'nom': 'inca_donnees_mco_2025.xlsx',
        'type': 'cancer'
    },
    'inca_smr': {
        'url': 'https://static.data.gouv.fr/resources/donnees-dactivite-de-cancerologie-des-etablissements-de-sante-en-france/20251022-143107/inca-donnees-smr-10.2025.xlsx',
        'nom': 'inca_donnees_smr_2025.xlsx',
        'type': 'cancer'
    },
    'inca_had': {
        'url': 'https://static.data.gouv.fr/resources/donnees-dactivite-en-lien-avec-le-cancer-dans-les-etablissements-de-sante-en-france/20251104-131101/inca-donnees-had-10.2025.xlsx',
        'nom': 'inca_donnees_had_2025.xlsx',
        'type': 'cancer'
    },
    'hdh_top_diabete': {
        'url': 'https://static.data.gouv.fr/resources/donnees-synthetiques-top-diabete/20240425-162212/snds-20240425t154700z-001.zip',
        'nom': 'top_diabete.zip',
        'type': 'diabete'
    },
    'cardio_mortalite': {
        'url': 'https://www.data.gouv.fr/fr/datasets/mortalite-due-aux-maladies-cardiovasculaires/',
        'nom': 'cardio_mortalite_page.html',
        'type': 'cardiovasculaire'
    },
    'deces_sida': {
        'url': 'https://www.data.gouv.fr/fr/datasets/deces-par-sida-vih/',
        'nom': 'deces_sida_page.html',
        'type': 'sida'
    },
    'insee_pop': {
        'url': 'https://www.insee.fr/fr/statistiques/fichier/3698339/population-legale-2019.csv',
        'nom': 'insee_population_2019.csv',
        'type': 'insee'
    }
}


def process_datasets_and_sync(to_db=True):
    engine = None
    if to_db:
        engine = db_config.get_engine()
        ensure_observations_table(engine)
    any_real = False
    for key, info in DATASETS.items():
        url = info.get('url')
        name = info.get('nom') or f"{key}.csv"
        maladie = info.get('type') or key
        print(f"  Traitement de la source {key} ({name})...")
        ok = telecharger_fichier(url, name)
        if not ok:
            print(f" Téléchargement échoué pour {key} — saut de cette source\n")
            continue
        chemin = os.path.join(OUTPUT_FOLDER, name)
        try:
            # choose special handlers by extension
            if name.lower().endswith(('.xls', '.xlsx')):
                df_norm = process_inca_xlsx(chemin, source_name=maladie)
            elif name.lower().endswith('.zip'):
                df_norm = process_zip_and_normalize(chemin, source_name=maladie)
            else:
                df_raw = read_any_file(chemin)
                df_norm = normalize_downloaded_dataframe(df_raw, source_name=maladie)
        except Exception as e:
            print(f" Impossible de lire {chemin}: {e}")
            continue
        if df_norm.empty:
            print(f" Normalisation a échoué pour {name}; saut de cette source\n")
            continue
        if to_db:
            dump_existing_for_maladie(engine, maladie)
            with engine.begin() as conn:
                print(f" Remplacement des entrées existantes pour '{maladie}' en base...")
                conn.execute(text("DELETE FROM observations WHERE maladie = :m"), {'m': maladie})
            upsert_observations(engine, df_norm)
        outname = f"{maladie}.csv"
        df_norm.to_csv(os.path.join(OUTPUT_FOLDER, outname), index=False, encoding='utf-8-sig')
        print(f" Source {key} traitée et sauvegardée -> {outname}\n")
        any_real = True
    if not any_real:
        print(" Aucune source distante valide traitée — aucun changement en base effectué.")
    return any_real


if __name__ == '__main__':
    try:
        any_real = process_datasets_and_sync(to_db=True)
    except Exception as e:
        print(f" Erreur lors du traitement des sources distantes: {e}")
        any_real = False
    if not any_real:
        print('Aucune source distante traitée — script terminé sans synchronisation.')
