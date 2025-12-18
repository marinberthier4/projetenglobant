import pandas as pd
import numpy as np
import os
import subprocess
import sys

# ========================================
# CONFIGURATION
# ========================================

# Dossiers de travail
DOSSIER_DONNEES = "donnees_sante"
FICHIER_SORTIE = "maladies_clean.csv"

# Fichiers CSV à traiter
FICHIERS_CSV = {
    'diabete': 'diabete.csv',
    'cardio': 'cardio.csv',
    'cancer': 'cancer.csv'
}

# Colonnes attendues dans le format final
COLONNES_FINALES = ['maladie', 'annee', 'region', 'indicateur', 'valeur']

# Dictionnaire de correction des noms de régions
CORRECTIONS_REGIONS = {
    'Ile-de-France': 'Île-de-France',
    'Ile de France': 'Île-de-France',
    'IDF': 'Île-de-France',
    'PACA': 'Provence-Alpes-Côte d\'Azur',
    'Auvergne Rhône Alpes': 'Auvergne-Rhône-Alpes',
    'Auvergne Rhone Alpes': 'Auvergne-Rhône-Alpes',
    'Nouvelle Aquitaine': 'Nouvelle-Aquitaine',
    'Hauts de France': 'Hauts-de-France'
}

print("=" * 70)
print("🧹 SCRIPT DE NETTOYAGE ET AGRÉGATION DES DONNÉES SANTÉ")
print("=" * 70)
print()


# ========================================
# ÉTAPE 1 : APPELER LE SCRIPT DE SCRAPING
# ========================================

def executer_scraping():
    """
    Exécute le script de scraping pour récupérer les données

    Returns:
        bool: True si succès, False sinon
    """
    print("=" * 70)
    print("ÉTAPE 1 : RÉCUPÉRATION DES DONNÉES")
    print("=" * 70)
    print()

    # Vérifier si le script de scraping existe
    if not os.path.exists('collecte_sante.py'):
        print("⚠️  Le fichier 'collecte_sante.py' n'existe pas.")
        print("📝 Voulez-vous continuer avec les données existantes ? (o/n)")

        # Pour l'automatisation, on continue si les fichiers existent
        if os.path.exists(DOSSIER_DONNEES):
            print("✅ Dossier de données trouvé, on continue...\n")
            return True
        else:
            print("❌ Aucune donnée disponible. Veuillez d'abord exécuter collecte_sante.py")
            return False

    try:
        print("🔄 Exécution du script de collecte des données...")
        # Exécuter le script de scraping
        result = subprocess.run([sys.executable, 'collecte_sante.py'],
                                capture_output=True,
                                text=True,
                                timeout=120)

        if result.returncode == 0:
            print("✅ Collecte des données réussie !\n")
            return True
        else:
            print(f"⚠️  Avertissement lors de la collecte : {result.stderr}")
            print("On continue avec les données existantes...\n")
            return True

    except subprocess.TimeoutExpired:
        print("⚠️  Le script de collecte a pris trop de temps.")
        print("On continue avec les données existantes...\n")
        return True
    except Exception as e:
        print(f"⚠️  Erreur lors de l'exécution : {e}")
        print("On continue avec les données existantes...\n")
        return True


# ========================================
# ÉTAPE 2 : CHARGEMENT DES DONNÉES
# ========================================

def charger_csv(nom_fichier):
    """
    Charge un fichier CSV depuis le dossier de données

    Args:
        nom_fichier (str): Nom du fichier CSV à charger

    Returns:
        DataFrame or None: Le dataframe chargé ou None si erreur
    """
    chemin = os.path.join(DOSSIER_DONNEES, nom_fichier)

    try:
        # Charger le CSV avec gestion de l'encodage
        df = pd.read_csv(chemin, encoding='utf-8-sig')
        print(f"✅ Chargé : {nom_fichier} ({len(df)} lignes)")
        return df
    except FileNotFoundError:
        print(f"❌ Fichier introuvable : {nom_fichier}")
        return None
    except Exception as e:
        print(f"❌ Erreur lors du chargement de {nom_fichier}: {e}")
        return None


def charger_tous_les_fichiers():
    """
    Charge tous les fichiers CSV nécessaires

    Returns:
        dict: Dictionnaire {nom_maladie: dataframe}
    """
    print("=" * 70)
    print("ÉTAPE 2 : CHARGEMENT DES FICHIERS CSV")
    print("=" * 70)
    print()

    dataframes = {}

    for nom_maladie, nom_fichier in FICHIERS_CSV.items():
        df = charger_csv(nom_fichier)
        if df is not None:
            dataframes[nom_maladie] = df

    print(f"\n📊 Total : {len(dataframes)} fichiers chargés avec succès\n")
    return dataframes


# ========================================
# ÉTAPE 3 : NETTOYAGE DES DONNÉES
# ========================================

def standardiser_colonnes(df):
    """
    Standardise les noms de colonnes (minuscules, sans espaces)

    Args:
        df (DataFrame): Le dataframe à standardiser

    Returns:
        DataFrame: Le dataframe avec colonnes standardisées
    """
    # Mettre en minuscules et supprimer les espaces
    df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')

    # Renommer certaines colonnes si nécessaire
    renommage = {
        'année': 'annee',
        'region': 'region',
        'région': 'region',
        'departement': 'region',
        'département': 'region'
    }

    df = df.rename(columns=renommage)

    return df


def corriger_regions(df):
    """
    Corrige et uniformise les noms de régions

    Args:
        df (DataFrame): Le dataframe avec une colonne 'region'

    Returns:
        DataFrame: Le dataframe avec régions corrigées
    """
    if 'region' not in df.columns:
        return df

    # Supprimer les espaces avant/après
    df['region'] = df['region'].str.strip()

    # Appliquer les corrections
    df['region'] = df['region'].replace(CORRECTIONS_REGIONS)

    return df


def convertir_types(df):
    """
    Convertit les colonnes dans les bons types de données

    Args:
        df (DataFrame): Le dataframe à convertir

    Returns:
        DataFrame: Le dataframe avec types corrects
    """
    # Convertir l'année en entier
    if 'annee' in df.columns:
        df['annee'] = pd.to_numeric(df['annee'], errors='coerce').astype('Int64')

    # Convertir la valeur en float
    if 'valeur' in df.columns:
        df['valeur'] = pd.to_numeric(df['valeur'], errors='coerce')

    # S'assurer que les colonnes texte sont bien des strings
    colonnes_texte = ['maladie', 'region', 'indicateur']
    for col in colonnes_texte:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df


def supprimer_doublons_et_vides(df):
    """
    Supprime les doublons et les lignes avec valeurs manquantes importantes

    Args:
        df (DataFrame): Le dataframe à nettoyer

    Returns:
        DataFrame: Le dataframe nettoyé
    """
    lignes_avant = len(df)

    # Supprimer les lignes complètement vides
    df = df.dropna(how='all')

    # Supprimer les lignes où les colonnes essentielles sont manquantes
    colonnes_essentielles = ['maladie', 'annee', 'region', 'valeur']
    colonnes_presentes = [col for col in colonnes_essentielles if col in df.columns]
    df = df.dropna(subset=colonnes_presentes)

    # Supprimer les doublons
    df = df.drop_duplicates()

    lignes_apres = len(df)
    lignes_supprimees = lignes_avant - lignes_apres

    if lignes_supprimees > 0:
        print(f"   🗑️  {lignes_supprimees} lignes supprimées (doublons/vides)")

    return df


def nettoyer_dataframe(df, nom_maladie):
    """
    Applique toutes les étapes de nettoyage sur un dataframe

    Args:
        df (DataFrame): Le dataframe à nettoyer
        nom_maladie (str): Nom de la maladie (pour les logs)

    Returns:
        DataFrame: Le dataframe nettoyé
    """
    print(f"🧹 Nettoyage : {nom_maladie}")
    print(f"   📊 Lignes avant nettoyage : {len(df)}")

    # Étape 1 : Standardiser les colonnes
    df = standardiser_colonnes(df)

    # Étape 2 : Corriger les régions
    df = corriger_regions(df)

    # Étape 3 : Convertir les types
    df = convertir_types(df)

    # Étape 4 : Supprimer doublons et vides
    df = supprimer_doublons_et_vides(df)

    # Étape 5 : Sélectionner uniquement les colonnes finales
    colonnes_presentes = [col for col in COLONNES_FINALES if col in df.columns]
    df = df[colonnes_presentes]

    print(f"   ✅ Lignes après nettoyage : {len(df)}\n")

    return df


def nettoyer_tous_les_dataframes(dataframes):
    """
    Nettoie tous les dataframes

    Args:
        dataframes (dict): Dictionnaire {nom_maladie: dataframe}

    Returns:
        dict: Dictionnaire des dataframes nettoyés
    """
    print("=" * 70)
    print("ÉTAPE 3 : NETTOYAGE DES DONNÉES")
    print("=" * 70)
    print()

    dataframes_clean = {}

    for nom_maladie, df in dataframes.items():
        df_clean = nettoyer_dataframe(df.copy(), nom_maladie)
        dataframes_clean[nom_maladie] = df_clean

    return dataframes_clean


# ========================================
# ÉTAPE 4 : AGRÉGATION DES DONNÉES
# ========================================

def agreger_donnees(df):
    """
    Agrège les données par année, région et indicateur
    Calcule la moyenne si plusieurs valeurs pour la même combinaison

    Args:
        df (DataFrame): Le dataframe à agréger

    Returns:
        DataFrame: Le dataframe agrégé
    """
    # Colonnes pour le groupement
    colonnes_groupe = ['maladie', 'annee', 'region', 'indicateur']
    colonnes_groupe = [col for col in colonnes_groupe if col in df.columns]

    # Agréger en calculant la moyenne des valeurs
    df_agrege = df.groupby(colonnes_groupe, as_index=False).agg({
        'valeur': 'mean'  # Moyenne des valeurs
    })

    # Arrondir les valeurs à 2 décimales
    df_agrege['valeur'] = df_agrege['valeur'].round(2)

    return df_agrege


def agreger_tous_les_dataframes(dataframes):
    """
    Agrège tous les dataframes

    Args:
        dataframes (dict): Dictionnaire {nom_maladie: dataframe}

    Returns:
        dict: Dictionnaire des dataframes agrégés
    """
    print("=" * 70)
    print("ÉTAPE 4 : AGRÉGATION DES DONNÉES")
    print("=" * 70)
    print()

    dataframes_agrege = {}

    for nom_maladie, df in dataframes.items():
        lignes_avant = len(df)
        df_agrege = agreger_donnees(df)
        lignes_apres = len(df_agrege)

        print(f"📊 {nom_maladie.capitalize()}")
        print(f"   Avant agrégation : {lignes_avant} lignes")
        print(f"   Après agrégation : {lignes_apres} lignes\n")

        dataframes_agrege[nom_maladie] = df_agrege

    return dataframes_agrege


# ========================================
# ÉTAPE 5 : FUSION DES DONNÉES
# ========================================

def fusionner_dataframes(dataframes):
    """
    Fusionne tous les dataframes en un seul

    Args:
        dataframes (dict): Dictionnaire {nom_maladie: dataframe}

    Returns:
        DataFrame: Le dataframe fusionné
    """
    print("=" * 70)
    print("ÉTAPE 5 : FUSION DES DONNÉES")
    print("=" * 70)
    print()

    print("🔗 Fusion de tous les dataframes...")

    # Concaténer tous les dataframes
    df_final = pd.concat(dataframes.values(), ignore_index=True)

    # Trier par année, maladie et région
    df_final = df_final.sort_values(['annee', 'maladie', 'region'])
    df_final = df_final.reset_index(drop=True)

    print(f"✅ Fusion terminée : {len(df_final)} lignes totales\n")

    return df_final


# ========================================
# ÉTAPE 6 : SAUVEGARDE ET RAPPORT
# ========================================

def sauvegarder_csv(df, nom_fichier):
    """
    Sauvegarde le dataframe en CSV

    Args:
        df (DataFrame): Le dataframe à sauvegarder
        nom_fichier (str): Nom du fichier de sortie
    """
    print("=" * 70)
    print("ÉTAPE 6 : SAUVEGARDE DU FICHIER FINAL")
    print("=" * 70)
    print()

    chemin = os.path.join(DOSSIER_DONNEES, nom_fichier)

    try:
        df.to_csv(chemin, index=False, encoding='utf-8-sig')
        print(f"💾 Fichier sauvegardé : {chemin}")
        print(f"📊 Nombre de lignes : {len(df)}")
        print(f"📋 Nombre de colonnes : {len(df.columns)}\n")
    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde : {e}\n")


def generer_rapport(df):
    """
    Génère un rapport statistique sur les données finales

    Args:
        df (DataFrame): Le dataframe final
    """
    print("=" * 70)
    print("📈 RAPPORT DE QUALITÉ DES DONNÉES")
    print("=" * 70)
    print()

    # Informations générales
    print("📊 INFORMATIONS GÉNÉRALES")
    print(f"   • Nombre total de lignes : {len(df)}")
    print(f"   • Nombre de colonnes : {len(df.columns)}")
    print(f"   • Colonnes : {', '.join(df.columns)}\n")

    # Statistiques par maladie
    print("🏥 RÉPARTITION PAR MALADIE")
    maladies = df['maladie'].value_counts()
    for maladie, count in maladies.items():
        pourcentage = (count / len(df)) * 100
        print(f"   • {maladie.capitalize()} : {count} lignes ({pourcentage:.1f}%)")
    print()

    # Plage temporelle
    if 'annee' in df.columns:
        print("📅 PLAGE TEMPORELLE")
        print(f"   • Année minimale : {df['annee'].min()}")
        print(f"   • Année maximale : {df['annee'].max()}")
        print(f"   • Nombre d'années : {df['annee'].nunique()}\n")

    # Couverture géographique
    if 'region' in df.columns:
        print("🗺️  COUVERTURE GÉOGRAPHIQUE")
        print(f"   • Nombre de régions : {df['region'].nunique()}")
        regions = df['region'].unique()[:5]  # Afficher les 5 premières
        print(f"   • Exemples : {', '.join(regions)}...\n")

    # Types d'indicateurs
    if 'indicateur' in df.columns:
        print("📌 TYPES D'INDICATEURS")
        indicateurs = df['indicateur'].value_counts()
        for indicateur, count in indicateurs.items():
            print(f"   • {indicateur} : {count} observations")
        print()

    # Valeurs manquantes
    print("🔍 VALEURS MANQUANTES")
    valeurs_manquantes = df.isnull().sum()
    if valeurs_manquantes.sum() == 0:
        print("   ✅ Aucune valeur manquante !")
    else:
        for col, count in valeurs_manquantes.items():
            if count > 0:
                pourcentage = (count / len(df)) * 100
                print(f"   • {col} : {count} manquantes ({pourcentage:.1f}%)")
    print()

    # Statistiques sur les valeurs
    if 'valeur' in df.columns:
        print("📊 STATISTIQUES SUR LES VALEURS")
        print(f"   • Minimum : {df['valeur'].min():.2f}")
        print(f"   • Maximum : {df['valeur'].max():.2f}")
        print(f"   • Moyenne : {df['valeur'].mean():.2f}")
        print(f"   • Médiane : {df['valeur'].median():.2f}\n")

    # Aperçu des données
    print("👀 APERÇU DES DONNÉES (5 premières lignes)")
    print(df.head().to_string(index=False))
    print()


# ========================================
# FONCTION PRINCIPALE
# ========================================

def main():
    """
    Fonction principale qui orchestre tout le pipeline de nettoyage
    """

    # Étape 1 : Exécuter le scraping (optionnel)
    if not executer_scraping():
        print("❌ Impossible de continuer sans données.")
        return

    # Étape 2 : Charger les fichiers CSV
    dataframes = charger_tous_les_fichiers()

    if not dataframes:
        print("❌ Aucun fichier CSV chargé. Impossible de continuer.")
        return

    # Étape 3 : Nettoyer les données
    dataframes_clean = nettoyer_tous_les_dataframes(dataframes)

    # Étape 4 : Agréger les données
    dataframes_agrege = agreger_tous_les_dataframes(dataframes_clean)

    # Étape 5 : Fusionner en un seul dataframe
    df_final = fusionner_dataframes(dataframes_agrege)

    # Étape 6 : Sauvegarder
    sauvegarder_csv(df_final, FICHIER_SORTIE)

    # Rapport final
    generer_rapport(df_final)

    print("=" * 70)
    print("✅ NETTOYAGE TERMINÉ AVEC SUCCÈS !")
    print("=" * 70)
    print()
    print(f"💡 Fichier final disponible : {DOSSIER_DONNEES}/{FICHIER_SORTIE}")
    print("💡 Prochaine étape : Visualisation des données !\n")


# ========================================
# EXÉCUTION DU SCRIPT
# ========================================

if __name__ == "__main__":
    main()