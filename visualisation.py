import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import urllib.request
import json
import random
import io
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from PIL import Image
import traceback

st.set_page_config(
    page_title="Dashboard Maladies Chroniques",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personnalisé pour améliorer le design
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stMetric {
        background-color: #0c111a;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .stMetric label {
        color: #ffffff !important;
    }
    .stMetric [data-testid="stMetricValue"] {
        color: #ffffff !important;
    }
    .stMetric [data-testid="stMetricDelta"] {
        color: #b0b0b0 !important;
    }
    h1 {
        color: #1f77b4;
        padding-bottom: 20px;
    }
    h2 {
        color: #2c3e50;
        padding-top: 20px;
    }
    </style>
    """, unsafe_allow_html=True)


def get_unit_label(indicateur):
    """Retourne une chaîne d'unité lisible pour un indicateur.

    - 'prevalence' -> '%'
    - 'incidence' -> 'pour 100 000 hab'
    - 'mortalite' -> 'pour 100 000 hab'
    """
    indic = str(indicateur).lower()
    if indic == 'prevalence':
        return '(%)'
    if indic in ('incidence', 'mortalite'):
        return '(pour 100 000 hab)'
    return ''


def format_value_with_unit(val, indicateur):
    """Formate une valeur numérique selon l'indicateur (pour affichage).
    Prévalence -> affiche avec 2 décimales et un %,
    Incidence/Mortalité -> affiche en entier (sans décimales) et label 'pour 100 000 hab'.
    """
    try:
        if pd.isna(val):
            return "N/A"
        indic = str(indicateur).lower()
        if indic == 'prevalence':
            return f"{float(val):.2f} %"
        if indic in ('incidence', 'mortalite'):
            # Montrer comme entier
            return f"{int(round(float(val))):,}".replace(',', ' ') + " par 100 000"
        return f"{val}"
    except Exception:
        return str(val)

@st.cache_data
def charger_donnees():
    """
    Charge le fichier CSV des données nettoyées

    Returns:
        DataFrame or None: Les données chargées ou None si erreur
    """
    # Première tentative : récupérer les données depuis l'API locale (si disponible)
    api_url = os.environ.get("API_URL", "http://127.0.0.1:8000/observations?limit=1000000")
    try:
        with urllib.request.urlopen(api_url, timeout=5) as resp:
            if getattr(resp, 'status', None) in (200, None):
                try:
                    data = json.load(resp)
                    df_api = pd.DataFrame(data)
                    colonnes_requises = ['maladie', 'annee', 'region', 'indicateur', 'valeur']
                    if all(col in df_api.columns for col in colonnes_requises):
                        df_api['annee'] = pd.to_numeric(df_api['annee'], errors='coerce')
                        df_api['valeur'] = pd.to_numeric(df_api['valeur'], errors='coerce')
                        df_api = df_api.dropna(subset=colonnes_requises)
                        return df_api
                except Exception as e:
                    st.info(f"Réponse API invalide : {e}")
    except Exception as e:
        # Ne pas spammer l'erreur, afficher un message d'info et continuer avec CSV
        st.info(f"")

    # Essayer plusieurs noms de fichiers possibles et choisir le plus complet (plus de lignes)
    fichiers_possibles = [
        "maladies_combine.csv",
        "maladies_combined.csv",
        "maladies_clean.csv"
    ]

    colonnes_requises = ['maladie', 'annee', 'region', 'indicateur', 'valeur']
    best_df = None
    best_file = None

    for nom_fichier in fichiers_possibles:
        chemin_csv = os.path.join("donnees_sante", nom_fichier)

        if os.path.exists(chemin_csv):
            try:
                df_candidate = pd.read_csv(chemin_csv, encoding='utf-8-sig')

                # Vérifier que les colonnes nécessaires existent
                if not all(col in df_candidate.columns for col in colonnes_requises):
                    # ignorer ce fichier s'il est incomplet
                    continue

                # Garder le fichier ayant le plus de lignes (plus complet)
                if best_df is None or len(df_candidate) > len(best_df):
                    best_df = df_candidate.copy()
                    best_file = nom_fichier

            except Exception:
                # ignorer ce fichier et continuer
                continue

    if best_df is not None:
        df = best_df
        # Convertir les types si nécessaire
        df['annee'] = pd.to_numeric(df['annee'], errors='coerce')
        df['valeur'] = pd.to_numeric(df['valeur'], errors='coerce')

        # Supprimer les lignes avec des valeurs NaN dans les colonnes essentielles
        df = df.dropna(subset=colonnes_requises)


        return df

    # Si aucun fichier n'a été trouvé
    st.error("Aucun fichier de données trouvé dans le dossier 'donnees_sante'")
    st.info("Fichiers recherchés : " + ", ".join(fichiers_possibles))
    st.info("Assurez-vous d'avoir exécuté les scripts de collecte et de nettoyage d'abord.")

    # Afficher les fichiers présents dans le dossier
    if os.path.exists("donnees_sante"):
        fichiers_presents = os.listdir("donnees_sante")
        if fichiers_presents:
            st.info(f"Fichiers présents dans 'donnees_sante' : {', '.join(fichiers_presents)}")

    return None

def creer_graphique_evolution_temporelle(df, maladie, indicateur):
    """
    Crée un graphique d'évolution temporelle pour une maladie et un indicateur
    """
    try:
        # Filtrer les données
        df_filtre = df[
            (df['maladie'] == maladie) &
            (df['indicateur'] == indicateur)
            ]

        if df_filtre.empty:
            return None

        # Agréger par année (moyenne des régions)
        df_agg = df_filtre.groupby('annee', as_index=False)['valeur'].mean()

        # Créer le graphique
        unit_label = get_unit_label(indicateur)
        fig = px.line(
            df_agg,
            x='annee',
            y='valeur',
            title=f'Évolution de {indicateur} - {maladie.capitalize()}',
            labels={'annee': 'Année', 'valeur': f'{indicateur.capitalize()} {unit_label}'},
            markers=True
        )

        # Personnaliser le graphique
        fig.update_traces(
            line_color='#1f77b4',
            line_width=3,
            marker=dict(size=8, color='#ff7f0e')
        )

        fig.update_layout(
            hovermode='x unified',
            plot_bgcolor='white',
            font=dict(size=12),
            title_font_size=16
        )

        return fig
    except Exception as e:
        st.error(f"Erreur dans creer_graphique_evolution_temporelle: {e}")
        return None


def creer_graphique_barres_regions(df, maladie, indicateur, annee):
    """
    Crée un bar chart par région pour une année donnée
    """
    try:
        # Filtrer les données
        df_filtre = df[
            (df['maladie'] == maladie) &
            (df['indicateur'] == indicateur) &
            (df['annee'] == annee)
            ]

        if df_filtre.empty:
            return None

        # Trier par valeur décroissante
        df_filtre = df_filtre.sort_values('valeur', ascending=True)

        # Créer le graphique horizontal
        fig = px.bar(
            df_filtre,
            x='valeur',
            y='region',
            orientation='h',
            title=f'{indicateur.capitalize()} par région - {maladie.capitalize()} ({int(annee)})',
                labels={'valeur': f'{indicateur.capitalize()} {get_unit_label(indicateur)}', 'region': 'Région'},
            color='valeur',
            color_continuous_scale='Blues'
        )

        fig.update_layout(
            height=500,
            plot_bgcolor='white',
            font=dict(size=11),
            title_font_size=16,
            showlegend=False
        )

        return fig
    except Exception as e:
        st.error(f"Erreur dans creer_graphique_barres_regions: {e}")
        return None


def creer_graphique_comparaison_maladies(df, indicateur, annee):
    """
    Crée un graphique de comparaison entre les trois maladies
    """
    try:
        # Filtrer les données
        df_filtre = df[
            (df['indicateur'] == indicateur) &
            (df['annee'] == annee)
            ]

        if df_filtre.empty:
            return None

        # Agréger par maladie (moyenne nationale)
        df_agg = df_filtre.groupby('maladie', as_index=False)['valeur'].mean()

        # Créer le graphique
        fig = px.bar(
            df_agg,
            x='maladie',
            y='valeur',
            title=f'Comparaison des maladies - {indicateur.capitalize()} ({int(annee)})',
            labels={'maladie': 'Maladie', 'valeur': f'{indicateur.capitalize()} {get_unit_label(indicateur)}'},
            color='maladie',
            color_discrete_map={
                'diabete': '#1f77b4',
                'cardiovasculaire': '#ff7f0e',
                'cancer': '#2ca02c'
            }
        )

        fig.update_layout(
            plot_bgcolor='white',
            font=dict(size=12),
            title_font_size=16,
            showlegend=False
        )

        return fig
    except Exception as e:
        st.error(f"Erreur dans creer_graphique_comparaison_maladies: {e}")
        return None


def creer_heatmap_region_annee(df, maladie, indicateur):
    """
    Crée une heatmap région × année pour une maladie
    """
    try:
        # Filtrer les données
        df_filtre = df[
            (df['maladie'] == maladie) &
            (df['indicateur'] == indicateur)
            ]

        if df_filtre.empty:
            return None

        # Créer un pivot table
        pivot = df_filtre.pivot_table(
            values='valeur',
            index='region',
            columns='annee',
            aggfunc='mean'
        )

        if pivot.empty:
            return None

        # Créer la heatmap
        # Utiliser une échelle divergeante où les faibles valeurs sont vertes
        # et les fortes valeurs sont rouges : inverser 'RdYlGn' pour obtenir
        # low=green -> high=red
        fig = px.imshow(
            pivot,
            labels=dict(x="Année", y="Région", color=f"{indicateur.capitalize()} {get_unit_label(indicateur)}"),
            title=f'Heatmap {indicateur.capitalize()} - {maladie.capitalize()}',
            color_continuous_scale='RdYlGn_r',
            aspect='auto'
        )

        fig.update_layout(
            height=500,
            font=dict(size=11),
            title_font_size=16
        )

        return fig
    except Exception as e:
        st.error(f"Erreur dans creer_heatmap_region_annee: {e}")
        return None


def creer_carte_france(df, maladie, indicateur, annee):
    """
    Crée une carte interactive de France avec les régions
    """
    try:
        # Filtrer les données
        df_filtre = df[
            (df['maladie'] == maladie) &
            (df['indicateur'] == indicateur) &
            (df['annee'] == annee)
            ]

        if df_filtre.empty:
            return None

        # Dictionnaire de correspondance région -> code GeoJSON
        regions_geojson = {
            'Île-de-France': 'Île-de-France',
            'Auvergne-Rhône-Alpes': 'Auvergne-Rhône-Alpes',
            'Nouvelle-Aquitaine': 'Nouvelle-Aquitaine',
            'Occitanie': 'Occitanie',
            'Hauts-de-France': 'Hauts-de-France',
            'Provence-Alpes-Côte d\'Azur': 'Provence-Alpes-Côte d\'Azur',
            'Grand Est': 'Grand Est',
            'Pays de la Loire': 'Pays de la Loire',
            'Bretagne': 'Bretagne',
            'Normandie': 'Normandie',
            'Bourgogne-Franche-Comté': 'Bourgogne-Franche-Comté',
            'Centre-Val de Loire': 'Centre-Val de Loire',
            'Corse': 'Corse'
        }

        # URL du GeoJSON des régions françaises
        geojson_url = "https://france-geojson.gregoiredavid.fr/repo/regions.geojson"

        # Créer la carte choroplèthe
        unit_label = get_unit_label(indicateur)
        fig = px.choropleth(
            df_filtre,
            geojson=geojson_url,
            locations='region',
            featureidkey="properties.nom",
            color='valeur',
            color_continuous_scale='Reds',
            hover_name='region',
            hover_data={'region': False, 'valeur': ':.2f'},
            labels={'valeur': f"{indicateur.capitalize()} {unit_label}"},
            title=f'Carte de France - {indicateur.capitalize()} - {maladie.capitalize()} ({int(annee)})'
        )

        # Centrer sur la France
        fig.update_geos(
            fitbounds="locations",
            visible=False,
            projection_type="mercator",
            center={"lat": 46.5, "lon": 2.5},
            scope="europe",
            bgcolor='rgba(0,0,0,0)',  # Fond transparent
            showland=False,           # Masquer les terres
            showocean=False,          # Masquer l'océan
            showcountries=False,      # Masquer les frontières des pays
            showlakes=False           # Masquer les lacs
        )

        fig.update_layout(
            height=800,
            font=dict(size=12),
            title_font_size=16,
            margin={"r": 0, "t": 50, "l": 0, "b": 0},
            coloraxis_colorbar=dict(
                title=f"{indicateur.capitalize()} {unit_label}",
                thickness=15,
                len=0.7
            ),
            dragmode=False,           # Désactiver le zoom et le pan
            paper_bgcolor='rgba(0,0,0,0)',  # Fond du papier transparent
            plot_bgcolor='rgba(0,0,0,0)'    # Fond du plot transparent
        )

        return fig
    except Exception as e:
        st.error(f"Erreur dans creer_carte_france: {e}")
        # En cas d'erreur, retourner un graphique en barres comme fallback
        return creer_carte_france_fallback(df, maladie, indicateur, annee)


def creer_carte_france_fallback(df, maladie, indicateur, annee):
    """
    Version de secours : graphique en barres si la carte ne fonctionne pas
    """
    try:
        df_filtre = df[
            (df['maladie'] == maladie) &
            (df['indicateur'] == indicateur) &
            (df['annee'] == annee)
            ]

        if df_filtre.empty:
            return None

        df_filtre = df_filtre.sort_values('valeur', ascending=False)

        fig = px.bar(
            df_filtre,
            x='region',
            y='valeur',
            title=f'Régions - {indicateur.capitalize()} - {maladie.capitalize()} ({int(annee)})',
            labels={'region': 'Région', 'valeur': f'{indicateur.capitalize()}'},
            color='valeur',
            color_continuous_scale='Reds'
        )

        fig.update_layout(
            height=500,
            plot_bgcolor='white',
            font=dict(size=10),
            title_font_size=16,
            xaxis_tickangle=-45
        )

        return fig
    except:
        return None


def calculer_statistiques(df, maladie, indicateur, annee):
    """
    Calcule les statistiques clés pour une maladie
    """
    try:
        df_filtre = df[
            (df['maladie'] == maladie) &
            (df['indicateur'] == indicateur) &
            (df['annee'] == annee)
            ]

        if df_filtre.empty:
            return None

        # Récupérer unité si présente dans le dataframe
        unite = None
        if 'unite' in df_filtre.columns:
            unite_vals = df_filtre['unite'].dropna().unique()
            if len(unite_vals) > 0:
                unite = unite_vals[0]

        stats = {
            'moyenne_nationale': df_filtre['valeur'].mean(),
            'min': df_filtre['valeur'].min(),
            'max': df_filtre['valeur'].max(),
            'region_min': df_filtre.loc[df_filtre['valeur'].idxmin(), 'region'],
            'region_max': df_filtre.loc[df_filtre['valeur'].idxmax(), 'region'],
            'unite': unite
        }

        return stats
    except Exception as e:
        st.error(f"Erreur dans calculer_statistiques: {e}")
        return None


def creer_rapport_pdf(figs, maladie, indicateur, annee, stats=None):
    """
    Génère un PDF en mémoire contenant les figures Plotly (PNG) et quelques métriques.
    Retourne les octets du PDF ou None en cas d'erreur.
    """
    try:
        # Assurer le dossier de sortie/log
        log_dir = os.path.join('donnees_sante')
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, 'rapport_error.log')

        buf = io.BytesIO()
        c = canvas.Canvas(buf, pagesize=A4)
        pw, ph = A4
        margin = 40

        # Titre
        c.setFont("Helvetica-Bold", 16)
        c.drawString(margin, ph - margin, f"Rapport — {maladie.capitalize()} — {indicateur.capitalize()} ({int(annee)})")

        y = ph - margin - 30

        # Statistiques résumé
        if stats:
            c.setFont("Helvetica", 10)
            try:
                moy = format_value_with_unit(stats.get('moyenne_nationale'), indicateur)
                mn = format_value_with_unit(stats.get('min'), indicateur)
                mx = format_value_with_unit(stats.get('max'), indicateur)
                region_min = stats.get('region_min')
                region_max = stats.get('region_max')
                text = f"Moyenne nationale: {moy} — Min: {mn} ({region_min}) — Max: {mx} ({region_max})"
                c.drawString(margin, y, text)
                y -= 20
            except Exception:
                pass

        any_image = False

        # Insérer chaque figure en tant qu'image, logger les échecs
        for idx, fig in enumerate(figs or []):
            if fig is None:
                with open(log_path, 'a', encoding='utf-8') as lf:
                    lf.write(f"[INFO] Figure {idx} is None — skipped\n")
                continue

            try:
                # Si la figure utilise Mapbox mais n'a pas de style, forcer open-street-map
                try:
                    if hasattr(fig, 'layout') and getattr(fig.layout, 'mapbox', None) is not None:
                        mb = fig.layout.mapbox
                        if not getattr(mb, 'style', None):
                            fig.update_layout(mapbox_style='open-street-map')
                except Exception:
                    pass

                img_bytes = fig.to_image(format='png', engine='kaleido')
            except Exception as e:
                tb = traceback.format_exc()
                with open(log_path, 'a', encoding='utf-8') as lf:
                    lf.write(f"[ERROR] fig.to_image failed for figure {idx}: {e}\n{tb}\n")
                continue

            try:
                img = Image.open(io.BytesIO(img_bytes)).convert('RGBA')
            except Exception as e:
                tb = traceback.format_exc()
                with open(log_path, 'a', encoding='utf-8') as lf:
                    lf.write(f"[ERROR] PIL open failed for figure {idx}: {e}\n{tb}\n")
                continue

            any_image = True

            # adapter la taille pour tenir la page
            max_w = pw - 2 * margin
            max_h = ph - 2 * margin - 60
            img_w, img_h = img.size
            ratio = min(max_w / img_w, max_h / img_h, 1.0)
            new_w = int(img_w * ratio)
            new_h = int(img_h * ratio)
            img_resized = img.resize((new_w, new_h), Image.Resampling.LANCZOS)

            if y - new_h < margin:
                c.showPage()
                y = ph - margin

            img_reader = ImageReader(img_resized)
            c.drawImage(img_reader, margin, y - new_h, width=new_w, height=new_h)
            y = y - new_h - 20

        if not any_image:
            # écrire message dans le PDF et renvoyer (mais log existant contiendra détails)
            c.setFont("Helvetica", 12)
            c.drawString(margin, y, "Aucune figure n'a pu être rendue dans le rapport. Voir donnees_sante/rapport_error.log pour les détails.")

        c.save()
        buf.seek(0)
        return buf.read()
    except Exception as e:
        tb = traceback.format_exc()
        # écrire erreur fatale dans logfile et stdout
        try:
            os.makedirs('donnees_sante', exist_ok=True)
            with open(os.path.join('donnees_sante', 'rapport_error.log'), 'a', encoding='utf-8') as lf:
                lf.write(f"[FATAL] Erreur création PDF: {e}\n{tb}\n")
        except Exception:
            pass
        try:
            st.error(f"Erreur création PDF: {e}")
            st.error(tb)
        except Exception:
            print("Erreur création PDF:", e)
            print(tb)
        return None


# ========================================
# INTERFACE PRINCIPALE DU DASHBOARD
# ========================================

def main():
    """
    Fonction principale qui crée l'interface du dashboard
    """

    # Titre principal
    st.title("Dashboard - Maladies Chroniques en France")
    st.markdown("*Analyse des maladies chroniques en France*")
    st.markdown("---")

    # Charger les données
    df = charger_donnees()

    if df is None:
        st.stop()

  


    st.sidebar.header("Filtres de visualisation")

    # Sélection de la maladie
    maladies_disponibles = sorted(df['maladie'].unique())
    maladie_selectionnee = st.sidebar.selectbox(
        "Sélectionner une maladie",
        maladies_disponibles,
        format_func=lambda x: x.capitalize()
    )

    # Sélection de l'indicateur
    indicateurs_disponibles = sorted(df['indicateur'].unique())
    indicateur_selectionne = st.sidebar.selectbox(
        "Sélectionner un indicateur",
        indicateurs_disponibles,
        format_func=lambda x: x.capitalize()
    )

    # Sélection de l'année
    annees_disponibles = sorted(df['annee'].dropna().unique(), reverse=True)
    annee_selectionnee = st.sidebar.selectbox(
        "Sélectionner une année",
        annees_disponibles,
        format_func=lambda x: str(int(x))
    )

    # Bouton pour générer un rapport PDF avec les graphiques courants
    if st.sidebar.button("Générer le rapport PDF"):
        with st.spinner("Génération du PDF..."):
            # Figures principales
            fig_evolution_tmp = creer_graphique_evolution_temporelle(df, maladie_selectionnee, indicateur_selectionne)
            fig_barres_tmp = creer_graphique_barres_regions(df, maladie_selectionnee, indicateur_selectionne, annee_selectionnee)
            fig_heatmap_tmp = creer_heatmap_region_annee(df, maladie_selectionnee, indicateur_selectionne)

            # Figure comparative (toutes les maladies)
            fig_comparaison_tmp = creer_graphique_comparaison_maladies(df, indicateur_selectionne, annee_selectionnee)

            # Carte des hôpitaux (générée via le même helper que l'UI)
            try:
                hop = charger_hopitaux(df)
                # choisir top établissements pour la maladie sélectionnée
                if maladie_selectionnee and f'score_{maladie_selectionnee}' in hop.columns:
                    top = hop.sort_values(by=f'score_{maladie_selectionnee}', ascending=False)
                    to_plot = top.head(50)
                else:
                    to_plot = hop.head(50)

                fig_hopitaux_tmp = px.scatter_mapbox(
                    to_plot,
                    lat='lat',
                    lon='lon',
                    hover_name='nom',
                    hover_data={'region': True},
                    size=to_plot.columns[0] if False else None,
                    color=f'score_{maladie_selectionnee}' if (maladie_selectionnee and f'score_{maladie_selectionnee}' in hop.columns) else None,
                    color_continuous_scale='RdYlGn_r',
                    size_max=16,
                    zoom=5,
                    center={'lat': 46.5, 'lon': 2.5},
                    title=f"Hôpitaux — {maladie_selectionnee.capitalize()}"
                )
                fig_hopitaux_tmp.update_layout(mapbox_style='open-street-map', height=450, margin={'r':0,'t':40,'l':0,'b':0})
            except Exception:
                fig_hopitaux_tmp = None

            stats_tmp = calculer_statistiques(df, maladie_selectionnee, indicateur_selectionne, annee_selectionnee)

            figs_for_pdf = [fig_evolution_tmp, fig_barres_tmp, fig_heatmap_tmp, fig_comparaison_tmp, fig_hopitaux_tmp]
            pdf_bytes = creer_rapport_pdf(figs_for_pdf, maladie_selectionnee, indicateur_selectionne, annee_selectionnee, stats_tmp)

        if pdf_bytes:
            st.sidebar.download_button(
                label="Télécharger le rapport PDF",
                data=pdf_bytes,
                file_name=f"rapport_{maladie_selectionnee}_{indicateur_selectionne}_{int(annee_selectionnee)}.pdf",
                mime='application/pdf'
            )
        else:
            st.sidebar.warning("Impossible de générer le PDF — vérifiez les dépendances (kaleido, Pillow, reportlab).")

    st.sidebar.markdown("---")
    
    # Bouton pour ouvrir la carte des hôpitaux / centres spécialisés
    if 'show_hosp_map' not in st.session_state:
        st.session_state['show_hosp_map'] = False

    if st.sidebar.button("Ouvrir la carte des hôpitaux / centres spécialisés"):
        st.session_state['show_hosp_map'] = True

    if st.sidebar.button("Fermer la carte des hôpitaux"):
        st.session_state['show_hosp_map'] = False

    # Si activé, afficher la section dédiée aux hôpitaux
    if st.session_state.get('show_hosp_map'):
        st.header("Carte : Hôpitaux et centres spécialisés")
        # Charger ou générer le dataset des hôpitaux
        hop = charger_hopitaux(df)

        # Barre de recherche / sélection de maladie
        maladies_dispo = sorted(df['maladie'].unique())
        maladie_recherche = st.selectbox("Rechercher une maladie (pour trouver les hôpitaux spécialisés)",
                                         [''] + maladies_dispo,
                                         format_func=lambda x: x.capitalize() if x else "-- Choisir une maladie --")

        if maladie_recherche:
            # Trouver les hôpitaux les plus compétents pour la maladie
            top = hop.sort_values(by=f"score_{maladie_recherche}", ascending=False)

            # Options d'affichage : afficher tous les hôpitaux ou seulement le top N
            show_all = st.checkbox("Afficher tous les hôpitaux", value=False)
            if not show_all:
                # par défaut, montrer le top 20 pour éviter le surpeuplement
                to_plot = top.head(20)
            else:
                to_plot = top.copy()

            # Filtre par score minimal pour améliorer lisibilité
            min_score = st.slider("Score minimal à afficher", 0, 100, 0)
            to_plot = to_plot[to_plot[f'score_{maladie_recherche}'] >= min_score]

            st.markdown(f"### Hôpitaux pour : **{maladie_recherche.capitalize()}** — points seulement")

            # Carte interactive (Plotly scatter_mapbox)
            # - style OpenStreetMap (pas de token nécessaire)
            # - couleur selon le score (vert->rouge), taille proportionnelle
            fig_h = px.scatter_mapbox(
                to_plot,
                lat='lat',
                lon='lon',
                hover_name='nom',
                hover_data={
                    'region': True,
                    f'score_{maladie_recherche}': ':.1f'
                },
                size=f'score_{maladie_recherche}',
                color=f'score_{maladie_recherche}',
                color_continuous_scale='RdYlGn_r',
                size_max=24,
                zoom=5,
                center={'lat': 46.5, 'lon': 2.5},
                title=f"Hôpitaux spécialisés pour {maladie_recherche.capitalize()}"
            )
            fig_h.update_layout(mapbox_style='open-street-map', height=650, margin={'r':0,'t':40,'l':0,'b':0})
            fig_h.update_traces(marker=dict(opacity=0.85))
            st.plotly_chart(fig_h, width='stretch')
        else:
            st.info("Sélectionnez une maladie pour voir les hôpitaux spécialisés et leur localisation.")
    # ========================================
    # STATISTIQUES CLÉS
    # ========================================

    st.header(" Statistiques clés")

    stats = calculer_statistiques(df, maladie_selectionnee, indicateur_selectionne, annee_selectionnee)

    if stats:
        col1, col2, col3 = st.columns(3)

        # Afficher les statistiques en tenant compte de l'unité
        unité_stats = stats.get('unite') if isinstance(stats, dict) else None

        with col1:
            st.metric(
                label="Moyenne nationale",
                value=format_value_with_unit(stats['moyenne_nationale'], indicateur_selectionne)
            )

        with col2:
            st.metric(
                label=f"Région minimale",
                value=format_value_with_unit(stats['min'], indicateur_selectionne),
                delta=stats['region_min']
            )

        with col3:
            st.metric(
                label=f"Région maximale",
                value=format_value_with_unit(stats['max'], indicateur_selectionne),
                delta=stats['region_max']
            )
    else:
        st.warning("Pas de statistiques disponibles pour cette combinaison.")

    st.markdown("---")

    # ========================================
    # GRAPHIQUES PRINCIPAUX
    # ========================================

    # Section 1 : Évolution temporelle
    st.header("Évolution temporelle")

    fig_evolution = creer_graphique_evolution_temporelle(
        df,
        maladie_selectionnee,
        indicateur_selectionne
    )

    if fig_evolution:
        st.plotly_chart(fig_evolution, width='stretch')
    else:
        st.warning("Pas de données disponibles pour cette combinaison.")

    st.markdown("---")

    # Section 2 : Deux colonnes - Barres par région + Comparaison maladies
    st.header("Analyse régionale et comparative")

    col1, col2 = st.columns(2)

    with col1:
        fig_barres = creer_graphique_barres_regions(
            df,
            maladie_selectionnee,
            indicateur_selectionne,
            annee_selectionnee
        )

        if fig_barres:
            st.plotly_chart(fig_barres, width='stretch')
        else:
            st.warning("Pas de données pour cette année.")

    with col2:
        fig_comparaison = creer_graphique_comparaison_maladies(
            df,
            indicateur_selectionne,
            annee_selectionnee
        )

        if fig_comparaison:
            st.plotly_chart(fig_comparaison, width='stretch')
        else:
            st.warning("Pas de données pour cette comparaison.")

    st.markdown("---")

    # Section 3 : Carte de France
    st.header("Carte interactive de France")

    fig_carte = creer_carte_france(
        df,
        maladie_selectionnee,
        indicateur_selectionne,
        annee_selectionnee
    )

    if fig_carte:
        st.plotly_chart(fig_carte, width='stretch')
    else:
        st.warning("Pas de données cartographiques disponibles.")

    st.markdown("---")

    # Section 4 : Heatmap
    st.header("Heatmap : Évolution par région")

    fig_heatmap = creer_heatmap_region_annee(
        df,
        maladie_selectionnee,
        indicateur_selectionne
    )

    if fig_heatmap:
        st.plotly_chart(fig_heatmap, width='stretch')
    else:
        st.warning("Pas assez de données pour créer la heatmap.")

    st.markdown("---")

    # ========================================
    # SECTION : DONNÉES BRUTES
    # ========================================

    st.header("Données brutes")

    with st.expander("Afficher les données filtrées"):
        df_filtre_affichage = df[
            (df['maladie'] == maladie_selectionnee) &
            (df['indicateur'] == indicateur_selectionne)
            ].sort_values(['annee', 'region'])

        st.dataframe(df_filtre_affichage, width='stretch')

        # Bouton de téléchargement
        csv = df_filtre_affichage.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Télécharger les données filtrées (CSV)",
            data=csv,
            file_name=f"{maladie_selectionnee}_{indicateur_selectionne}.csv",
            mime="text/csv"
        )

    # ========================================
    # FOOTER
    # ========================================

    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 20px;'>
        <p>Projet B3 Data - Maladies Chroniques en France</p>
        <p>Dashboard créé avec Streamlit & Plotly</p>
    </div>
    """, unsafe_allow_html=True)


# ========================================
# POINT D'ENTRÉE DU SCRIPT
# ========================================

def charger_hopitaux(df):
    """Charge ou génère un fichier synthétique `donnees_sante/hopitaux.csv`.
    Le fichier contient : nom, region, lat, lon, et des colonnes score_<maladie> indiquant
    la compétence de l'établissement pour chaque maladie.
    """
    path = os.path.join('donnees_sante', 'hopitaux.csv')
    if os.path.exists(path):
        try:
            hop = pd.read_csv(path, encoding='utf-8-sig')
            return hop
        except Exception:
            pass

    # Générer un jeu synthétique
    regions_coords = {
        'Île-de-France': (48.8566, 2.3522),
        'Auvergne-Rhône-Alpes': (45.75, 4.85),
        'Nouvelle-Aquitaine': (45.75, -0.6),
        'Occitanie': (43.6, 1.44),
        'Hauts-de-France': (50.4, 3.0),
        'Provence-Alpes-Côte d\'Azur': (43.5, 6.0),
        'Bretagne': (48.1, -2.0),
        'Normandie': (49.1, 0.3),
        'Pays de la Loire': (47.5, -0.5),
        'Grand Est': (48.6, 7.8),
        'Bourgogne-Franche-Comté': (47.3, 5.0),
        'Centre-Val de Loire': (47.9, 1.9),
        'Corse': (42.0, 9.0)
    }

    maladies = sorted(df['maladie'].unique())
    rows = []
    random.seed(42)
    for region, (latc, lonc) in regions_coords.items():
        n = random.randint(3, 6)  # 3-6 hospitals per region
        for i in range(1, n+1):
            name = f"CHU {region} {i}"
            # jitter around centroid
            lat = latc + random.uniform(-0.3, 0.3)
            lon = lonc + random.uniform(-0.5, 0.5)
            row = {'nom': name, 'region': region, 'lat': lat, 'lon': lon}
            # competency scores per disease (0-100)
            for m in maladies:
                # base score depends on disease and random
                base = 50 + (hash(region + m) % 20) - 10
                score = max(0.0, min(100.0, base + random.uniform(-20, 20)))
                row[f'score_{m}'] = round(score, 1)
            rows.append(row)

    hop = pd.DataFrame(rows)
    try:
        hop.to_csv(path, index=False, encoding='utf-8-sig')
    except Exception:
        pass

    # Essayer d'enregistrer la table des hôpitaux dans la base de données locale (si disponible)
    try:
        # Import local db helper (définit `get_engine()`)
        from db_config import get_engine
        engine = get_engine()
        # Utiliser pandas.to_sql pour écrire dans une table nommée 'hospitals'
        # if_exists='replace' pour garder une version fraîche lors du développement
        hop.to_sql('hospitals', engine, if_exists='replace', index=False)
        try:
            st.info("Hôpitaux enregistrés dans la base de données (table 'hospitals').")
        except Exception:
            print("Hôpitaux enregistrés dans la base de données (table 'hospitals').")
    except Exception as e:
        # Ne pas échouer si la base n'est pas accessible — afficher info discrète
        try:
            st.info(f"Écriture en base non disponible : {e}")
        except Exception:
            print("Écriture en base non disponible:", e)
    return hop


# ========================================
# POINT D'ENTRÉE DU SCRIPT
# ========================================

if __name__ == "__main__":
    main()