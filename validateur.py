# -*- coding: utf-8 -*-
"""
Validateur de données Réseaux EAU 
Contrôle qualité des GeoPackages avant intégration SIG
"""

import streamlit as st
import geopandas as gpd
import pandas as pd
import io
import zipfile
import tempfile
import os

st.set_page_config(
    page_title="Validateur SIG - Saint-Lô Agglo",
    page_icon="🛡️",
    layout="wide"
)

# ─── CSS personnalisé ────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');
    
    html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
    
    .main-title {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 1.8rem;
        font-weight: 600;
        color: #1a3a5c;
        border-left: 5px solid #e63946;
        padding-left: 1rem;
        margin-bottom: 0.3rem;
    }
    .subtitle {
        color: #6b7c93;
        font-size: 0.9rem;
        margin-bottom: 2rem;
        padding-left: 1.3rem;
    }
    .metric-card {
        background: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 6px;
        padding: 1rem;
        text-align: center;
    }
    .metric-val { font-size: 2rem; font-weight: 600; font-family: 'IBM Plex Mono', monospace; }
    .metric-lbl { font-size: 0.75rem; color: #6b7c93; text-transform: uppercase; letter-spacing: 0.05em; }
    .err-critical { color: #e63946; }
    .err-warn { color: #f4a261; }
    .ok { color: #2a9d8f; }
    .section-header {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.85rem;
        font-weight: 600;
        color: #1a3a5c;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        border-bottom: 2px solid #1a3a5c;
        padding-bottom: 0.3rem;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# ─── En-tête ────────────────────────────────────────────────────────────────
st.markdown('<div class="main-title">🛡️ Validateur de données Réseaux</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">Communauté d\'Agglomération Saint-Lô Agglo — Contrôle qualité SIG</div>', unsafe_allow_html=True)

# ─── Règles de validation ────────────────────────────────────────────────────

# Colonnes obligatoires par type de table (détectées via le nom de la couche)
MANDATORY_COLS = {
    # Colonnes universelles
    "_default": ["id_0", "geom"],
    # Canalisations linéaires
    "_cana": ["id_0", "geom", "gid", "id", "annee_pose", "diametre", "longueur", "materiau", "exploitant", "prec_clas"],
    # Points / regards
    "_regard": ["id_0", "geom", "gid", "id", "annee_pose", "z_tn", "z_radier", "profondeur"],
    # Branchements
    "_branch": ["id_0", "geom", "gid", "id", "annee_pose", "diametre", "longueur"],
    # Éclairage public
    "_ep_obj": ["id", "geom", "annee_pose", "exploitant", "prec_clas"],
}

# Matériaux autorisés selon le réseau
MATERIAUX_AEP = {'PVC', 'Inconnu', 'Fonte ductile', 'Acier', 'CPV', 'PEHD', 'Eternit'}
MATERIAUX_EU  = {'PVC', 'Inconnu', 'Fonte ductile', 'Beton arme', 'Fonte'}
MATERIAUX_EP  = {'PVC', 'Inconnu', 'Fonte ductile', 'Acier', 'CPV', 'PEHD', 'Eternit',
                 'Polypro', 'Gres', 'Amiante', 'Beton arme'}

# Exploitants autorisés selon le réseau
EXPLOITANTS_AEP = {'Veolia', 'Saur', 'Regie'}
EXPLOITANTS_EU  = {'Saur', 'Regie'}
EXPLOITANTS_EP  = {'Regie'}
EXPLOITANTS_ECLAIRAGE = {'Regie'}


def detect_network(layer_name: str) -> str:
    """Détecte le type de réseau depuis le nom de la couche."""
    name = layer_name.lower()
    if 'eclairagepublic' in name or 'eclairage' in name:
        return 'eclairage'
    if '_aep_' in name:
        return 'aep'
    if '_eu_' in name:
        return 'eu'
    if '_ep_' in name:
        return 'ep'
    return 'inconnu'


def get_id_col(gdf: gpd.GeoDataFrame) -> str:
    """Retourne la colonne d'identifiant principale."""
    for col in ['id_0', 'gid', 'fid']:
        if col in gdf.columns:
            return col
    return gdf.index.name or 'index'


def check_layer(layer_name: str, gdf: gpd.GeoDataFrame) -> list[dict]:
    """Applique toutes les règles de contrôle sur une couche GDF."""
    errors = []
    network = detect_network(layer_name)
    id_col = get_id_col(gdf)

    def add_errors(mask, message):
        subset = gdf[mask]
        for _, row in subset.iterrows():
            obj_id = str(row[id_col]) if id_col in row.index else str(row.name)
            errors.append({
                "table": layer_name,
                "id_objet": obj_id,
                "type_erreur": message,
                "réseau": network.upper()
            })

    # ── 0. Géométrie nulle ────────────────────────────────────────────────
    if 'geometry' in gdf.columns or gdf.geometry is not None:
        try:
            add_errors(gdf.geometry.is_empty | gdf.geometry.isna(), "Valeur nulle : geom")
        except Exception:
            pass

    # ── 1. Valeurs nulles sur colonnes clés ───────────────────────────────
    key_cols = ['id_0', 'gid', 'id', 'annee_pose', 'diametre', 'longueur',
                'z_tn', 'z_radier', 'profondeur', 'materiau', 'exploitant', 'prec_clas']
    for col in key_cols:
        if col in gdf.columns:
            add_errors(gdf[col].isna(), f"Valeur nulle : {col}")

    # Colonne spéciale millesime (compteurs)
    if 'millesime' in gdf.columns:
        add_errors(gdf['millesime'].isna(), "Valeur nulle : millesime")

    # Colonne annee_abdn (canalisations abandonnées EU)
    if 'annee_abdn' in gdf.columns:
        add_errors(gdf['annee_abdn'].isna(), "Valeur nulle : annee_abdn")

    # ── 2. Classe de précision (A, B ou C) ───────────────────────────────
    prec_col = None
    if 'prec_clas' in gdf.columns:
        prec_col = 'prec_clas'
    elif 'prec_class' in gdf.columns:
        prec_col = 'prec_class'

    if prec_col:
        valid_prec = {'A', 'B', 'C'}
        mask_invalid_prec = gdf[prec_col].notna() & ~gdf[prec_col].isin(valid_prec)
        add_errors(mask_invalid_prec, f"Classe de précision invalide (doit être A, B ou C) : valeur={gdf.loc[mask_invalid_prec, prec_col].tolist()}"[:80])

        # Classe A obligatoire après 2025
        if 'annee_pose' in gdf.columns:
            try:
                annee = pd.to_numeric(gdf['annee_pose'], errors='coerce')
                mask_post2025 = (annee > 2025) & (gdf[prec_col] != 'A')
                add_errors(mask_post2025, "Classe de précision <> A (post 2025)")
            except Exception:
                pass

    # ── 3. Année de pose en 4 chiffres ───────────────────────────────────
    if 'annee_pose' in gdf.columns:
        try:
            annee = pd.to_numeric(gdf['annee_pose'], errors='coerce')
            mask_bad_year = gdf['annee_pose'].notna() & (
                annee.isna() | (annee < 1000) | (annee > 9999)
            )
            add_errors(mask_bad_year, "Année de pose invalide (doit être sur 4 chiffres)")
        except Exception:
            pass

    # ── 4. Exploitant ────────────────────────────────────────────────────
    if 'exploitant' in gdf.columns:
        if network == 'aep':
            valid_exp = EXPLOITANTS_AEP
        elif network == 'eu':
            valid_exp = EXPLOITANTS_EU
        elif network in ('ep', 'eclairage'):
            valid_exp = EXPLOITANTS_EP
        else:
            valid_exp = EXPLOITANTS_AEP | EXPLOITANTS_EU  # permissif si inconnu

        mask_bad_exp = gdf['exploitant'].notna() & ~gdf['exploitant'].isin(valid_exp)
        add_errors(mask_bad_exp, f"Valeur non conforme : exploitant (valeurs autorisées : {', '.join(sorted(valid_exp))})")

    # ── 5. Matériaux ─────────────────────────────────────────────────────
    if 'materiau' in gdf.columns:
        if network == 'aep':
            valid_mat = MATERIAUX_AEP
        elif network == 'eu':
            valid_mat = MATERIAUX_EU
        elif network == 'ep':
            valid_mat = MATERIAUX_EP
        else:
            valid_mat = MATERIAUX_AEP | MATERIAUX_EU | MATERIAUX_EP

        mask_bad_mat = gdf['materiau'].notna() & ~gdf['materiau'].isin(valid_mat)
        add_errors(mask_bad_mat, f"Matériau non conforme (valeurs autorisées : {', '.join(sorted(valid_mat))})")

    return errors


# ─── Interface upload ────────────────────────────────────────────────────────
st.markdown('<div class="section-header">📂 Chargement du fichier</div>', unsafe_allow_html=True)

uploaded_file = st.file_uploader(
    "Glissez-déposez un GeoPackage (.gpkg) ou un Shapefile compressé (.zip)",
    type=['gpkg', 'zip'],
    help="Pour un Shapefile, zipper ensemble les fichiers .shp, .dbf, .shx et .prj"
)

if uploaded_file:
    try:
        file_bytes = uploaded_file.read()
        file_name  = uploaded_file.name.lower()

        layers_data = {}  # {nom_couche: GeoDataFrame}

        # ── Lecture GPKG (peut contenir plusieurs couches) ────────────────
        if file_name.endswith('.gpkg'):
            import fiona
            available_layers = fiona.listlayers(io.BytesIO(file_bytes))
            if len(available_layers) > 1:
                selected_layers = st.multiselect(
                    f"Ce GeoPackage contient {len(available_layers)} couche(s). Sélectionnez celles à valider :",
                    available_layers,
                    default=available_layers
                )
            else:
                selected_layers = available_layers

            for lyr in selected_layers:
                gdf = gpd.read_file(io.BytesIO(file_bytes), layer=lyr)
                layers_data[lyr] = gdf

        # ── Lecture ZIP (Shapefile) ───────────────────────────────────────
        elif file_name.endswith('.zip'):
            with tempfile.TemporaryDirectory() as tmpdir:
                with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
                    zf.extractall(tmpdir)
                shp_files = [f for f in os.listdir(tmpdir) if f.endswith('.shp')]
                if not shp_files:
                    st.error("Aucun fichier .shp trouvé dans le ZIP.")
                    st.stop()
                for shp in shp_files:
                    layer_name = os.path.splitext(shp)[0]
                    gdf = gpd.read_file(os.path.join(tmpdir, shp))
                    layers_data[layer_name] = gdf

        if not layers_data:
            st.warning("Aucune couche n'a pu être chargée.")
            st.stop()

        # ─── Résumé du fichier ───────────────────────────────────────────
        total_features = sum(len(g) for g in layers_data.values())
        st.markdown('<div class="section-header">📊 Aperçu</div>', unsafe_allow_html=True)

        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(f'<div class="metric-card"><div class="metric-val">{len(layers_data)}</div><div class="metric-lbl">Couche(s)</div></div>', unsafe_allow_html=True)
        with col2:
            st.markdown(f'<div class="metric-card"><div class="metric-val">{total_features:,}</div><div class="metric-lbl">Entités totales</div></div>', unsafe_allow_html=True)

        # ─── Validation ──────────────────────────────────────────────────
        all_errors = []
        progress = st.progress(0, text="Validation en cours…")

        for i, (layer_name, gdf) in enumerate(layers_data.items()):
            errs = check_layer(layer_name, gdf)
            all_errors.extend(errs)
            progress.progress((i + 1) / len(layers_data), text=f"Validation : {layer_name}")

        progress.empty()

        df_errors = pd.DataFrame(all_errors) if all_errors else pd.DataFrame(
            columns=["table", "id_objet", "type_erreur", "réseau"]
        )

        with col3:
            color_class = "err-critical" if len(df_errors) > 0 else "ok"
            st.markdown(f'<div class="metric-card"><div class="metric-val {color_class}">{len(df_errors)}</div><div class="metric-lbl">Erreurs détectées</div></div>', unsafe_allow_html=True)

        # ─── Résultats ───────────────────────────────────────────────────
        st.markdown('<div class="section-header">🔍 Résultats du contrôle qualité</div>', unsafe_allow_html=True)

        if df_errors.empty:
            st.success("✅ Aucune erreur détectée — toutes les règles sont respectées.")
        else:
            st.error(f"⚠️ {len(df_errors)} erreur(s) détectée(s) sur {df_errors['table'].nunique()} couche(s).")

            # Filtres
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                filtre_table = st.multiselect("Filtrer par couche", sorted(df_errors['table'].unique()))
            with col_f2:
                filtre_type  = st.multiselect("Filtrer par type d'erreur", sorted(df_errors['type_erreur'].unique()))

            df_view = df_errors.copy()
            if filtre_table:
                df_view = df_view[df_view['table'].isin(filtre_table)]
            if filtre_type:
                df_view = df_view[df_view['type_erreur'].isin(filtre_type)]

            st.dataframe(
                df_view.reset_index(drop=True),
                use_container_width=True,
                height=420,
                column_config={
                    "table":       st.column_config.TextColumn("Couche",        width="medium"),
                    "réseau":      st.column_config.TextColumn("Réseau",        width="small"),
                    "id_objet":    st.column_config.TextColumn("ID Objet",      width="small"),
                    "type_erreur": st.column_config.TextColumn("Type d'erreur", width="large"),
                }
            )

            # Synthèse par couche
            st.markdown('<div class="section-header">📈 Synthèse par couche</div>', unsafe_allow_html=True)
            synthese = (df_errors.groupby(['réseau', 'table', 'type_erreur'])
                        .size().reset_index(name='nb_erreurs')
                        .sort_values(['réseau', 'nb_erreurs'], ascending=[True, False]))
            st.dataframe(synthese, use_container_width=True, height=300)

            # Export CSV
            st.markdown('<div class="section-header">💾 Export</div>', unsafe_allow_html=True)
            csv = df_errors.to_csv(index=False, sep=';', encoding='utf-8-sig')
            st.download_button(
                label="⬇️ Télécharger le rapport CSV",
                data=csv,
                file_name="rapport_controle_qualite_saintlo.csv",
                mime="text/csv"
            )

        # Aperçu des données
        with st.expander("👁️ Aperçu des données chargées"):
            layer_choice = st.selectbox("Couche à prévisualiser", list(layers_data.keys()))
            st.dataframe(layers_data[layer_choice].drop(columns='geometry', errors='ignore').head(50),
                         use_container_width=True)

    except Exception as e:
        st.error(f"Erreur lors de la lecture du fichier : {e}")
        st.info("Conseil : pour un Shapefile, zippez ensemble les fichiers .shp, .dbf, .shx et .prj.")

else:
    st.info("👆 Chargez un fichier GeoPackage (.gpkg) ou un Shapefile compressé (.zip) pour démarrer le contrôle.")

# ─── Aide / Documentation des règles ────────────────────────────────────────
with st.expander("📋 Règles de contrôle appliquées"):
    st.markdown("""
| # | Règle | Couches concernées |
|---|-------|-------------------|
| 0 | Géométrie non nulle | Toutes |
| 1 | Valeurs nulles sur champs clés (`id_0`, `gid`, `id`, `geom`, `annee_pose`, `diametre`, `longueur`, `z_tn`, `z_radier`, `profondeur`) | Selon couche |
| 2 | Classe de précision dans {A, B, C} | Toutes les canalisations + câbles EP |
| 2b | Classe de précision = A obligatoire pour les objets posés après 2025 | Idem |
| 3 | Année de pose sur 4 chiffres | Toutes les couches avec `annee_pose` |
| 4 | Exploitant conforme au réseau (AEP : veolia/saur/regie — EU : saur/regie — EP+Éclairage : regie) | Toutes |
| 5 | Matériau conforme au référentiel du réseau | Canalisations |
    """)
