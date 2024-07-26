import pandas as pd
import numpy as np
import sqlite3
import os
from geopy.distance import geodesic
from opencage.geocoder import OpenCageGeocode
import time
import requests

# URL du fichier CSV des valeurs foncières géolocalisées pour 2023
url_2023 = 'https://files.data.gouv.fr/geo-dvf/latest/csv/2023/full.csv.gz'

# Nom du fichier local pour enregistrer les données de 2023
local_filename_2023 = 'latest_dvf_2023.csv.gz'


# Définir les types de données pour chaque colonne du DataFrame
dtype_dict = {
    "id_mutation": str,
    "date_mutation": str,
    "numero_disposition": str,
    "nature_mutation": str,
    "valeur_fonciere": float,
    "adresse_numero": str,
    "adresse_suffixe": str,
    "adresse_nom_voie": str,
    "adresse_code_voie": str,
    "code_postal": str,
    "code_commune": str,
    "nom_commune": str,
    "code_departement": str,
    "ancien_code_commune": str,
    "ancien_nom_commune": str,
    "id_parcelle": str,
    "ancien_id_parcelle": str,
    "numero_volume": str,
    "lot1_numero": str,
    "lot1_surface_carrez": float,
    "lot2_numero": str,
    "lot2_surface_carrez": float,
    "lot3_numero": str,
    "lot3_surface_carrez": float,
    "lot4_numero": str,
    "lot4_surface_carrez": float,
    "lot5_numero": str,
    "nombre_lots": float,
    "code_type_local": str,
    "type_local": str,
    "surface_reelle_bati": float,
    "nombre_pieces_principales": float,
    "code_nature_culture": str,
    "nature_culture": str,
    "code_nature_culture_speciale": str,
    "nature_culture_speciale": str,
    "surface_terrain": float,
    "longitude": float,
    "latitude": float
}

def download_file(url, local_filename):
    """Télécharge le fichier depuis l'URL et l'enregistre localement."""
    response = requests.get(url)
    with open(local_filename, 'wb') as file:
        file.write(response.content)
    print(f"Fichier téléchargé et enregistré sous {local_filename}")

def load_data_to_db(file_path, dtype_dict, db_path):
    """Charge le fichier CSV dans une base de données SQLite pour un accès plus rapide."""
    conn = sqlite3.connect(db_path)
    df_iter = pd.read_csv(file_path, dtype=dtype_dict, compression='infer', chunksize=100000)
    for i, chunk in enumerate(df_iter):
        chunk.to_sql('dvf', conn, if_exists='append', index=False)
        print(f"Chunk {i+1} loaded into database.")
    conn.close()

def get_coordinates(address, api_key):
    geocoder = OpenCageGeocode(api_key)
    result = geocoder.geocode(address)
    if result and len(result):
        return result[0]['geometry']['lat'], result[0]['geometry']['lng']
    return None, None

def filter_properties(conn, code_postal, ville, surface_habitable, surface_terrain, nature):
    """
    Filtre les propriétés selon les critères donnés, puis affine les résultats en excluant les biens dont la valeur est trop éloignée de la moyenne.
    """
    ville = ville.lower()
    query = """
        SELECT *
        FROM dvf
        WHERE code_postal = ?
        AND LOWER(nom_commune) LIKE ?
        AND LOWER(type_local) LIKE ?
        AND surface_reelle_bati > 0
    """
    params = (code_postal, f"%{ville}%", f"%{nature}")

    # Critères supplémentaires
    if surface_habitable > 0:
        surface_min = surface_habitable * 0.8
        surface_max = surface_habitable * 1.2
        query += " AND surface_reelle_bati BETWEEN ? AND ?"
        params += (surface_min, surface_max)

    if nature == 'maison' and surface_terrain > 0:
        terrain_min = surface_terrain * 0.7
        terrain_max = surface_terrain * 1.3
        query += " AND surface_terrain BETWEEN ? AND ?"
        params += (terrain_min, terrain_max)

    df = pd.read_sql_query(query, conn, params=params)

    # Si aucune propriété n'est trouvée, retourner un DataFrame vide
    if df.empty:
        return df

    # Calculer la moyenne des valeurs foncières
    valeur_moyenne = df['valeur_fonciere'].mean()

    # Appliquer le second filtre pour exclure les biens trop éloignés de la moyenne
    valeur_min = valeur_moyenne * 0.5
    valeur_max = valeur_moyenne * 1.5
    df_filtered = df[(df['valeur_fonciere'] >= valeur_min) & (df['valeur_fonciere'] <= valeur_max)]

    return df_filtered


def filter_properties_no_restrictions(conn, code_postal, ville, nature):
    """Filtre les propriétés sans restriction sauf la surface du terrain > 0."""
    ville = ville.lower()

    query = """
        SELECT *
        FROM dvf
        WHERE code_postal = ?
        AND LOWER(nom_commune) LIKE ?
        AND LOWER(type_local) LIKE ?
        AND surface_reelle_bati > 0
    """
    params = (code_postal, f"%{ville}%", f"%{nature}")

    df = pd.read_sql_query(query, conn, params=params)
    return df.sort_values(by='date_mutation', ascending=False)

def filter_properties_by_distance(df, latitude, longitude, radius):
    """Filtre les propriétés basées sur la distance de l'adresse de l'utilisateur."""
    coords_1 = np.array([latitude, longitude])
    coords_2 = df[['latitude', 'longitude']].values
    distances = np.apply_along_axis(lambda x: geodesic(coords_1, x).meters, 1, coords_2)
    df = df.assign(distance=distances)
    return df[df['distance'] <= radius]

def get_comparable_biens(df, latitude, longitude, perimetre=500, step=500, min_comparables=5, max_perimetre=3000):
    """Trouver des biens comparables dans un périmètre donné et ajuster si nécessaire."""
    while perimetre <= max_perimetre:
        comparables = filter_properties_by_distance(df, latitude, longitude, perimetre)
        if len(comparables) >= min_comparables:
            return comparables, perimetre
        perimetre += step
    return pd.DataFrame(), perimetre

def calculate_value_per_sqm(df_combined, latitude, longitude, max_distance=1500, max_properties=500):
    """Calcule la valeur au mètre carré dans un périmètre donné en excluant les valeurs trop éloignées de la moyenne."""
    df_combined = df_combined.dropna(subset=['latitude', 'longitude'])
    coords_1 = np.array([latitude, longitude])
    coords_2 = df_combined[['latitude', 'longitude']].values
    distances = np.apply_along_axis(lambda x: geodesic(coords_1, x).meters, 1, coords_2)
    df_combined = df_combined.assign(distance=distances)
    within_distance = df_combined[df_combined['distance'] <= max_distance]
    within_distance = within_distance.head(max_properties)

    print("Properties within distance:")
    print(within_distance[['id_mutation', 'valeur_fonciere', 'surface_reelle_bati', 'distance']])
    
    if not within_distance.empty:
        within_distance = within_distance[within_distance['surface_reelle_bati'] > 0]
        within_distance['value_per_sqm'] = within_distance['valeur_fonciere'] / within_distance['surface_reelle_bati']

        # Calculer la valeur moyenne par mètre carré
        mean_value_per_sqm = within_distance['value_per_sqm'].mean()
        print(f"Mean value per sqm: {mean_value_per_sqm}")
        
        # Définir les limites à -50% et +150% de la valeur moyenne
        lower_bound = mean_value_per_sqm * 0.2
        upper_bound = mean_value_per_sqm * 2.5
        print(f"Lower bound: {lower_bound}, Upper bound: {upper_bound}")
        
        # Filtrer les propriétés dans les limites définies
        within_threshold = within_distance[(within_distance['value_per_sqm'] >= lower_bound) & (within_distance['value_per_sqm'] <= upper_bound)]
        
        print("Properties within thresholds:")
        print(within_threshold[['id_mutation', 'value_per_sqm', 'valeur_fonciere', 'surface_reelle_bati']])
        
        if not within_threshold.empty:
            mean_value = within_threshold['value_per_sqm'].mean()
            print(f"Mean value per sqm within thresholds: {mean_value}")
            return mean_value
        else:
            print("No properties within thresholds")
            return None
    else:
        print("No properties within distance")
        return None


def main(adresse, code_postal, ville, surface_habitable, nature, surface_terrain=0, piscine='non'):
    api_key = 'f116c16163d14e3b908ebfe569b6e04f'  # Remplacez par votre clé API OpenCage

    full_address = f"{adresse}, {code_postal}, {ville}, FR"
    latitude, longitude = get_coordinates(full_address, api_key)

    if latitude is None or longitude is None:
        return None

    db_path = 'dvf_data.db'

    if not os.path.exists(db_path):
        download_file(url_2023, local_filename_2023)
        load_data_to_db(local_filename_2023, dtype_dict, db_path)

    conn = sqlite3.connect(db_path)
    df_filtered = filter_properties(conn, code_postal, ville, float(surface_habitable), float(surface_terrain), nature)

    df_filtered = df_filtered.drop_duplicates(subset=['date_mutation', 'id_mutation'])

    if df_filtered.shape[0] >= 5:
        df_filtered['price_per_sqm'] = df_filtered['valeur_fonciere'] / df_filtered['surface_reelle_bati']
        estimated_price = df_filtered['price_per_sqm'].mean() * float(surface_habitable)
        h_esti = estimated_price * 1.05
        b_esti = estimated_price * 0.95
        return round(b_esti), round(h_esti)
    else:
        df_filtered = filter_properties_no_restrictions(conn, code_postal, ville, nature)
        df_filtered = df_filtered.drop_duplicates(subset=['date_mutation', 'id_mutation'])
        if not df_filtered.empty:
            avg_value_per_sqm = calculate_value_per_sqm(df_filtered, latitude, longitude, max_distance=1000)
            if avg_value_per_sqm:
                estimated_value = avg_value_per_sqm * float(surface_habitable)
                if piscine == 'oui':
                    estimated_value = estimated_value * 1.05
                h_esti = estimated_value * 1.05
                b_esti = estimated_value * 0.95
                return round(b_esti), round(h_esti)
    return None

# Test de la fonction principale
if __name__ == "__main__":
    adresse = input("Entrez l'adresse : ").lower()
    code_postal = input("Entrez le code postal : ")
    ville = input("Entrez la ville : ").lower()
    surface_habitable = input("Entrez la surface habitable : ")
    nature = input("Maison ou Appartement ? : ").strip().lower()
    if nature == 'maison':
        surface_terrain = input("Entrez la surface du terrain : ")
        piscine = input("Piscine ? Oui / Non : ").lower()
    else:
        surface_terrain = 0
        piscine = 'non'
    result = main(adresse, code_postal, ville, surface_habitable, nature, surface_terrain, piscine)
    if result:
        print(f"Estimation : entre {result[0]} et {result[1]} euros.")
    else:
        print("Impossible de calculer une estimation.")
