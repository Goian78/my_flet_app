import flet as ft
import pandas as pd
import numpy as np
import sqlite3
from geopy.distance import geodesic
from opencage.geocoder import OpenCageGeocode
import requests

def get_coordinates(address, api_key):
    geocoder = OpenCageGeocode(api_key)
    result = geocoder.geocode(address)
    if result and len(result):
        return result[0]['geometry']['lat'], result[0]['geometry']['lng']
    return None, None

def estimate_property(e):
    adresse = adresse_input.value.lower()
    code_postal = code_postal_input.value
    ville = ville_input.value.lower()
    surface_habitable = surface_habitable_input.value
    nature = nature_input.value.strip().lower()
    surface_terrain = surface_terrain_input.value if nature == "maison" else 0
    piscine = "oui" if piscine_input.value else "non"

    try:
        surface_habitable = float(surface_habitable)
        if nature == "maison":
            surface_terrain = float(surface_terrain)
    except ValueError:
        result_text.value = "Veuillez entrer des valeurs numériques valides."
        page.update()
        return

    api_key = 'f116c16163d14e3b908ebfe569b6e04f'  # Remplacez par votre clé API OpenCage
    full_address = f"{adresse}, {code_postal}, {ville}, FR"
    latitude, longitude = get_coordinates(full_address, api_key)

    if latitude is None or longitude is None:
        result_text.value = "Impossible de trouver les coordonnées pour l'adresse donnée."
        page.update()
        return

    # Simulez l'appel à la fonction main avec des résultats fictifs pour le test
    result = (200000, 250000)  # Valeurs fictives pour l'exemple
    if result:
        b_esti, h_esti = result
        result_text.value = f"Estimation finale : entre {b_esti} et {h_esti} euros."
    else:
        result_text.value = "Impossible de trouver des biens comparables ou de faire une estimation."
    
    page.update()

def main_flet(page: ft.Page):
    global adresse_input, code_postal_input, ville_input, surface_habitable_input, nature_input, surface_terrain_input, piscine_input, result_text

    adresse_input = ft.TextField(label="Adresse")
    code_postal_input = ft.TextField(label="Code Postal")
    ville_input = ft.TextField(label="Ville")
    surface_habitable_input = ft.TextField(label="Surface Habitable")
    nature_input = ft.RadioGroup(
        content=ft.Column([
            ft.Radio(value="maison", label="Maison"),
            ft.Radio(value="appartement", label="Appartement"),
        ]),
        value="maison"
    )
    surface_terrain_input = ft.TextField(label="Surface du Terrain", visible=True)
    piscine_input = ft.Checkbox(label="Piscine")
    result_text = ft.Text()

    nature_input.on_change = lambda e: surface_terrain_input.update(visible=nature_input.value == "maison")
    estimate_button = ft.ElevatedButton(text="Estimer", on_click=estimate_property)

    page.add(
        ft.Column([
            adresse_input,
            code_postal_input,
            ville_input,
            surface_habitable_input,
            nature_input,
            surface_terrain_input,
            piscine_input,
            estimate_button,
            result_text
        ])
    )

ft.app(target=main_flet)
