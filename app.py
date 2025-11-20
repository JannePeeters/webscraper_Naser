import streamlit as st
import pandas as pd
import requests
from io import BytesIO
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin
import unidecode
import re
from datetime import datetime
import gspread
from Tools.scripts.dutree import display
from google.oauth2.service_account import Credentials
from streamlit_folium import st_folium
import folium
import numpy as np
from geopy.distance import geodesic

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
COMMON_PATHS = ["/contact", "/contact-us", "/contacten", "/about", "/over-ons", "/impressum", "/contact.html"]

st.title("Webscraper via Google Places API")
st.write("Zoek automatisch bedrijven!")

# --- Search reset ---
if st.button("Nieuwe zoekopdracht"):
    st.session_state.scrape_results = None
    if "clicked_location" in st.session_state:
        del st.session_state.clicked_location
    if "map_bounds" in st.session_state:
        del st.session_state.map_bounds
    st.rerun() # Refresh de app voor nieuwe search

# --- Input ---
search_option = st.radio(
    "Kies zoekmethode",
    ("Typen: categorie + plaats", "Kaart + radius")
)

category_input = st.text_input("Categorie (bijv. restaurant, cafe, hotel)", "Restaurant")

if search_option == "Typen: categorie + plaats":
    place_input = st.text_input("Plaatsnaam (bijv. Nijmegen, Oosterhout)", "Nijmegen")
else:
    st.write("Klik op de kaart om het centrum van je zoekgebied te selecteren.")

    # Radius slider
    radius_m = st.slider("Straal (meters)", 100, 5000, 1000)

# --- API key ---
API_KEY = st.secrets["google"]["places_api_key"]

# --- Google Sheets setup ---
creds = Credentials.from_service_account_info(st.secrets["gspread"], scopes=SCOPES)
google_client = gspread.authorize(creds)
SHEET_NAME = "ScraperResults"

# --- Helper functions ---
def get_place_details(place_id):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "name,formatted_address,formatted_phone_number,website",
        "key": API_KEY
    }
    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        if data.get("status") != "OK":
            st.warning(f"Place Details failed: {data.get('status')} - {data.get('error_message')}")
        return data.get("result", {})
    except Exception as e:
        st.warning(f"Fout bij ophalen gegevens: {e}")
        return {}

def address_matches_place(address, place):
    """Check of de ingevoerde plaatsnaam in het adres voorkomt"""
    if not address or not place:
        return False

    address_norm = unidecode.unidecode(address.lower())
    place_norm = unidecode.unidecode(place.lower())

    # Splits aders in woorden
    tokens = re.findall(r"[a-zA-Z]+", address_norm)

    # Exacte woordmatch of plaatsnaam gevolgd door komma
    if place_norm in tokens or f"{place_norm}," in address_norm:
        return True

    return False

def generate_grid(center_lat, center_lon, radius_m, step_m=500):
    """
    Genereer een raster/grid rond het centrum
    radius_m: totale radius
    step_m: stap tussen cirkels
    """
    step_deg_lat = step_m / 111_000 # 1° ≈ 111 km
    step_deg_lon = step_m / (111_000 * np.cos(np.radians(center_lat)))

    lat_min = center_lat - radius_m / 111_000
    lat_max = center_lat + radius_m / 111_000
    lon_min = center_lon - radius_m / (111_000 * np.cos(np.radians(center_lat)))
    lon_max = center_lon + radius_m / (111_000 * np.cos(np.radians(center_lat)))

    lats = np.arange(lat_min, lat_max, step_deg_lat)
    lons = np.arange(lon_min, lon_max, step_deg_lon)

    return [(lat, lon) for lat in lats for lon in lons]

def find_email_on_url(url):
    try:
        response = requests.get(url, timeout=6, headers={"User-Agent": "Mozilla/5.0"})
        if response.status_code != 200:
            return None

        # mailto first
        m = re.search(r'href=["\']mailto:([^"\']+)["\']', response.text, flags=re.I)
        if m:
            return m.group(1).split("?")[0]

        # Regex fallback
        emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}", response.text)
        return emails[0] if emails else None
    except Exception:
        return None

def find_email_for_domain(base_url):
    parsed = urlparse(base_url)
    base = f"{parsed.scheme}://{parsed.netloc}/"

    # Try common paths first
    for path in COMMON_PATHS:
        email = find_email_on_url(urljoin(base, path))
        if email:
            return email

    # Try homepage
    return find_email_on_url(base)

def fetch_emails(urls, max_workers=8):
    """Fetch emails in parallel"""
    domain_map = {}
    for url in urls:
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
            if domain:
                domain_map.setdefault(domain, url)
        except Exception:
            continue

    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(find_email_for_domain, url): domain for domain, url in domain_map.items()}
        for future in as_completed(futures):
            domain = futures[future]
            try:
                email = future.result()
            except Exception:
                email = None
            results[domain] = email
    return results

def normalise_for_compare(value):
    if pd.isna(value) or value in [None, "None", "", "nan", "NaN"]:
        return ""
    return str(value)

def upload_to_google_sheets(df, category_input, input):
    """
    Update Google Sheets met nieuwe search results.
      - Vergelijk nieuwe met oude entries voor dezelfde zoekcontext + categorie (+ plaats).
      - Detecteert nieuwe, gewijzigde, ongewijzigde (en verdwenen) bedrijven.
      - Past automatisch de Status en Datum aan.
    Returnt altijd de resultaten van de huidige search voor Streamlit + Excel-download.
    """
    try:
        # Verbinding maken met Google Sheets
        sheet = google_client.open_by_key("1tZNnGy-KBW0LdnmzqDbKGgkQ1I8wM7_rf5qnbAkTy0s")
        worksheet = sheet.sheet1

        # Bestaande data ophalen
        existing_records = worksheet.get_all_records()
        df_existing = pd.DataFrame(existing_records) if existing_records else pd.DataFrame(columns=df.columns)

        # Belangrijke kolommen voor vergelijking (alles behalve Status en Datum)
        compare_cols = ["Naam", "Adres", "Telefoon", "Website", "E-mail"]

        # Zorg dat alle belangrijke kolommen bestaan in beide dataframes
        for col in compare_cols + ["Input", "Latitude", "Longitude", "Status", "Datum"]:
            if col not in df_existing.columns:
                df_existing[col] = None
            if col not in df.columns:
                df[col] = None

        # Filter bestaande data op dezelfde zoekcontext en bepaal de nuttige kolommen voor display/download
        if input.startswith("Getypt:"):
            mask = df_existing["Input"].fillna("").str.lower() == input.lower()
            display_cols = ["Naam", "Adres", "Telefoon", "Website", "E-mail"]
        else:
            # Kaart search: filter op categorie alleen
            # Haal categorie uit input (bijv. "Kaart: Restaurant in 52.0, 5.0 (radius 1000 m)")
            mask = df_existing["Input"].fillna("").str.lower().str.startswith(f"kaart: {category_input.lower()}")
            display_cols = ["Naam", "Adres", "Latitude", "Longitude", "Telefoon", "Website", "E-mail"]
        df_existing_search = df_existing[mask].copy()
        updated_sheet = df_existing.copy()

        # Als er geen eerdere resultaten zijn voor deze input, voeg alles nieuw toe
        if df_existing_search.empty:
            updated_sheet = pd.concat([updated_sheet, df], ignore_index=True)
            worksheet.clear()
            worksheet.update([updated_sheet.columns.tolist()] + updated_sheet.values.tolist())
            st.success("Resultaten geüpload naar Google Sheets!")
            return df[[c for c in display_cols if c in df.columns]]

        # Zorg dat vergelijkingskolommen altijd strings zijn, en normaliseer lege waarden
        df_existing_search[compare_cols] = df_existing_search[compare_cols].applymap(normalise_for_compare)
        df[compare_cols] = df[compare_cols].applymap(normalise_for_compare)

        # Maak hulplijsten voor vergelijking
        existing_tuples = df_existing_search[compare_cols].apply(tuple, axis=1).tolist()
        new_tuples = df[compare_cols].apply(tuple, axis=1).tolist()

        # Counters voor diff
        nieuw_count = gewijzigd_count = ongewijzigd_count = verwijderd_count = 0

        # Loop door nieuwe resultaten
        for i, new_row in df.iterrows():
            new_tuple = tuple(new_row[col] for col in compare_cols)

            if new_tuple in existing_tuples:
                # Exact dezelfde rij bestaat al dus bestaande (oude) entry krijg datumupdate
                ongewijzigd_count += 1
                idx = df_existing_search.index[existing_tuples.index(new_tuple)]
                global_idx = updated_sheet.index.get_loc(idx)
                updated_sheet.loc[global_idx, "Datum"] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            else:
                # Check op gedeeltelijke match (2+ overeenkomende kolommen)
                match_found = False
                for j, old_row in df_existing_search.iterrows():
                    matches = sum(
                        1 for col in compare_cols
                        if pd.notna(new_row[col]) and str(new_row[col]).strip().lower() == str(old_row[col]).strip().lower()
                    )
                    if matches >= 2: # Gedeeltelijke overeenkomst dus bestaande (oude) entry krijgt statuswijziging en datumupdate
                        match_found = True
                        global_idx = updated_sheet.index.get_loc(j)
                        updated_sheet.loc[global_idx, "Status"] = "CHECKEN: verouderde gegevens?"
                        updated_sheet.loc[global_idx, "Datum"] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                        break

                if match_found:
                    gewijzigd_count += 1
                    # Nieuwe geüpdate entry toevoegen
                    new_row["Status"] = "CHECKEN: huidige gegevens?"
                    updated_sheet = pd.concat([updated_sheet, pd.DataFrame([new_row])], ignore_index=True)
                else:
                    nieuw_count += 1
                    # Volledig nieuwe entry toevoegen; status staat al op "Nieuw"
                    updated_sheet = pd.concat([updated_sheet, pd.DataFrame([new_row])], ignore_index=True)

        # Oude resultaten die niet meer voorkomen in de nieuwe search (alleen bij getypte searches)
        if input.startswith("Getypt:"):
            for j, old_row in df_existing_search.iterrows():
                old_tuple = tuple(old_row[col] for col in compare_cols)
                if old_tuple not in new_tuples:
                    verwijderd_count += 1
                    global_idx = updated_sheet.index.get_loc(j)
                    updated_sheet.loc[global_idx, "Status"] = "Niet meer actief"
                    updated_sheet.loc[global_idx, "Datum"] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

        # Upload terug naar Google Sheets
        worksheet.clear()
        worksheet.update([updated_sheet.columns.tolist()] + updated_sheet.values.tolist())

        st.success("Resultaten geüpload naar Google Sheets!")

        return df[[c for c in display_cols if c in df.columns]]

    except Exception as e:
        st.error(f"Fout bij uploaden naar Google Sheets: {e}")
        return df # Fallback zodat iets terugkomt

def google_places_search(query=None, location=None, radius=None):
    results = []
    next_page_token = None
    pages_checked = 0
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json" if query else "https://maps.googleapis.com/maps/api/place/nearbysearch/json"

    while True:
        params = {"key": API_KEY}
        if query:
            params["query"] = query
        if location:
            params["location"] = f"{location[0]},{location[1]}"
            params["radius"] = radius
        if next_page_token:
            params["page_token"] = next_page_token

        try:
            response = requests.get(url, params=params, timeout=30)
            data = response.json()
            results.extend(data.get("results", []))
            next_page_token = data.get("nextPageToken")
            pages_checked += 1
            if not next_page_token or pages_checked >= 3:
                break
            time.sleep(2)
        except Exception as e:
            st.warning(f"Fout bij API-call: {e}")
            break

    return results

# --- Kaart initiëren ---
if search_option == "Kaart + radius":
    if "map_center" not in st.session_state:
        st.session_state.map_center = [52.0, 5.0]

    m = folium.Map(location=st.session_state.map_center, zoom_start=8)

    # Herstel bounds als eerder gebruikt
    if st.session_state.get("map_bounds"):
        m.fit_bounds([
            [st.session_state.map_bounds["south"], st.session_state.map_bounds["west"]],
            [st.session_state.map_bounds["north"], st.session_state.map_bounds["east"]]
        ])

    # Marker + cirkel
    if "clicked_location" in st.session_state:
        folium.Marker(
            location=st.session_state.clicked_location,
            icon=folium.Icon(icon="map-pin", prefix='fa', color='red'),
            popup="Geselecteerd centrum"
        ).add_to(m)
        folium.Circle(
            location=st.session_state.clicked_location,
            radius=radius_m,
            color="blue",
            fill=True,
            fill_opacity=0.2
        ).add_to(m)

    map_data = st_folium(m, width=700, height=500, returned_objects=["last_clicked", "bounds"])

    # Klik en bounds opslaan
    if map_data:
        bounds = map_data.get("bounds")
        if bounds:
            ne = bounds.get("northeast") or bounds.get("northEast")
            sw = bounds.get("southwest") or bounds.get("southWest")
            if ne and sw:
                st.session_state.map_bounds = {
                    "north": ne["lat"],
                    "east": ne["lng"],
                    "south": sw["lat"],
                    "west": sw["lng"]
                }
        if map_data.get("last_clicked"):
            st.session_state.clicked_location = [
                map_data["last_clicked"]["lat"],
                map_data["last_clicked"]["lng"]
            ]

# --- Search query ---
if st.button("Zoek"):
    st.info(f"Zoeken... Even geduld alsjeblieft :)")
    data_list = []
    seen_place_ids = set()

    if search_option == "Typen: categorie + plaats":
        query = f"{category_input} in {place_input}"
        results = google_places_search(query=query)
        input = f"Getypt: {category_input} in {place_input}"
        filename = f"{category_input}_{place_input}.xlsx".replace(" ", "_")
    else:
        # Kaart + radius
        if "clicked_location" not in st.session_state:
            st.warning("Klik eerst op de kaart om een locatie te selecteren!")
            st.stop()
        lat, lon = st.session_state.clicked_location
        step_m = min(radius_m//3, 1000)
        grid_points = generate_grid(lat, lon, radius_m, step_m=step_m)
        results = []
        for g_lat, g_lon in grid_points:
            batch = google_places_search(query=category_input, location=(g_lat, g_lon), radius=radius_m)
            for result in batch:
                pid = result.get("place_id")
                if pid not in seen_place_ids:
                    seen_place_ids.add(pid)
                    results.append(result)
        input = f"Kaart: {category_input} in {lat:.5f}, {lon:.5f} (radius {radius_m} m)"
        filename = f"{category_input}_{f'{lat}_{lon}'}.xlsx".replace(" ", "_")

    if not results:
        st.warning("Geen resultaten gevonden.")
    else:
        if search_option == "Kaart + radius":
            center = st.session_state.clicked_location
            filtered_results = []
            for result in results:
                loc = result.get("geometry", {}).get("location", {})
                if loc:
                    # Afstand berekenen van het centrale punt
                    dist = geodesic(center, (loc["lat"], loc["lng"])).meters
                    if dist <= radius_m:
                        filtered_results.append(result)
            results = filtered_results
        for result in results:
            place_id = result.get("place_id")
            details = get_place_details(place_id)

            if search_option == "Typen: categorie + plaats":
                if not address_matches_place(details.get("formatted_address", ""), place_input):
                    continue

            loc = result.get("geometry", {}).get("location", {})
            data_list.append({
                "Input": input,
                "Naam": details.get("name") or None,
                "Adres": details.get("formatted_address") or None,
                "Latitude": loc.get("lat") or None,
                "Longitude": loc.get("lng") or None,
                "Telefoon": details.get("formatted_phone_number") or None,
                "Website": details.get("website") or None,
                "E-mail": None, # placeholder
                "Status": "Nieuw", # default
                "Datum": datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            })

        df = pd.DataFrame(data_list)

        # E-mails ophalen
        if not df.empty and "Website" in df.columns:
            websites = [w for w in df["Website"].dropna().unique()]
            if websites:
                emails_map = fetch_emails(websites)
                df["E-mail"] = df["Website"].apply(lambda w: emails_map.get(urlparse(w).netloc) if pd.notna(w) else None)

        # Upload naar Google Sheets en tonen
        df_active = upload_to_google_sheets(df, category_input, input)
        # TODO dit fixen?

        # Excel buffer maken
        excel_buffer = BytesIO()
        df_active.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)

        st.dataframe(df_active)
        st.download_button(
            label="Download Excel-bestand",
            data=excel_buffer.getvalue(),
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )