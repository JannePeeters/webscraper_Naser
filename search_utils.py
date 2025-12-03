import streamlit as st
import requests
import time
import numpy as np
import unidecode
import re
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from geopy.distance import geodesic
from datetime import datetime
import pandas as pd
from urllib.parse import urlparse

API_KEY = st.secrets["google"]["places_api_key"]
COMMON_PATHS = ["/contact", "/contact-us", "/contacten", "/about", "/over-ons", "/impressum", "/contact.html"]

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
                results[domain] = future.result()
            except Exception:
                results[domain] = None
    return results

def run_search(search_option, category_input, place_input, clicked_location, radius_m):
    st.info(f"Zoeken... Even geduld alsjeblieft :)")
    data_list = []
    seen_place_ids = set()

    if search_option == "Categorie en plaats typen":
        query = f"{category_input} in {place_input}"
        results = google_places_search(query=query)
        input_text = f"Getypt: {category_input} in {place_input}"
        filename = f"{category_input}_{place_input}.xlsx".replace(" ", "_")
    else:  # Kaart + radius
        if not clicked_location:
            st.warning("Klik eerst op de kaart om een locatie te selecteren!")
            return None, None, None

        lat, lon = clicked_location
        step_m = min(radius_m // 3, 1000)
        grid_points = generate_grid(lat, lon, radius_m, step_m=step_m)

        results = []
        for g_lat, g_lon in grid_points:
            batch = google_places_search(query=category_input, location=(g_lat, g_lon), radius=radius_m)
            for result in batch:
                pid = result.get("place_id")
                if pid and pid not in seen_place_ids:
                    seen_place_ids.add(pid)
                    results.append(result)

        input_text = f"Kaart: {category_input} in {lat:.5f}, {lon:.5f} (radius {radius_m} m)"
        filename = f"{category_input}_{f'{lat}_{lon}'}.xlsx".replace(" ", "_")

    if not results:
        return None, input_text, filename

    if search_option == "Categorie typen en plaats selecteren op kaart":
        center = clicked_location
        results = [
            result for result in results
            if (loc := result.get("geometry", {}).get("location", {}))
            and geodesic(center, (loc["lat"], loc["lng"])).meters <= radius_m
        ]

    for result in results:
        place_id = result.get("place_id")
        details = get_place_details(place_id) if place_id else {}

        if search_option == "Categorie en plaats typen":
            if not address_matches_place(details.get("formatted_address", ""), place_input):
                continue

        loc = result.get("geometry", {}).get("location", {})
        data_list.append({
            "Input": input_text,
            "Naam": details.get("name") or result.get("name") or None,
            "Adres": details.get("formatted_address") or None,
            "Latitude": loc.get("lat") or None,
            "Longitude": loc.get("lng") or None,
            "Telefoon": details.get("formatted_phone_number") or None,
            "Website": details.get("website") or None,
            "E-mail": None,  # placeholder
            "Status": "Nieuw",  # default
            "Datum": datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        })

    df = pd.DataFrame(data_list)

    # E-mails ophalen
    if not df.empty and "Website" in df.columns:
        websites = [w for w in df["Website"].dropna().unique()]
        if websites:
            emails_map = fetch_emails(websites)
            df["E-mail"] = df["Website"].apply(
                lambda w: emails_map.get(urlparse(w).netloc) if pd.notna(w) else None
            )

    # Resultaten opslaan for map markers
    # st.session_state.last_results = df.to_dict(orient="records")

    return df, input_text, filename
