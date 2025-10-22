import streamlit as st
import pandas as pd
import requests
from io import BytesIO
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin

COMMON_PATHS = ["/contact", "/contact-us", "/contacten", "/about", "/over-ons", "/impressum", "/contact.html"]

st.title("Webscraper via Google Places API")
st.write("Zoek automatisch bedrijven en exporteer naar Excel!")

# --- Input ---
category_input = st.text_input("Categorie (bijv. restaurant, cafe, hotel)", "Restaurant")
place_input = st.text_input("Plaatsnaam (bijv. Nijmegen, Oosterhout)", "Nijmegen")
include_email = st.checkbox("Probeer e-mailadressen ook te vinden (dit kan iets langer duren)")

# --- API key ---
API_KEY = st.secrets["google"]["places_api_key"]

# --- Helper functions ---
@st.cache_data
def get_place_details(place_id, api_key):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "name,formatted_address,formatted_phone_number,website",
        "key": API_KEY
    }
    response = requests.get(url, params=params, timeout=10)
    data = response.json()
    if data.get("status") != "OK":
        st.warning(f"Place Details failed: {data.get('status')} - {data.get('error_message')}")
    return data.get("result", {})

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

def fetch_emails_with_progress(urls, progress_bar, max_workers=12):
    """Fetch emails in parallel and update Streamlit progress bar"""
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
        total = len(futures)
        for i, future in enumerate(as_completed(futures)):
            domain = futures[future]
            try:
                email = future.result()
            except Exception:
                email = None
            results[domain] = email
            progress_bar.progress((i+1) / total)
    return results

# --- Search query ---
if st.button("Zoek"):
    st.info(f"Zoeken naar {category_input.capitalize()} in {place_input.title()}...")

    query = f"{category_input} in {place_input}"
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {"query": query, "key": API_KEY}

    results = []
    next_page_token = None
    pages_checked = 0
    progress_bar = st.progress(0)

    # Google places kan meerdere pagina's met resultaten teruggeven
    while True:
        if next_page_token:
            params["pagetoken"] = next_page_token
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        results.extend(data.get("results", []))
        next_page_token = data.get("next_page_token")
        pages_checked += 1
        if not next_page_token or pages_checked >= 3:
            break
        time.sleep(2) # Google eist korte delay tussen pages

    if not results:
        st.warning("Geen resultaten gevonden.")
    else:
        data_list = []

        for i, result in enumerate(results):
            place_id = result.get("place_id")
            details = get_place_details(place_id, API_KEY)

            data_list.append({
                "Naam": details.get("name"),
                "Adres": details.get("formatted_address"),
                "Telefoon": details.get("formatted_phone_number"),
                "Website": details.get("website")
            })
            progress_bar.progress((i+1)/len(results))

        df = pd.DataFrame(data_list)

        if include_email:
            websites = [w for w in df["Website"].dropna().unique()]
            if websites:
                st.info("E-mails ophalen...")
                progress_bar = st.progress(0)
                emails_map = fetch_emails_with_progress(websites, progress_bar)
                df["E-mail"] = df["Website"].apply(lambda w: emails_map.get(urlparse(w).netloc) if pd.notna(w) else None)

        st.success(f"Gevonden: {len(df)} resultaten!")
        st.dataframe(df)

        # Excel export
        filename = f"{category_input.lower()}s_{place_input}.xlsx".replace(" ", "_")
        excel_buffer = BytesIO()
        df.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)

        st.download_button(
            label = "Download Excel-bestand",
            data = excel_buffer,
            file_name = filename,
            mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )


# ***** OSM app *****
# def get_first_available(tags, possible_keys):
#     """Return the first existing tag value from a list of possible keys"""
#     for key in possible_keys:
#         if key in tags and tags[key]:
#             return tags[key]
#     return None
#
# # --- Mapping category ---
# category_map = {
#     "restaurant": ("amenity", "restaurant"),
#     "cafe": ("amenity", "cafe"),
#     "bar": ("amenity", "bar"),
#     "fastfood": ("amenity", "fast_food"),
#     "bank": ("amenity", "bank"),
#     "apotheek": ("amenity", "pharmacy"),
#     "school": ("amenity", "school"),
#     "ziekenhuis": ("amenity", "hospital"),
#
#     "hotel": ("tourism", "hotel"),
#     "hostel": ("tourism", "hostel"),
#     "museum": ("tourism", "museum"),
#     "attractie": ("tourism", "attraction"),
#     "uitzichtpunt": ("tourism", "viewpoint"),
#     "galerie": ("tourism", "galery"),
#
#     "supermarkt": ("shop", "supermarket"),
#     "bakkerij": ("shop", "bakery"),
#     "kledingwinkel": ("shop", "clothes"),
#     "elektronica": ("shop", "electronics"),
#     "winkel": ("shop", "mall")
# }
#
# osm_key, osm_value = category_map.get(category_input.lower(), ("amenity", category_input.lower()))
# osm_tag = f"['{osm_key}'='{osm_value}']"
#
# if st.button("Zoek"):
#     st.info(f"Zoeken naar {category_input.lower()}s in {place_input}...")
#
#     overpass_url = "http://overpass-api.de/api/interpreter"
#     query = f"""
#     [out:json][timeout:25];
#     area[name="{place_input}"]->.searchArea;
#     (
#         node{osm_tag}(area.searchArea);
#         way{osm_tag}(area.searchArea);
#         relation{osm_tag}(area.searchArea);
#     );
#     out center tags;
#     """
#
#     response = requests.get(overpass_url, params={"data": query})
#     data = response.json().get("elements", [])
#
#     if not data:
#         st.warning("Geen resultaten gevonden.")
#     else:
#         # Extract relevant info
#         data_list = []
#         for place in data:
#             tags = place.get("tags", {})
#             name = tags.get("name")
#             address = ", ".join(filter(None, [
#                 tags.get("addr:street"),
#                 tags.get("addr:housenumber"),
#                 tags.get("addr:postcode"),
#                 tags.get("addr:city")
#             ]))
#             phone = get_first_available(tags, ["phone", "contact:phone"])
#             website = get_first_available(tags, ["website", "contact:website", "url"])
#
#             data_list.append({
#                 "Naam": name,
#                 "Adres": address if address else None,
#                 "Telefoon": phone,
#                 "Website": website
#             })
#
#         df = pd.DataFrame(data_list)
#         st.success(f"Gevonden: {len(df)} resultaten!")
#         st.dataframe(df)
#
#         # Excel export
#         filename = f"{category_input}_{place_input}.xlsx".replace(" ", "_")
#         excel_buffer = BytesIO()
#         df.to_excel(excel_buffer, index=False)
#         excel_buffer.seek(0)
#
#         st.download_button(
#             label = "Download Excel-bestand",
#             data = excel_buffer,
#             file_name = filename,
#             mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
#         )