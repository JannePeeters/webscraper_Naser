import streamlit as st
import pandas as pd
import requests
from io import BytesIO

st.title("Webscraperrrrrrrr")
st.write("Zoek automatisch bedrijven en exporteer naar Excel.")

# --- Input ---
category_input = st.text_input("Categorie (bijv. restaurant, cafe, hotel)", "Restaurant")
place_input = st.text_input("Plaatsnaam (bijv. Nijmegen, Oosterhout)", "Nijmegen")

# --- Helper function ---
def get_first_available(tags, possible_keys):
    """Return the first existing tag value from a list of possible keys"""
    for key in possible_keys:
        if key in tags and tags[key]:
            return tags[key]
    return None

# --- Mapping category ---
category_map = {
    "restaurant": ("amenity", "restaurant"),
    "cafe": ("amenity", "cafe"),
    "bar": ("amenity", "bar"),
    "fastfood": ("amenity", "fast_food"),
    "bank": ("amenity", "bank"),
    "apotheek": ("amenity", "pharmacy"),
    "school": ("amenity", "school"),
    "ziekenhuis": ("amenity", "hospital"),

    "hotel": ("tourism", "hotel"),
    "hostel": ("tourism", "hostel"),
    "museum": ("tourism", "museum"),
    "attractie": ("tourism", "attraction"),
    "uitzichtpunt": ("tourism", "viewpoint"),
    "galerie": ("tourism", "galery"),

    "supermarkt": ("shop", "supermarket"),
    "bakkerij": ("shop", "bakery"),
    "kledingwinkel": ("shop", "clothes"),
    "elektronica": ("shop", "electronics"),
    "winkel": ("shop", "mall")
}

osm_key, osm_value = category_map.get(category_input.lower(), ("amenity", category_input.lower()))
osm_tag = f"['{osm_key}'='{osm_value}']"

if st.button("Zoek"):
    st.info(f"Zoeken naar {category_input.lower()}s in {place_input}...")

    overpass_url = "http://overpass-api.de/api/interpreter"
    query = f"""
    [out:json][timeout:25];
    area[name="{place_input}"]->.searchArea;
    (
        node{osm_tag}(area.searchArea);
        way{osm_tag}(area.searchArea);
        relation{osm_tag}(area.searchArea);
    );
    out center tags;
    """

    response = requests.get(overpass_url, params={"data": query})
    data = response.json().get("elements", [])

    if not data:
        st.warning("Geen resultaten gevonden.")
    else:
        # Extract relevant info
        data_list = []
        for place in data:
            tags = place.get("tags", {})
            name = tags.get("name")
            address = ", ".join(filter(None, [
                tags.get("addr:street"),
                tags.get("addr:housenumber"),
                tags.get("addr:postcode"),
                tags.get("addr:city")
            ]))
            phone = get_first_available(tags, ["phone", "contact:phone"])
            website = get_first_available(tags, ["website", "contact:website", "url"])

            data_list.append({
                "Naam": name,
                "Adres": address if address else None,
                "Telefoon": phone,
                "Website": website
            })

        df = pd.DataFrame(data_list)
        st.success(f"Gevonden: {len(df)} resultaten!")
        st.dataframe(df)

        # Excel export
        filename = f"{category_input}_{place_input}.xlsx".replace(" ", "_")
        excel_buffer = BytesIO()
        df.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)

        st.download_button(
            label = "Download Excel-bestand",
            data = excel_buffer,
            file_name = filename,
            mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# ***** Google API ***** (untested)
# API_KEY = ""
#
# query = input("Wat wil je zoeken? (Bijv. restaurants Nijmegen): ")
# query_encoded = query.replace(" ", "+")
# places_url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={query_encoded}&key={API_KEY}"
#
# response = requests.get(places_url)
# results = response.json().get("results", [])
#
# data = []
# for place in results:
#     place_id = place["place_id"]
#     details_url = f"https://maps.googleapis.com/maps/api/plac/details/json?place_id={place_id}&fields=name,formatted_address,website,formatted_phone_number&key={API_KEY}"
#     details = requests.get(details_url).json().get("result", [])
#
#     data.append({
#         "Naam": details.get("name"),
#         "Adres": details.get("formatted_address"),
#         "Website": details.get("website"),
#         "Telefoon": details.get("formatted_phone_number")
#     })
#
# filename = query.replace(" ", "_") + ".xlsx"
# pd.DataFrame(data).to_excel(filename, index=False)
# print(f"Bestand opgeslagen als {filename}")