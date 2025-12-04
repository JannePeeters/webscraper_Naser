import streamlit as st

def render_ui():
    search_option = st.radio(
        "Kies zoekmethode",
        ("Categorie en plaats typen", "Categorie typen en plaats selecteren op kaart")
    )

    category_input = st.text_input("Categorie (bijv. restaurant, supermarkt)", "Restaurant")
    place_input = None
    radius_m = None

    if search_option == "Categorie en plaats typen":
        place_input = st.text_input("Plaatsnaam (bijv. Nijmegen, Oosterhout)", "Nijmegen")
    else:
        st.write("Dubbelklik op de kaart om het centrum van je zoekgebied te selecteren.")
        radius_m = st.slider("Straal (meters)", 100, 5000, 1000)

    return search_option, category_input, place_input, radius_m
