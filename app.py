import streamlit as st
from ui import render_ui
from map_utils import render_map_and_get_state
from search_utils import run_search
from sheets_utils import upload_to_google_sheets
from io import BytesIO
import pandas as pd

st.title("Webscraper via Google Places API")
st.write("Hier kun je automatisch gegevens van door jou gekozen bedrijven ophalen!")

# Zet default search_option
if "search_option" not in st.session_state:
    st.session_state.search_option = "Categorie en plaats typen"
if "radius_m" not in st.session_state:
    st.session_state.radius_m = 1000

search_option, category_input, place_input, radius_m = render_ui()

# Default kaart als er nog geen resultaten zijn
if search_option == "Categorie typen en plaats selecteren op kaart" and not st.session_state.get("last_results"):
    clicked_location = render_map_and_get_state(radius_m, force_render=True, key_suffix="default")

zoek, annuleren = st.columns([1,1])
with zoek:
    zoek_knop = st.button("Zoek", key="btn_zoek")
with annuleren:
    annuleren_knop = st.button("Annuleren", key="btn_annuleren")

if zoek_knop:
    # Reset tijdelijke markers en session_state
    st.session_state.last_results = []

    clicked_location = st.session_state.get("clicked_location") if search_option == "Categorie typen en plaats selecteren op kaart" else None

    df, input_text, filename = run_search(
        search_option=search_option,
        category_input=category_input,
        place_input=place_input,
        clicked_location=clicked_location,
        radius_m=radius_m
    )

    if df is None or df.empty:
        st.warning("Geen resultaten gevonden.")
    else:
        # Flag dat er gezocht is
        st.session_state.has_searched = True

        df_active = upload_to_google_sheets(df, category_input, input_text)
        st.session_state.last_results = df_active.to_dict(orient="records")

# DF (en map) tonen
if st.session_state.get("last_results"):
    df_active = pd.DataFrame(st.session_state.last_results)

    if search_option == "Categorie typen en plaats selecteren op kaart":
        map_col, df_col = st.columns([3, 2])

        with map_col:
            st.subheader("Resultaten op kaart")
            render_map_and_get_state(radius_m, results=st.session_state.last_results, force_render=True, key_suffix=f"search_{hash(str(st.session_state.last_results))}")

        with df_col:
            st.subheader("Resultaten tabel")
            st.dataframe(df_active)

            excel_buffer = BytesIO()
            df_active.to_excel(excel_buffer, index=False)
            excel_buffer.seek(0)
            st.download_button(
                label="Download Excel-bestand",
                data=excel_buffer.getvalue(),
                file_name="resultaten.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_excel_rerun"
            )

            # Link naar Google Sheet
            st.markdown(
                "[Klik hier om gegevens van eerdere bedrijven in Google Sheets te bekijken](https://docs.google.com/spreadsheets/d/1tZNnGy-KBW0LdnmzqDbKGgkQ1I8wM7_rf5qnbAkTy0s/edit?gid=0#gid=0)",
                unsafe_allow_html=True
            )
    else:
        st.subheader("Resultaten tabel")
        st.dataframe(df_active)

        excel_buffer = BytesIO()
        df_active.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)
        st.download_button(
            label="Download Excel-bestand",
            data=excel_buffer.getvalue(),
            file_name="resultaten.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_excel_rerun"
        )

        # Link naar Google Sheet
        st.markdown(
            "[Klik hier om gegevens van eerdere bedrijven in Google Sheets te bekijken](https://docs.google.com/spreadsheets/d/1tZNnGy-KBW0LdnmzqDbKGgkQ1I8wM7_rf5qnbAkTy0s/edit?gid=0#gid=0)",
            unsafe_allow_html=True
        )

if st.session_state.get("has_searched"):
    if st.button("Nieuwe zoekopdracht", key="btn_reset"):
        for k in ["clicked_location", "last_results", "scrape_results", "map_center", "map_zoom", "map_bounds", "has_searched", "radius_m"]:
            if k in st.session_state:
                del st.session_state[k]
        st.rerun()

if annuleren_knop:
    for k in list(st.session_state.keys()):
        del st.session_state[k]
    st.rerun()