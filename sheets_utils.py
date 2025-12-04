from google.oauth2.service_account import Credentials
import streamlit as st
import gspread
import pandas as pd
from datetime import datetime

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

creds = Credentials.from_service_account_info(st.secrets["gspread"], scopes=SCOPES)
google_client = gspread.authorize(creds)

def normalise_for_compare(value):
    if pd.isna(value) or value in [None, "None", "", "nan", "NaN"]:
        return ""
    return str(value)

def upload_to_google_sheets(df, category_input, input_text):
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
        if input_text.startswith("Getypt:"):
            mask = df_existing["Input"].fillna("").str.lower() == input_text.lower()
            display_cols = ["Naam", "Adres", "Telefoon", "Website", "E-mail"]
        else: # Kaart search: filter op categorie alleen
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
                continue

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
        if input_text.startswith("Getypt:"):
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
        return df[[c for c in display_cols if c in df.columns]]

    except Exception as e:
        st.error(f"Fout bij uploaden naar Google Sheets: {e}")
        return df # Fallback zodat iets terugkomt