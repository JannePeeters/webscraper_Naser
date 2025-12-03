import numpy as np
import streamlit as st
import folium
from folium.plugins import MarkerCluster
from streamlit import session_state
from streamlit_folium import st_folium

def render_map_and_get_state(radius_m, results=None, force_render=False, key_suffix="default"):
    # Defaults
    if "map_center" not in st.session_state: st.session_state.map_center = [52.0, 5.0]
    if "map_zoom" not in st.session_state: st.session_state.map_zoom = 8
    if "clicked_location" not in st.session_state: st.session_state.clicked_location = None
    if "last_results" not in st.session_state: st.session_state.last_results = []  # will hold list of dicts with lat/lon/name etc.

    markers_to_show = results or st.session_state.last_results

    # Als er geen reden is om de kaart te tonen, stop
    if not force_render and not st.session_state.clicked_location and not markers_to_show:
        return None

    # Map build
    m = folium.Map(
        location=st.session_state.map_center,
        zoom_start=st.session_state.map_zoom,
        control_scale=True,
    )

    # UX: Crosshair cursor
    m.get_root().html.add_child(folium.Element("<style>.leaflet-container { cursor: crosshair !important; }</style>"))

    # Marker + radius
    if st.session_state.clicked_location:
        folium.Marker(
            location=st.session_state.clicked_location,
            icon=folium.Icon(icon="map-pin", prefix='fa', color='red'), popup="Geselecteerd centrum"
        ).add_to(m)

        folium.Circle(
            location=st.session_state.clicked_location,
            radius=radius_m,
            color="blue",
            fill=True,
            fill_opacity=0.15
        ).add_to(m)

    # Resultaten
    if markers_to_show:
        cluster = MarkerCluster()
        for result in markers_to_show:
            lat, lon = result.get("Latitude"), result.get("Longitude")
            if lat and lon:
                popup = f"{result.get('Naam', 'Resultaat')}<br>{result.get('Adres', '')}"
                folium.Marker([lat, lon], popup=popup).add_to(cluster)
        m.add_child(cluster)

    # Auto-zoom
    bounds_to_fit = None
    if session_state.clicked_location:
        # Zoom op radius van clicked_location
        lat, lon = st.session_state.clicked_location
        delta_lat = radius_m / 111_000
        delta_lon = radius_m / (111_000 * np.cos(np.radians(lat)))
        bounds_to_fit = [
            [lat - delta_lat, lon - delta_lon],
            [lat + delta_lat, lon + delta_lon]
        ]
    elif markers_to_show:
            lats = [r.get("Latitude") for r in markers_to_show if r.get("Latitude")]
            lons = [r.get("Longitude") for r in markers_to_show if r.get("Longitude")]
            if lats and lons:
                bounds_to_fit = [
                    [min(lats), min(lons)],
                    [max(lats), max(lons)]
                ]

    if bounds_to_fit:
        m.fit_bounds(bounds_to_fit)

        # Update map center
        sw, ne = bounds_to_fit
        center_lat = (sw[0] + ne[0]) / 2
        center_lon = (sw[1] + ne[1]) / 2
        st.session_state.map_center = [center_lat, center_lon]

    # Map renderen met unieke key
    map_data = st_folium(
        m,
        height=500,
        width=700,
        returned_objects=["last_clicked", "bounds"],
        key=f"map_{key_suffix}"
    )

    # Bounds opslaan
    if map_data and map_data.get("last_clicked"):
        last_clicked = map_data["last_clicked"]
        st.session_state.clicked_location = [last_clicked["lat"], last_clicked["lng"]]

    return st.session_state.clicked_location
