import streamlit as st
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

# Load env vars
load_dotenv()

# --- Database Connection ---
def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

# --- Fetch list of trips with route info ---
def fetch_trip_options(conn):
    query = """
        SELECT td.trip_id, dt.route_id, dr.route_short_name, dt.trip_headsign
        FROM trip_delays td
        JOIN dim_trips dt ON td.trip_id = dt.trip_id
        LEFT JOIN dim_routes dr ON dt.route_id = dr.route_id
        GROUP BY td.trip_id, dt.route_id, dr.route_short_name, dt.trip_headsign
        ORDER BY td.trip_id DESC
        LIMIT 100
    """
    df = pd.read_sql(query, conn)
    # Create a display string combining trip and route info
    df['display'] = df.apply(lambda row: f"Trip: {row['trip_id']} | Route: {row['route_short_name']} | Headsign: {row['trip_headsign']}", axis=1)
    return df

# --- Fetch delay data for trip ---
def fetch_trip_delays(conn, trip_id):
    query = """
        SELECT stop_sequence, stop_id, scheduled_arrival, actual_arrival, delay_seconds
        FROM trip_delays
        WHERE trip_id = %s and delay_seconds > 0
        ORDER BY stop_sequence
    """
    return pd.read_sql(query, conn, params=(trip_id,))

# --- Fetch trip metadata ---
def fetch_trip_info(conn, trip_id):
    query = """
        SELECT route_id, trip_headsign, direction_id
        FROM dim_trips
        WHERE trip_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (trip_id,))
        return cur.fetchone()

# --- UI ---
st.set_page_config(page_title="GTFS Trip Delay Viewer", layout="wide")
st.title("MBTA Trip Delay Viewer")

try:
    with st.spinner("Connecting to database..."):
        conn = get_connection()

    trip_options_df = fetch_trip_options(conn)

    if not trip_options_df.empty:
        # Create a dictionary mapping display string to trip_id for selection
        options = trip_options_df.set_index('display')['trip_id'].to_dict()

        selected_display = st.selectbox("Select a Trip with Route Info", list(options.keys()))

        if selected_display:
            selected_trip = options[selected_display]

            st.subheader(f"Trip ID: {selected_trip}")
            info = fetch_trip_info(conn, selected_trip)

            if info:
                route_id, headsign, direction = info
                st.markdown(f"**Route ID:** {route_id}  \n**Direction:** {direction}  \n**Headsign:** {headsign}")

            df = fetch_trip_delays(conn, selected_trip)

            if not df.empty:
                st.line_chart(df.set_index("stop_sequence")["delay_seconds"])
                st.dataframe(df)
            else:
                st.warning("No delay data found for this trip.")
    else:
        st.warning("No trips available in database.")

except Exception as e:
    st.error(f"Database error: {e}")

finally:
    try:
        conn.close()
    except:
        pass
