import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, date

# --- Configuration ---
DB_NAME = 'Yang.db'
KPI_TABLE_NAME = 'kpis'
PORTCO_TABLE_NAME = 'list_of_portco' # Needed for company list and linking

# --- Database Functions ---

def db_connect():
    """Establishes database connection."""
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def create_kpi_table():
    """Creates the kpis table if it doesn't exist (idempotent)."""
    conn = db_connect()
    c = conn.cursor()
    try:
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {KPI_TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portco_name TEXT NOT NULL COLLATE NOCASE,
                kpi_name TEXT NOT NULL COLLATE NOCASE,
                kpi_value REAL,
                kpi_date TEXT NOT NULL,
                units TEXT,
                notes TEXT,
                FOREIGN KEY (portco_name) REFERENCES {PORTCO_TABLE_NAME}(portco_name) ON DELETE CASCADE ON UPDATE CASCADE
            )
        ''')
        conn.commit()
        c.execute(f"CREATE INDEX IF NOT EXISTS idx_kpi_portco_date ON {KPI_TABLE_NAME} (portco_name, kpi_date);")
        c.execute(f"CREATE INDEX IF NOT EXISTS idx_kpi_name ON {KPI_TABLE_NAME} (kpi_name);")
        conn.commit()
    except sqlite3.Error as e:
        print(f"Warning during {KPI_TABLE_NAME} creation/check (in Pages/3...): {e}")
    finally:
        if conn: conn.close()

def get_portco_names_from_db():
    """Retrieves a sorted list of unique *active* portfolio company names."""
    conn = db_connect()
    names = ["Error: Could not load"] # Default error value
    try:
        c = conn.cursor()
        c.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{PORTCO_TABLE_NAME}'")
        if not c.fetchone():
            st.error(f"Error: Portfolio company table '{PORTCO_TABLE_NAME}' not found.")
            return ["Error: Company table missing"]

        df = pd.read_sql_query(f"SELECT DISTINCT portco_name FROM {PORTCO_TABLE_NAME} WHERE status = 'Active' ORDER BY portco_name COLLATE NOCASE", conn)
        names = df['portco_name'].tolist()
        if not names:
             return ["Error: No active companies found"] # More specific message
        return names
    except Exception as e:
        st.error(f"Error reading portfolio company names: {e}")
        return names # Return default error value
    finally:
        if conn: conn.close()


def add_kpi(portco_name, kpi_name, kpi_value, kpi_date_obj, units, notes):
    """Adds a new KPI record to the database."""
    conn = db_connect()
    c = conn.cursor()
    date_str = kpi_date_obj.strftime('%Y-%m-%d') if kpi_date_obj else None
    value_to_db = kpi_value if kpi_value is not None else None
    units_to_db = units.strip() if units else None
    notes_to_db = notes.strip() if notes else None
    try:
        c.execute(f'''
            INSERT INTO {KPI_TABLE_NAME} (portco_name, kpi_name, kpi_value, kpi_date, units, notes)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (portco_name, kpi_name.strip(), value_to_db, date_str, units_to_db, notes_to_db))
        conn.commit()
        st.toast(f"✅ Added KPI '{kpi_name}' for '{portco_name}'!", icon="✅")
        return True
    except sqlite3.IntegrityError as e:
        if "FOREIGN KEY constraint failed" in str(e):
             st.error(f"Error: Could not add KPI. Portfolio company '{portco_name}' may not exist or is inactive.")
        else:
             st.error(f"Database integrity error adding KPI: {e}")
        return False
    except sqlite3.Error as e:
        st.error(f"Database error adding KPI: {e}")
        return False
    finally:
        if conn: conn.close()

def get_all_kpis_with_portco_id():
    """Retrieves all KPIs JOINED with portco table to get company ID for linking."""
    conn = db_connect()
    try:
        # Join to get portco.id for linking to the main app page
        query = f"""
            SELECT
                k.id, k.portco_name, k.kpi_name, k.kpi_value, k.kpi_date, k.units, k.notes,
                p.id as portco_id
            FROM {KPI_TABLE_NAME} k
            LEFT JOIN {PORTCO_TABLE_NAME} p ON k.portco_name = p.portco_name COLLATE NOCASE
            ORDER BY k.portco_name COLLATE NOCASE, k.kpi_date DESC, k.kpi_name COLLATE NOCASE
            """
        df = pd.read_sql_query(query, conn)

        # Convert types
        df['kpi_date'] = pd.to_datetime(df['kpi_date'], errors='coerce')
        df['kpi_value'] = pd.to_numeric(df['kpi_value'], errors='coerce')
        df['portco_id'] = df['portco_id'].fillna(-1).astype(int) # Handle missing join

        return df
    except Exception as e:
        st.error(f"Error reading all KPIs: {e}")
        # Return specific columns even on error
        return pd.DataFrame(columns=['id', 'portco_name', 'kpi_name', 'kpi_value', 'kpi_date',
                                     'units', 'notes', 'portco_id'])
    finally:
        if conn: conn.close()

# --- Helper Function ---
def format_currency(value):
    """Formats a number as USD currency string with commas."""
    if pd.isna(value) or value is None: return None
    try: return "${:,.2f}".format(float(value))
    except (ValueError, TypeError): return "Invalid Format"


# --- Streamlit Page ---
#st.set_page_config(layout="wide", page_title="KPIs")

def render_kpi_dashboard():
    """Displays the main KPI dashboard: KPI List (Markdown Table) and Add Form."""
    st.title("Key Performance Indicators (KPIs) Overview")

    # Ensure table exists
    create_kpi_table()

    # --- Display Table of All KPIs ---
    df_all_kpis = get_all_kpis_with_portco_id()
    st.markdown(f"**{len(df_all_kpis)}** KPI records found.")

    if df_all_kpis.empty:
        st.info("ℹ️ No KPIs recorded yet. Add one using the form below.")
    else:
        df_display = df_all_kpis.copy()

        # Create clickable company links (pointing to main app page)
        df_display['Company'] = df_display.apply(
            lambda row: f"[{row['portco_name']}](/?page=company&id={row['portco_id']})" if row['portco_id'] != -1 else row['portco_name'],
            axis=1
        )

        # Format Value + Units
        df_display['Value'] = df_display.apply(
             lambda row: f"{row['kpi_value']:,.2f} {row['units']}" if pd.notna(row['kpi_value']) and row['units']
             else f"{row['kpi_value']:,.2f}" if pd.notna(row['kpi_value'])
             else "N/A",
             axis=1
        )
        df_display['Date'] = df_display['kpi_date'].dt.strftime('%Y-%m-%d').fillna('N/A') # Format Date string

        # --- Prepare DataFrame for Markdown Table ---
        display_columns_map = {
            'id': 'KPI ID',
            'Company': 'Company', # This now contains the Markdown link
            'Date': 'Date',
            'kpi_name': 'KPI Name',
            'Value': 'Value',
            'notes': 'Notes'
        }
        df_to_show = df_display[list(display_columns_map.keys())].rename(columns=display_columns_map)

        # --- Fix 1: Use st.markdown to render links ---
        st.markdown(
            df_to_show.to_markdown(index=False),
            unsafe_allow_html=True # IMPORTANT: Allows the links to be rendered
        )


    # --- Add KPI Form ---
    st.divider()
    with st.expander("➕ Add New KPI Record", expanded=False):
        available_portcos = get_portco_names_from_db()

        # Only show form if companies are available
        if not available_portcos or available_portcos[0].startswith("Error:"):
             st.warning(f"Cannot add KPIs: {available_portcos[0]}")
        else:
            with st.form("kpi_form_add", clear_on_submit=True):
                st.subheader("Enter KPI Details")

                add_portco_name = st.selectbox("Company*", available_portcos, index=None, placeholder="Select Company...")
                add_kpi_name = st.text_input("KPI Name*", placeholder="e.g., Monthly Recurring Revenue")
                add_kpi_value = st.number_input("Value*", value=None, format="%.2f", help="Enter the numeric value of the KPI")
                add_kpi_units = st.text_input("Units (e.g., $, %, users)", placeholder="Optional")
                add_kpi_date = st.date_input("Date*", value=date.today(), max_value=date.today()) # Default to today
                add_kpi_notes = st.text_area("Notes", placeholder="Optional context...")

                submitted_add = st.form_submit_button("Add KPI Record", type="primary")

                if submitted_add:
                    is_valid = True
                    if not add_portco_name: st.error("Company Name required."); is_valid = False
                    if not add_kpi_name: st.error("KPI Name required."); is_valid = False
                    if add_kpi_value is None: st.error("KPI Value required."); is_valid = False # Check for None
                    if add_kpi_date is None: st.error("KPI Date required."); is_valid = False

                    if is_valid:
                        # Clean optional fields
                        units_clean = add_kpi_units.strip() if add_kpi_units else None
                        notes_clean = add_kpi_notes.strip() if add_kpi_notes else None
                        if add_kpi(add_portco_name, add_kpi_name.strip(), add_kpi_value, add_kpi_date, units_clean, notes_clean):
                            st.rerun()

# --- Main Execution ---
render_kpi_dashboard()