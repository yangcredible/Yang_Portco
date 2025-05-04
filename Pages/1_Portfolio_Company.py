# Pages/1_Portfolio_Company.py (Simplified - Lists Companies & Adds New)

import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime

# --- Configuration ---
DB_NAME = 'Yang.db'
PORTCO_TABLE_NAME = 'list_of_portco'
INDUSTRY_LIST = ['Energy','Materials', 'Industrials', 'Consumer Discretionary', 'Consumer Staples', 'Health Care', 'Financials', 'Information Technology', 'Communication Services', 'Utilities', 'Real Estate']
# --- Country list is now fetched dynamically ---

# --- Database Functions ---

def db_connect():
    """Establishes database connection."""
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

# Import the function from the main script (if structure allows)
# If not, duplicate the function here. For simplicity, let's duplicate.
#@st.cache_data(ttl=3600)
def get_distinct_countries_from_db():
    """Retrieves a sorted list of unique establishment countries."""
    conn = db_connect()
    countries = []
    try:
        c = conn.cursor()
        c.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{PORTCO_TABLE_NAME}'")
        if not c.fetchone(): return []
        df = pd.read_sql_query(f"""
            SELECT DISTINCT establishment_country FROM {PORTCO_TABLE_NAME}
            WHERE establishment_country IS NOT NULL AND establishment_country != ''
            ORDER BY establishment_country COLLATE NOCASE
            """, conn)
        countries = df['establishment_country'].tolist()
        return countries
    except Exception as e:
        print(f"Error reading distinct countries (in Pages/1...): {e}")
        return []
    finally:
        if conn: conn.close()

def create_portco_table():
    """Creates the portfolio company table if it doesn't exist."""
    conn = db_connect()
    c = conn.cursor()
    try:
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {PORTCO_TABLE_NAME}(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portco_name TEXT NOT NULL UNIQUE COLLATE NOCASE,
                year_founded INT NOT NULL,
                industry_classification TEXT NOT NULL,
                establishment_country TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'Active' CHECK(status IN ('Active', 'Inactive'))
                )
        ''')
        conn.commit()
        c.execute(f"PRAGMA table_info({PORTCO_TABLE_NAME})")
        columns = {col[1]: col[2] for col in c.fetchall()}
        if 'status' not in columns:
            try:
                c.execute(f"ALTER TABLE {PORTCO_TABLE_NAME} ADD COLUMN status TEXT NOT NULL DEFAULT 'Active' CHECK(status IN ('Active', 'Inactive'))")
                conn.commit()
            except sqlite3.Error as e_alter:
                 if "duplicate column name" not in str(e_alter): print(f"Warning: Could not add 'status' column to {PORTCO_TABLE_NAME} (in Pages/1...). {e_alter}")
    except sqlite3.Error as e:
        print(f"Warning during {PORTCO_TABLE_NAME} creation/check (in Pages/1...): {e}")
    finally:
        if conn: conn.close()

def get_all_portcos():
    """Retrieves all portfolio companies."""
    conn = db_connect()
    try:
        df = pd.read_sql_query(f"SELECT id, portco_name, year_founded, industry_classification, establishment_country, status FROM {PORTCO_TABLE_NAME} ORDER BY portco_name COLLATE NOCASE", conn)
        return df
    except Exception as e:
        st.error(f"Error reading portfolio companies: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()

def add_portco(portco_name, year_founded, industry_classification_str, establishment_country_str):
    """Adds a new portfolio company."""
    conn = db_connect()
    c = conn.cursor()
    try:
        c.execute(f'''INSERT INTO {PORTCO_TABLE_NAME}
                  (portco_name, year_founded, industry_classification, establishment_country)
                  VALUES (?, ?, ?, ?)
                  ''', (portco_name, year_founded, industry_classification_str, establishment_country_str))
        conn.commit()
        st.toast(f"✅ Successfully added '{portco_name}'!", icon="✅")
        return True
    except sqlite3.IntegrityError:
         st.error(f"Error: Portfolio company '{portco_name}' already exists.")
         return False
    except sqlite3.Error as e:
        st.error(f"Database error during add: {e}")
        return False
    finally:
        if conn: conn.close()

# --- Streamlit Page ---
#st.set_page_config(layout="wide", page_title="Portfolio Companies")

# --- Render Functions ---
def render_main_dashboard():
    """Displays the main dashboard: Company List and Add Form."""
    st.title("Portfolio Company Overview")
    create_portco_table()
    df_portcos_full = get_all_portcos()

    st.markdown(f"**{len(df_portcos_full)}** companies listed.")
    st.caption("Click on a company name link to view details.")

    if df_portcos_full.empty: st.info("ℹ️ No companies found. Add one below.")
    else:
        df_display = df_portcos_full.copy()
        df_display['Company Name'] = df_display.apply(lambda row: f"[{row['portco_name']}](/?page=company&id={row['id']})", axis=1)
        display_columns_map = {'Company Name': 'Company Name','year_founded': 'Year Founded','industry_classification': 'Industries','establishment_country': 'Country','status': 'Status'}
        df_to_show = df_display[list(display_columns_map.keys())].rename(columns=display_columns_map)
        st.markdown(df_to_show.to_markdown(index=False), unsafe_allow_html=True)

    st.divider()
    with st.expander("➕ Add New Company", expanded=False):
        with st.form("portco_form_add", clear_on_submit=True):
            st.subheader("Enter New Company Details")
            add_portco_name = st.text_input("Company Name*", placeholder="Enter company name")
            add_year_founded = st.slider("Founded in Year*", 2000, datetime.now().year + 1, value=datetime.now().year)
            add_industry_list = st.multiselect("Industry Classification(s)*", INDUSTRY_LIST, help="Select one or more industries")

            # --- Fetch dynamic country list for Add form ---
            available_countries = get_distinct_countries_from_db()
            # Allow adding a new country via text input if list is empty or needed
            allow_new_country = not available_countries or st.checkbox("Add a new country not listed?")

            if allow_new_country:
                 add_establishment_country = st.text_input("Established in (Country)*", placeholder="Enter country name")
                 if not available_countries:
                     st.caption("No existing countries found. Enter the country name.")
                 else:
                     st.caption("Or select from existing:")
                     selected_existing = st.selectbox("Select Existing", ["---"] + available_countries, key="add_select_existing_country", index=0)
                     # If user selects an existing one after checking the box, use that one instead
                     if selected_existing != "---":
                         add_establishment_country = selected_existing

            else:
                 add_establishment_country = st.selectbox(
                     "Established in*",
                     available_countries, # Use dynamic list
                     index=None,
                     placeholder="Select country..."
                 )

            submitted_add = st.form_submit_button("Add Company", type="primary")
            if submitted_add:
                is_valid = True
                if not add_portco_name: st.error("Company Name required."); is_valid = False
                if not add_industry_list: st.error("Industry required."); is_valid = False
                if not add_establishment_country: st.error("Country required."); is_valid = False
                if is_valid:
                    industry_str = ", ".join(sorted(add_industry_list))
                    # Use .strip() for the country from text input
                    country_final = add_establishment_country.strip() if isinstance(add_establishment_country, str) else add_establishment_country
                    if add_portco(add_portco_name.strip(), add_year_founded, industry_str, country_final):
                        st.rerun()

# --- Main Execution ---
render_main_dashboard()