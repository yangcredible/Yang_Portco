# Pages/4_Events.py

import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, date

# --- Configuration ---
DB_NAME = 'Yang.db'
EVENT_TABLE_NAME = 'events'
PORTCO_TABLE_NAME = 'list_of_portco' # Needed for company list and linking

# Event Config (copied from main)
EVENT_TYPES = ['Exit', 'Dividend', 'Valuation Update']
CURRENCY_OPTIONS = ['USD', 'SGD', 'EUR', 'GBP', 'Other']

# --- Database Functions ---

def db_connect():
    """Establishes database connection and enables Foreign Keys."""
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

# Only need functions relevant to this page: add, get_all, get_portco_names
def create_event_table():
    """Creates the events table if it doesn't exist (idempotent)."""
    # (Same function as in Yang_Portfolio.py)
    conn = db_connect()
    c = conn.cursor()
    try:
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {EVENT_TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portco_name TEXT NOT NULL COLLATE NOCASE,
                event_date TEXT NOT NULL,
                event_type TEXT NOT NULL CHECK(event_type IN {tuple(EVENT_TYPES)}),
                cash_flow_amount REAL, currency TEXT DEFAULT 'USD',
                percent_holding_sold REAL, fund_holding_valuation REAL, notes TEXT,
                FOREIGN KEY (portco_name) REFERENCES {PORTCO_TABLE_NAME}(portco_name) ON DELETE CASCADE ON UPDATE CASCADE
            ) ''')
        conn.commit()
        c.execute(f"CREATE INDEX IF NOT EXISTS idx_event_portco_date ON {EVENT_TABLE_NAME} (portco_name, event_date);")
        conn.commit()
    except sqlite3.Error as e: print(f"Warning during {EVENT_TABLE_NAME} creation/check (in Pages/4...): {e}")
    finally:
        if conn: conn.close()

def get_portco_names_from_db():
    """Retrieves a sorted list of unique *active* portfolio company names."""
    # (Same function as used elsewhere)
    conn = db_connect()
    names = ["Error: Could not load"]
    try:
        c = conn.cursor()
        c.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{PORTCO_TABLE_NAME}'")
        if not c.fetchone(): st.error(f"Error: PortCo table '{PORTCO_TABLE_NAME}' not found."); return ["Error: Table missing"]
        df = pd.read_sql_query(f"SELECT DISTINCT portco_name FROM {PORTCO_TABLE_NAME} WHERE status = 'Active' ORDER BY portco_name COLLATE NOCASE", conn)
        names = df['portco_name'].tolist()
        if not names: return ["Error: No active companies"]
        return names
    except Exception as e: st.error(f"Error reading PortCo names: {e}"); return names
    finally:
        if conn: conn.close()

def add_event(portco_name, event_type, event_date_obj, cash_flow_amount, currency, percent_holding_sold, fund_holding_valuation, notes):
    """Adds a new event record to the database."""
    conn = db_connect()
    c = conn.cursor()
    date_str = event_date_obj.strftime('%Y-%m-%d') if event_date_obj else None
    # Handle Nones explicitly
    cf = cash_flow_amount if cash_flow_amount is not None else None
    phs = percent_holding_sold if percent_holding_sold is not None else None
    fhv = fund_holding_valuation if fund_holding_valuation is not None else None
    curr = currency.strip() if currency else 'USD'
    nts = notes.strip() if notes else None

    try:
        c.execute(f'''
            INSERT INTO {EVENT_TABLE_NAME}
            (portco_name, event_date, event_type, cash_flow_amount, currency, percent_holding_sold, fund_holding_valuation, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (portco_name, date_str, event_type, cf, curr, phs, fhv, nts))
        conn.commit()
        st.toast(f"✅ Added '{event_type}' event for '{portco_name}'!", icon="✅")
        return True
    except sqlite3.IntegrityError as e:
        if "FOREIGN KEY constraint failed" in str(e): st.error(f"Error: Company '{portco_name}' may not exist or is inactive.")
        elif "CHECK constraint failed" in str(e): st.error(f"Error: Invalid Event Type '{event_type}'.")
        else: st.error(f"DB integrity error adding event: {e}")
        return False
    except sqlite3.Error as e:
        st.error(f"DB error adding event: {e}")
        return False
    finally:
        if conn: conn.close()

def get_all_events_with_portco_id():
    """Retrieves all Events JOINED with portco table to get company ID for linking."""
    conn = db_connect()
    try:
        query = f"""
            SELECT
                e.id, e.portco_name, e.event_date, e.event_type, e.cash_flow_amount, e.currency,
                e.percent_holding_sold, e.fund_holding_valuation, e.notes,
                p.id as portco_id
            FROM {EVENT_TABLE_NAME} e
            LEFT JOIN {PORTCO_TABLE_NAME} p ON e.portco_name = p.portco_name COLLATE NOCASE
            ORDER BY e.event_date DESC, e.portco_name COLLATE NOCASE, e.id DESC
            """
        df = pd.read_sql_query(query, conn)
        # Convert types
        df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
        df['cash_flow_amount'] = pd.to_numeric(df['cash_flow_amount'], errors='coerce')
        df['percent_holding_sold'] = pd.to_numeric(df['percent_holding_sold'], errors='coerce')
        df['fund_holding_valuation'] = pd.to_numeric(df['fund_holding_valuation'], errors='coerce')
        df['portco_id'] = df['portco_id'].fillna(-1).astype(int)
        return df
    except Exception as e:
        st.error(f"Error reading all Events: {e}")
        return pd.DataFrame(columns=['id', 'portco_name', 'event_date', 'event_type', 'cash_flow_amount',
                                     'currency', 'percent_holding_sold', 'fund_holding_valuation', 'notes', 'portco_id'])
    finally:
        if conn: conn.close()

# --- Helper ---
def format_currency(value, currency_symbol='$'):
    # (Same as in Yang_Portfolio.py)
    if pd.isna(value) or value is None: return None
    try: return f"{currency_symbol}{float(value):,.2f}"
    except (ValueError, TypeError): return "Invalid Format"

# --- Streamlit Page ---
#st.set_page_config(layout="wide", page_title="Financial Events")

def render_events_dashboard():
    """Displays the main Events dashboard: Event List and Add Forms."""
    st.title("Financial Events Log")

    # Ensure table exists
    create_event_table()

    # --- Add Event Section (Using Separate Forms) ---
    st.subheader("➕ Add New Event")
    available_portcos = get_portco_names_from_db()

    # Only proceed if companies are available
    if not available_portcos or available_portcos[0].startswith("Error:"):
        st.warning(f"Cannot add Events: {available_portcos[0]}")
    else:
        # Event Type selection OUTSIDE the forms
        add_event_type = st.selectbox(
            "Select Event Type to Add:",
            options=["Select..."] + EVENT_TYPES,
            key="add_event_type_select"
        )

        # Conditionally display the correct form
        if add_event_type == "Exit":
            with st.form("add_exit_form", clear_on_submit=True):
                st.markdown("**Record Exit Event**")
                add_portco_name = st.selectbox("Company*", available_portcos, index=None, placeholder="Select Company...")
                add_event_date = st.date_input("Date*", value=date.today(), max_value=date.today())
                add_cash_flow = st.number_input("Proceeds Received*", value=None, format="%.2f", help="Cash received by the fund from selling shares.")
                add_percent_sold = st.number_input("Percent Holding Sold* (0.0 to 1.0)", min_value=0.0, max_value=1.0, value=None, format="%.4f", help="Fraction of the fund's stake sold (e.g., 0.25 for 25%).")
                add_remaining_valuation = st.number_input("Valuation of Remaining Holding*", value=0.0, format="%.2f", help="Value of the fund's stake NOT sold, after the exit (0 if full exit).")
                add_currency = st.selectbox("Currency", options=CURRENCY_OPTIONS, index=0)
                add_event_notes = st.text_area("Notes", placeholder="Exit details, buyer, etc.")
                submitted = st.form_submit_button("Add Exit Event")
                if submitted:
                    is_valid = True
                    if not add_portco_name: st.error("Company required."); is_valid = False
                    if not add_event_date: st.error("Date required."); is_valid = False
                    if add_cash_flow is None: st.error("Proceeds required."); is_valid = False
                    if add_percent_sold is None: st.error("% Sold required."); is_valid = False
                    if add_remaining_valuation is None: st.error("Remaining Valuation required."); is_valid = False
                    if is_valid:
                        if add_event(add_portco_name, 'Exit', add_event_date, add_cash_flow, add_currency, add_percent_sold, add_remaining_valuation, add_event_notes):
                            st.rerun()

        elif add_event_type == "Dividend":
            with st.form("add_dividend_form", clear_on_submit=True):
                st.markdown("**Record Dividend/Distribution**")
                add_portco_name = st.selectbox("Company*", available_portcos, index=None, placeholder="Select Company...")
                add_event_date = st.date_input("Date*", value=date.today(), max_value=date.today())
                add_cash_flow = st.number_input("Amount Received*", value=None, format="%.2f", help="Cash distribution received by the fund.")
                add_currency = st.selectbox("Currency", options=CURRENCY_OPTIONS, index=0)
                add_event_notes = st.text_area("Notes", placeholder="Dividend details, per share amount, etc.")
                # Decide if valuation update is needed post-dividend
                add_holding_valuation = st.number_input("Post-Dividend Holding Valuation (Optional)", value=None, format="%.2f", help="Update valuation only if dividend significantly impacts it.")
                submitted = st.form_submit_button("Add Dividend Event")
                if submitted:
                    is_valid = True
                    if not add_portco_name: st.error("Company required."); is_valid = False
                    if not add_event_date: st.error("Date required."); is_valid = False
                    if add_cash_flow is None: st.error("Amount Received required."); is_valid = False
                    if is_valid:
                        # Pass None for fields not applicable to Dividend
                        if add_event(add_portco_name, 'Dividend', add_event_date, add_cash_flow, add_currency, None, add_holding_valuation, add_event_notes):
                            st.rerun()

        elif add_event_type == "Valuation Update":
            with st.form("add_valuation_form", clear_on_submit=True):
                st.markdown("**Record Valuation Update**")
                add_portco_name = st.selectbox("Company*", available_portcos, index=None, placeholder="Select Company...")
                add_event_date = st.date_input("Date*", value=date.today(), max_value=date.today())
                add_holding_valuation = st.number_input("New Fund Holding Valuation*", value=None, format="%.2f", help="The fund's assessed value of its entire holding on this date.")
                add_currency = st.selectbox("Currency", options=CURRENCY_OPTIONS, index=0)
                add_event_notes = st.text_area("Notes", placeholder="Source of valuation (e.g., 409A, new round), methodology, etc.")
                submitted = st.form_submit_button("Add Valuation Event")
                if submitted:
                    is_valid = True
                    if not add_portco_name: st.error("Company required."); is_valid = False
                    if not add_event_date: st.error("Date required."); is_valid = False
                    if add_holding_valuation is None: st.error("Holding Valuation required."); is_valid = False
                    if is_valid:
                         # Pass None for fields not applicable to Valuation Update
                        if add_event(add_portco_name, 'Valuation Update', add_event_date, None, add_currency, None, add_holding_valuation, add_event_notes):
                            st.rerun()

    # --- Display Table of All Events ---
    st.divider()
    st.subheader("All Recorded Events")
    df_all_events = get_all_events_with_portco_id()
    st.markdown(f"**{len(df_all_events)}** total event records found.")

    if df_all_events.empty:
        st.info("ℹ️ No Events recorded yet.")
    else:
        df_display = df_all_events.copy()

        # Create clickable company links
        df_display['Company'] = df_display.apply(
            lambda row: f"[{row['portco_name']}](/?page=company&id={row['portco_id']})" if row['portco_id'] != -1 else row['portco_name'],
            axis=1
        )
        # Format other fields for display
        df_display['Date'] = df_display['event_date'].dt.strftime('%Y-%m-%d').fillna('N/A')
        df_display['Cash Flow'] = df_display.apply(lambda row: format_currency(row['cash_flow_amount'], row['currency']), axis=1).fillna("-")
        df_display['% Sold'] = df_display['percent_holding_sold'].apply(lambda x: f"{x:.1%}" if pd.notna(x) else "-")
        df_display['Holding Val'] = df_display.apply(lambda row: format_currency(row['fund_holding_valuation'], row['currency']), axis=1).fillna("-")

        # Prepare DataFrame for Markdown Table
        display_columns_map = {
            'id': 'Event ID',
            'Company': 'Company',
            'Date': 'Date',
            'event_type': 'Type',
            'Cash Flow': 'Cash Flow',
            '% Sold': '% Sold',
            'Holding Val': 'Post-Event Val',
            'notes': 'Notes'
        }
        df_to_show = df_display[list(display_columns_map.keys())].rename(columns=display_columns_map)

        # Use st.markdown to render links correctly
        st.markdown(
            df_to_show.to_markdown(index=False),
            unsafe_allow_html=True
        )

# --- Main Execution ---
render_events_dashboard()