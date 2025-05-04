# Yang_Portfolio.py (Main Application Script)

import streamlit as st
import sqlite3
import pandas as pd
import time
from datetime import datetime, date
import numpy_financial as npf

# -- Configuration (Combined from all parts) --
DB_NAME = 'Yang.db'
PORTCO_TABLE_NAME = 'list_of_portco'
INVESTMENT_TABLE_NAME = 'investments'
KPI_TABLE_NAME = 'kpis'
EVENT_TABLE_NAME = 'events'

# Portfolio Company Config
INDUSTRY_LIST = ['Energy','Materials', 'Industrials',
                 'Consumer Discretionary', 'Consumer Staples', 'Health Care',
                 'Financials', 'Information Technology', 'Communication Services',
                 'Utilities', 'Real Estate']
COUNTRY_LIST = ['China','Germany','India','Singapore','United Kingdom', 'United States']
STATUS_OPTIONS = ['Active', 'Inactive']

# Investment Config
LIST_OF_FUNDS = sorted([f'Yang Fund {i+1}' for i in range(3)])
LIST_OF_TYPES_OF_INVESTMENTS = sorted(["Debt", "Equity", "SAFE Note", "Convertible Note", "Warrants"])
ROUND_STAGES = ['Pre-Seed', 'Seed', 'Series A', 'Series B', 'Series C', 'Series D+', 'Growth Equity', 'Mezzanine', 'Other']

# Event Config
EVENT_TYPES = ['Exit', 'Dividend', 'Valuation Update']
CURRENCY_OPTIONS = ['USD', 'SGD', 'EUR', 'GBP', 'Other']

# --- Database Functions (Combined & Centralized) ---

def db_connect():
    """Establishes database connection and enables Foreign Keys."""
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

# --- Portfolio Company DB Functions ---
def create_portco_table():
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
                conn.commit(); print(f"'{PORTCO_TABLE_NAME}': Status column added.")
            except sqlite3.Error as e_alter:
                 if "duplicate column name" not in str(e_alter): print(f"Warning: Could not add 'status' column to {PORTCO_TABLE_NAME}. {e_alter}")
    except sqlite3.Error as e: print(f"Warning during {PORTCO_TABLE_NAME} creation/check: {e}")
    finally:
        if conn: conn.close()

def get_portco_by_id(portco_id):
    conn = db_connect()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute(f"SELECT * FROM {PORTCO_TABLE_NAME} WHERE id = ?", (portco_id,))
        data = c.fetchone()
        return dict(data) if data else None
    except sqlite3.Error as e: st.error(f"DB error fetching company ID {portco_id}: {e}"); return None
    finally:
        if conn: conn.close()

def update_portco(portco_id, portco_name, year_founded, industry_classification_str, establishment_country_str, status):
    if status not in STATUS_OPTIONS: st.error(f"Invalid status: {status}"); return False
    conn = db_connect()
    c = conn.cursor()
    try:
        c.execute(f'''UPDATE {PORTCO_TABLE_NAME}
                     SET portco_name = ?, year_founded = ?, industry_classification = ?, establishment_country = ?, status = ?
                     WHERE id = ?
                  ''', (portco_name, year_founded, industry_classification_str, establishment_country_str, status, portco_id))
        conn.commit(); st.toast(f"✅ Updated '{portco_name}'!", icon="✅"); return True
    except sqlite3.IntegrityError as e:
         if "UNIQUE constraint failed" in str(e): st.error(f"Error: Name '{portco_name}' might already exist.")
         elif "CHECK constraint failed" in str(e): st.error(f"Error: Invalid status value ('{status}').")
         else: st.error(f"DB integrity error updating company: {e}")
         return False
    except sqlite3.Error as e: st.error(f"DB error updating company: {e}"); return False
    finally:
        if conn: conn.close()

def delete_portco(portco_id):
    """Deletes a portfolio company by its ID (Cascade should delete investments and KPIs)."""
    conn = db_connect()
    c = conn.cursor()
    portco_name = f"ID {portco_id}"
    try:
        c.execute(f"SELECT portco_name FROM {PORTCO_TABLE_NAME} WHERE id = ?", (portco_id,))
        result = c.fetchone()
        if result: portco_name = result[0]
        else: st.warning(f"Company ID {portco_id} not found."); return False
        c.execute(f"DELETE FROM {PORTCO_TABLE_NAME} WHERE id = ?", (portco_id,))
        conn.commit()
        if c.rowcount > 0: st.toast(f"✅ Deleted '{portco_name}' and associated records.", icon="✅"); return True
        else: st.warning(f"Company ID {portco_id} targeted, but deletion affected 0 rows."); return False
    except sqlite3.IntegrityError as e:
        st.error(f"Database integrity error deleting '{portco_name}': {e}. Check Foreign Key constraints and linked data.")
        if conn: conn.rollback()
        return False
    except sqlite3.Error as e:
        st.error(f"General database error during company delete for '{portco_name}': {e}")
        if conn: conn.rollback()
        return False
    finally:
        if conn: conn.close()

# --- Investment DB Functions ---
def create_investment_table():
    conn = db_connect()
    c = conn.cursor()
    try:
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {INVESTMENT_TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_name TEXT NOT NULL,
                portco_name TEXT NOT NULL COLLATE NOCASE,
                type_of_investment TEXT,
                investment_round_number INTEGER NOT NULL,
                round_stage TEXT,
                date_of_investment TEXT NOT NULL, -- Store as YYYY-MM-DD
                size_of_investment REAL NOT NULL,
                total_round_size REAL,
                post_money_valuation REAL,
                FOREIGN KEY (portco_name) REFERENCES {PORTCO_TABLE_NAME}(portco_name) ON DELETE CASCADE ON UPDATE CASCADE
            ) ''')
        conn.commit()
        c.execute(f"PRAGMA table_info({INVESTMENT_TABLE_NAME})")
        columns = {col[1]: col[2] for col in c.fetchall()}
        if 'round_stage' not in columns:
            try:
                c.execute(f"ALTER TABLE {INVESTMENT_TABLE_NAME} ADD COLUMN round_stage TEXT")
                conn.commit(); print(f"'{INVESTMENT_TABLE_NAME}': 'round_stage' added.")
            except sqlite3.Error as e_alter:
                 if "duplicate column name" not in str(e_alter): print(f"Warning: Could not add 'round_stage' to {INVESTMENT_TABLE_NAME}. {e_alter}")
    except sqlite3.Error as e: print(f"Warning during {INVESTMENT_TABLE_NAME} creation/check: {e}")
    finally:
        if conn: conn.close()

def get_investments_by_company_name(company_name):
    conn = db_connect()
    try:
        df = pd.read_sql_query(f"""
            SELECT id, fund_name, type_of_investment, investment_round_number, round_stage,
                   date_of_investment, size_of_investment, total_round_size, post_money_valuation
            FROM {INVESTMENT_TABLE_NAME} WHERE portco_name = ? COLLATE NOCASE ORDER BY date_of_investment DESC, id DESC
            """, conn, params=(company_name,))
        df['date_of_investment'] = pd.to_datetime(df['date_of_investment'], errors='coerce')
        df['investment_round_number']= pd.to_numeric(df['investment_round_number'], errors='coerce').astype(pd.Int64Dtype())
        df['size_of_investment']= pd.to_numeric(df['size_of_investment'], errors='coerce')
        df['total_round_size']= pd.to_numeric(df['total_round_size'], errors='coerce')
        df['post_money_valuation']= pd.to_numeric(df['post_money_valuation'], errors='coerce')
        return df
    except Exception as e: st.error(f"Error reading investments for {company_name}: {e}"); return pd.DataFrame()
    finally:
        if conn: conn.close()

def get_investment_by_id(investment_id):
    conn = db_connect()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute(f"SELECT * FROM {INVESTMENT_TABLE_NAME} WHERE id = ?", (investment_id,))
        data = c.fetchone()
        if data:
            investment_dict = dict(data)
            try:
                if investment_dict.get('date_of_investment'): investment_dict['date_of_investment'] = datetime.strptime(investment_dict['date_of_investment'], '%Y-%m-%d').date()
            except ValueError: investment_dict['date_of_investment'] = None
            investment_dict['type_of_investment_list'] = parse_string_list(investment_dict.get('type_of_investment'))
            return investment_dict
        else: return None
    except sqlite3.Error as e: st.error(f"DB error fetching investment ID {investment_id}: {e}"); return None
    finally:
        if conn: conn.close()

def update_investment(investment_id, fund_name, type_of_investment_str, investment_round_number, round_stage, date_of_investment_obj, size_of_investment, total_round_size, post_money_valuation):
    conn = db_connect()
    c = conn.cursor()
    date_str = date_of_investment_obj.strftime('%Y-%m-%d') if date_of_investment_obj else None
    total_round_size_db = total_round_size if total_round_size is not None else None
    post_money_valuation_db = post_money_valuation if post_money_valuation is not None else None
    try:
        c.execute(f'''UPDATE {INVESTMENT_TABLE_NAME}
                     SET fund_name = ?, type_of_investment = ?, investment_round_number = ?, round_stage = ?, date_of_investment = ?, size_of_investment = ?, total_round_size = ?, post_money_valuation = ?
                     WHERE id = ?
                  ''', (fund_name, type_of_investment_str, investment_round_number, round_stage, date_str, size_of_investment, total_round_size_db, post_money_valuation_db, investment_id))
        conn.commit()
        if c.rowcount > 0: st.toast(f"✅ Updated Investment ID {investment_id}!", icon="✅"); return True
        else: st.warning(f"Investment ID {investment_id} not found for update."); return False
    except sqlite3.Error as e: st.error(f"DB error updating investment: {e}"); return False
    finally:
        if conn: conn.close()

def delete_investment(investment_id):
    conn = db_connect()
    c = conn.cursor()
    try:
        c.execute(f"DELETE FROM {INVESTMENT_TABLE_NAME} WHERE id = ?", (investment_id,))
        conn.commit()
        if c.rowcount > 0: st.toast(f"✅ Deleted Investment ID {investment_id}.", icon="✅"); return True
        else: st.warning(f"Investment ID {investment_id} not found for deletion."); return False
    except sqlite3.Error as e: st.error(f"DB error deleting investment: {e}"); return False
    finally:
        if conn: conn.close()

# --- KPI DB Functions ---
def create_kpi_table():
    conn = db_connect()
    c = conn.cursor()
    try:
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {KPI_TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portco_name TEXT NOT NULL COLLATE NOCASE,
                kpi_name TEXT NOT NULL COLLATE NOCASE,
                kpi_value REAL,
                kpi_date TEXT NOT NULL, units TEXT, notes TEXT,
                FOREIGN KEY (portco_name) REFERENCES {PORTCO_TABLE_NAME}(portco_name) ON DELETE CASCADE ON UPDATE CASCADE
            ) ''')
        conn.commit()
        c.execute(f"CREATE INDEX IF NOT EXISTS idx_kpi_portco_date ON {KPI_TABLE_NAME} (portco_name, kpi_date);")
        c.execute(f"CREATE INDEX IF NOT EXISTS idx_kpi_name ON {KPI_TABLE_NAME} (kpi_name);")
        conn.commit()
    except sqlite3.Error as e: print(f"Warning during {KPI_TABLE_NAME} creation/check: {e}")
    finally:
        if conn: conn.close()

def get_kpis_by_company_name(company_name):
    conn = db_connect()
    try:
        df = pd.read_sql_query(f"""
            SELECT id, kpi_name, kpi_value, kpi_date, units, notes FROM {KPI_TABLE_NAME}
            WHERE portco_name = ? COLLATE NOCASE ORDER BY kpi_date DESC, kpi_name COLLATE NOCASE
            """, conn, params=(company_name,))
        df['kpi_date'] = pd.to_datetime(df['kpi_date'], errors='coerce')
        df['kpi_value'] = pd.to_numeric(df['kpi_value'], errors='coerce')
        df.dropna(subset=['kpi_date'], inplace=True)
        return df
    except Exception as e: st.error(f"Error reading KPIs for {company_name}: {e}"); return pd.DataFrame()
    finally:
        if conn: conn.close()

def get_kpi_by_id(kpi_id):
    conn = db_connect()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute(f"SELECT * FROM {KPI_TABLE_NAME} WHERE id = ?", (kpi_id,))
        data = c.fetchone()
        if data:
            kpi_dict = dict(data)
            try:
                if kpi_dict.get('kpi_date'): kpi_dict['kpi_date'] = datetime.strptime(kpi_dict['kpi_date'], '%Y-%m-%d').date()
            except ValueError: kpi_dict['kpi_date'] = None
            return kpi_dict
        else: return None
    except sqlite3.Error as e: st.error(f"DB error fetching KPI ID {kpi_id}: {e}"); return None
    finally:
        if conn: conn.close()

def update_kpi(kpi_id, kpi_name, kpi_value, kpi_date_obj, units, notes):
    conn = db_connect()
    c = conn.cursor()
    date_str = kpi_date_obj.strftime('%Y-%m-%d') if kpi_date_obj else None
    value_to_db = kpi_value if kpi_value is not None else None
    units_to_db = units.strip() if units else None
    notes_to_db = notes.strip() if notes else None
    try:
        c.execute(f'''UPDATE {KPI_TABLE_NAME}
                     SET kpi_name = ?, kpi_value = ?, kpi_date = ?, units = ?, notes = ? WHERE id = ?
                  ''', (kpi_name, value_to_db, date_str, units_to_db, notes_to_db, kpi_id))
        conn.commit()
        if c.rowcount > 0: st.toast(f"✅ Updated KPI ID {kpi_id}!", icon="✅"); return True
        else: st.warning(f"KPI ID {kpi_id} not found for update."); return False
    except sqlite3.Error as e: st.error(f"DB error updating KPI: {e}"); return False
    finally:
        if conn: conn.close()

def delete_kpi(kpi_id):
    conn = db_connect()
    c = conn.cursor()
    try:
        c.execute(f"DELETE FROM {KPI_TABLE_NAME} WHERE id = ?", (kpi_id,))
        conn.commit()
        if c.rowcount > 0: st.toast(f"✅ Deleted KPI ID {kpi_id}.", icon="✅"); return True
        else: st.warning(f"KPI ID {kpi_id} not found for deletion."); return False
    except sqlite3.Error as e: st.error(f"DB error deleting KPI: {e}"); return False
    finally:
        if conn: conn.close()

# --- Event DB Functions ---
def create_event_table():
    conn = db_connect()
    c = conn.cursor()
    try:
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {EVENT_TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portco_name TEXT NOT NULL COLLATE NOCASE, event_date TEXT NOT NULL,
                event_type TEXT NOT NULL CHECK(event_type IN {tuple(EVENT_TYPES)}),
                cash_flow_amount REAL, currency TEXT DEFAULT 'USD', percent_holding_sold REAL,
                fund_holding_valuation REAL, notes TEXT,
                FOREIGN KEY (portco_name) REFERENCES {PORTCO_TABLE_NAME}(portco_name) ON DELETE CASCADE ON UPDATE CASCADE
            ) ''')
        conn.commit()
        c.execute(f"CREATE INDEX IF NOT EXISTS idx_event_portco_date ON {EVENT_TABLE_NAME} (portco_name, event_date);")
        conn.commit()
    except sqlite3.Error as e: print(f"Warning during {EVENT_TABLE_NAME} creation/check: {e}")
    finally:
        if conn: conn.close()

def get_events_by_company_name(company_name):
    conn = db_connect()
    try:
        df = pd.read_sql_query(f"""
            SELECT id, event_date, event_type, cash_flow_amount, currency,
                   percent_holding_sold, fund_holding_valuation, notes
            FROM {EVENT_TABLE_NAME} WHERE portco_name = ? COLLATE NOCASE ORDER BY event_date DESC, id DESC
            """, conn, params=(company_name,))
        df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
        df['cash_flow_amount'] = pd.to_numeric(df['cash_flow_amount'], errors='coerce')
        df['percent_holding_sold'] = pd.to_numeric(df['percent_holding_sold'], errors='coerce')
        df['fund_holding_valuation'] = pd.to_numeric(df['fund_holding_valuation'], errors='coerce')
        return df
    except Exception as e: st.error(f"Error reading Events for {company_name}: {e}"); return pd.DataFrame()
    finally:
        if conn: conn.close()

def get_event_by_id(event_id):
    conn = db_connect()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute(f"SELECT * FROM {EVENT_TABLE_NAME} WHERE id = ?", (event_id,))
        data = c.fetchone()
        if data:
            event_dict = dict(data)
            try:
                if event_dict.get('event_date'): event_dict['event_date'] = datetime.strptime(event_dict['event_date'], '%Y-%m-%d').date()
            except ValueError: event_dict['event_date'] = None
            return event_dict
        else: return None
    except sqlite3.Error as e: st.error(f"DB error fetching Event ID {event_id}: {e}"); return None
    finally:
        if conn: conn.close()

def update_event(event_id, event_type, event_date_obj, cash_flow_amount, currency, percent_holding_sold, fund_holding_valuation, notes):
    conn = db_connect()
    c = conn.cursor()
    date_str = event_date_obj.strftime('%Y-%m-%d') if event_date_obj else None
    cf = cash_flow_amount if cash_flow_amount is not None else None
    phs = percent_holding_sold if percent_holding_sold is not None else None
    fhv = fund_holding_valuation if fund_holding_valuation is not None else None
    curr = currency.strip() if currency else 'USD'
    nts = notes.strip() if notes else None
    try:
        c.execute(f'''UPDATE {EVENT_TABLE_NAME}
                     SET event_type = ?, event_date = ?, cash_flow_amount = ?, currency = ?,
                         percent_holding_sold = ?, fund_holding_valuation = ?, notes = ?
                     WHERE id = ?
                  ''', (event_type, date_str, cf, curr, phs, fhv, nts, event_id))
        conn.commit()
        if c.rowcount > 0: st.toast(f"✅ Updated Event ID {event_id}!", icon="✅"); return True
        else: st.warning(f"Event ID {event_id} not found for update."); return False
    except sqlite3.Error as e: st.error(f"DB error updating Event: {e}"); return False
    finally:
        if conn: conn.close()

def delete_event(event_id):
    conn = db_connect()
    c = conn.cursor()
    try:
        c.execute(f"DELETE FROM {EVENT_TABLE_NAME} WHERE id = ?", (event_id,))
        conn.commit()
        if c.rowcount > 0: st.toast(f"✅ Deleted Event ID {event_id}.", icon="✅"); return True
        else: st.warning(f"Event ID {event_id} not found for deletion."); return False
    except sqlite3.Error as e: st.error(f"DB error deleting Event: {e}"); return False
    finally:
        if conn: conn.close()

# --- DB Functions for Homepage ---
def get_portfolio_summary_stats():
    stats = { "total_companies": 0, "active_companies": 0, "total_invested": 0.0, "current_value": 0.0 }
    conn = db_connect(); c = conn.cursor()
    try:
        c.execute(f"SELECT status, COUNT(*) FROM {PORTCO_TABLE_NAME} GROUP BY status"); status_counts = dict(c.fetchall()); stats["active_companies"] = status_counts.get('Active', 0); stats["total_companies"] = sum(status_counts.values())
        c.execute(f"SELECT SUM(size_of_investment) FROM {INVESTMENT_TABLE_NAME}"); total_invested_result = c.fetchone()[0]; stats["total_invested"] = total_invested_result or 0.0
        c.execute(f"SELECT portco_name FROM {PORTCO_TABLE_NAME} WHERE status = 'Active'"); active_companies = [row[0] for row in c.fetchall()]
        if active_companies:
            placeholders = ','.join('?'*len(active_companies))
            query_latest_val = f"""SELECT SUM(fund_holding_valuation) FROM ( SELECT portco_name, fund_holding_valuation, ROW_NUMBER() OVER(PARTITION BY portco_name ORDER BY event_date DESC, id DESC) as rn FROM {EVENT_TABLE_NAME} WHERE portco_name IN ({placeholders}) AND fund_holding_valuation IS NOT NULL ) WHERE rn = 1;"""
            c.execute(query_latest_val, active_companies); current_value_result = c.fetchone()[0]; stats["current_value"] = current_value_result or 0.0
    except Exception as e: st.warning(f"Could not calculate summary stats: {e}")
    finally:
        if conn: conn.close()
    return stats

def get_recent_investments(limit=5):
    conn = db_connect()
    try:
        query = f"SELECT i.id, i.portco_name, i.date_of_investment, i.size_of_investment, p.id as portco_id FROM {INVESTMENT_TABLE_NAME} i LEFT JOIN {PORTCO_TABLE_NAME} p ON i.portco_name = p.portco_name COLLATE NOCASE ORDER BY i.date_of_investment DESC, i.id DESC LIMIT ?"
        df = pd.read_sql_query(query, conn, params=(limit,)); df['date_of_investment'] = pd.to_datetime(df['date_of_investment']).dt.date; df['portco_id'] = df['portco_id'].fillna(-1).astype(int); return df
    except Exception as e: st.warning(f"Could not fetch recent investments: {e}"); return pd.DataFrame()
    finally:
        if conn: conn.close()

def get_recent_events(limit=5):
    conn = db_connect()
    try:
        query = f"SELECT e.id, e.portco_name, e.event_date, e.event_type, e.notes, p.id as portco_id FROM {EVENT_TABLE_NAME} e LEFT JOIN {PORTCO_TABLE_NAME} p ON e.portco_name = p.portco_name COLLATE NOCASE ORDER BY e.event_date DESC, e.id DESC LIMIT ?"
        df = pd.read_sql_query(query, conn, params=(limit,)); df['event_date'] = pd.to_datetime(df['event_date']).dt.date; df['portco_id'] = df['portco_id'].fillna(-1).astype(int); return df
    except Exception as e: st.warning(f"Could not fetch recent events: {e}"); return pd.DataFrame()
    finally:
        if conn: conn.close()

# --- Helper Functions ---
def parse_string_list(string_list):
    if not string_list: return []
    return [item.strip() for item in string_list.split(',') if item.strip()]

def format_currency(value, currency_symbol='$'):
    if pd.isna(value) or value is None: return None
    try: return f"{currency_symbol}{float(value):,.2f}"
    except (ValueError, TypeError): return "Invalid Format"

# --- Streamlit Page Configuration ---
st.set_page_config(layout="wide", page_title="Yang Portfolio")

# --- Render Functions ---

def render_company_page(company_id):
    """Displays the unified page for a specific company: Details, Investments, KPIs, Events, Manage All."""
    if st.button("⬅️ Back"): st.query_params.clear(); st.rerun()
    st.divider(); company_data = get_portco_by_id(company_id);
    if not company_data: st.error(f"Company with ID {company_id} not found."); return
    st.title(f"{company_data['portco_name']}")

    # --- Company Details & Update ---
    st.subheader("Company Information")
    info_cols = st.columns(4); info_cols[0].metric("ID", company_data['id']); info_cols[1].metric("Year Founded", company_data.get('year_founded', 'N/A')); info_cols[2].metric("Country", company_data.get('establishment_country', 'N/A')); info_cols[3].metric("Status", company_data.get('status', 'N/A'))
    st.write("**Industry Classification(s):**"); industries = parse_string_list(company_data.get('industry_classification', ''));
    if industries: st.multiselect("Industries", options=industries, default=industries, disabled=True, label_visibility="collapsed")
    else: st.caption("N/A")
    with st.expander("✏️ Update Company Details", expanded=False):
        with st.form(f"portco_form_update_{company_id}", clear_on_submit=False):
            st.subheader("Edit Information")
            update_portco_name = st.text_input("Company Name*", value=company_data['portco_name'])
            update_year_founded = st.slider("Founded in Year*", 2000, datetime.now().year + 1, value=company_data.get('year_founded', datetime.now().year))
            current_industries = parse_string_list(company_data.get('industry_classification', ''))
            update_industry_list = st.multiselect("Industry Classification(s)*", INDUSTRY_LIST, default=current_industries)
            try: default_country = company_data.get('establishment_country'); country_index = COUNTRY_LIST.index(default_country) if default_country and default_country in COUNTRY_LIST else None
            except ValueError: country_index = None
            update_establishment_country = st.selectbox("Established in*", COUNTRY_LIST, index=country_index, placeholder="Select country...") # Now uses updated COUNTRY_LIST
            try: default_status = company_data.get('status'); status_index = STATUS_OPTIONS.index(default_status) if default_status and default_status in STATUS_OPTIONS else 0
            except ValueError: status_index = 0
            update_status = st.selectbox("Status*", STATUS_OPTIONS, index=status_index)
            submitted_update = st.form_submit_button("Save Company Changes", type="primary")
            if submitted_update:
                is_valid_update = True
                if not update_portco_name: st.error("Company Name required."); is_valid_update = False
                if not update_industry_list: st.error("Industry required."); is_valid_update = False
                if not update_establishment_country: st.error("Country required."); is_valid_update = False
                if is_valid_update:
                    industry_str_update = ", ".join(sorted(update_industry_list))
                    if update_portco(company_id, update_portco_name.strip(), update_year_founded, industry_str_update, update_establishment_country, update_status): st.rerun()

    st.divider()
    # --- Investment History & Management Expander ---
    st.subheader("Investment History"); df_investments_raw = get_investments_by_company_name(company_data['portco_name'])
    if not df_investments_raw.empty:
        df_inv_disp = df_investments_raw.copy()
        money_cols_inv = ['size_of_investment', 'total_round_size', 'post_money_valuation']
        for col in money_cols_inv: df_inv_disp[col] = df_inv_disp[col].apply(format_currency)
        inv_cols_cfg = {"id": st.column_config.NumberColumn("Inv. ID"), "fund_name": st.column_config.TextColumn("Fund"), "date_of_investment": st.column_config.DateColumn("Date", format="YYYY-MM-DD"), "investment_round_number": st.column_config.NumberColumn("Round #", format="%d"), "round_stage": st.column_config.TextColumn("Stage"), "size_of_investment": st.column_config.TextColumn("Investment (USD)"), "type_of_investment": st.column_config.TextColumn("Type(s)"), "total_round_size": st.column_config.TextColumn("Round Size (USD)"), "post_money_valuation": st.column_config.TextColumn("Post-Money Val (USD)")}
        st.dataframe(df_inv_disp, column_config=inv_cols_cfg, hide_index=True, use_container_width=True)
        with st.expander("Manage Investments"): # Investment CRUD
            inv_opts = {f"ID {r['id']} ({pd.to_datetime(r['date_of_investment']):%Y-%m-%d if pd.notna(r['date_of_investment']) else 'N/A'})": r['id'] for _, r in df_investments_raw.iterrows()}; sel_inv_lbl = st.selectbox("Select Inv:", [None]+list(inv_opts.keys()), format_func=lambda x: x or "Select...", key=f"mng_inv_{company_id}"); sel_inv_id = inv_opts.get(sel_inv_lbl)
            if sel_inv_id: # Update/Delete Forms
                inv_data = get_investment_by_id(sel_inv_id)
                if inv_data:
                    col1, col2 = st.columns(2)
                    with col1, st.form(f"inv_upd_{sel_inv_id}", clear_on_submit=False):
                         st.write(f"**Update Inv ID: {sel_inv_id}**");
                         fnd = inv_data.get('fund_name'); fnd_idx = LIST_OF_FUNDS.index(fnd) if fnd and fnd in LIST_OF_FUNDS else None; u_fnd = st.selectbox("Fund*", LIST_OF_FUNDS, index=fnd_idx, placeholder="Select..."); stg = inv_data.get('round_stage'); stg_idx = ROUND_STAGES.index(stg) if stg and stg in ROUND_STAGES else None; u_stg = st.selectbox("Stage*", ROUND_STAGES, index=stg_idx, placeholder="Select..."); u_types = st.multiselect("Type(s)", LIST_OF_TYPES_OF_INVESTMENTS, default=inv_data.get('type_of_investment_list',[])); u_rnd = st.number_input("Rnd #*", 1, value=inv_data.get('investment_round_number',1)); u_dt = st.date_input("Date*", value=inv_data.get('date_of_investment')); u_sz = st.number_input("Size*", 0.01, value=inv_data.get('size_of_investment'), format="%.2f"); u_tsz = st.number_input("Total Rnd Size", 0.0, value=inv_data.get('total_round_size'), format="%.2f"); u_pmv = st.number_input("Post-Money", 0.0, value=inv_data.get('post_money_valuation'), format="%.2f");
                         if st.form_submit_button("Save"): u_types_str = ", ".join(sorted(u_types)); update_investment(sel_inv_id, u_fnd, u_types_str, u_rnd, u_stg, u_dt, u_sz, u_tsz, u_pmv); st.rerun()
                    with col2:
                         st.write(f"**Delete Inv ID: {sel_inv_id}**"); cfm_del = st.checkbox("Confirm", key=f"d_inv_{sel_inv_id}");
                         if st.button("Delete", type="primary", disabled=not cfm_del, key=f"d_inv_b_{sel_inv_id}"): delete_investment(sel_inv_id); st.rerun()
    else: st.info("No investment records found.")

    st.divider()
    # KPI History, Management & Graph...
    st.subheader("Key Performance Indicators (KPIs)"); df_kpis_raw = get_kpis_by_company_name(company_data['portco_name'])
    if not df_kpis_raw.empty:
        df_kpi_disp = df_kpis_raw.copy(); df_kpi_disp['kpi_value_num'] = pd.to_numeric(df_kpi_disp['kpi_value'], errors='coerce'); df_kpi_disp['kpi_date'] = pd.to_datetime(df_kpi_disp['kpi_date'], errors='coerce'); df_kpi_disp['Value'] = df_kpi_disp.apply( lambda r: f"{r['kpi_value_num']:,.2f} {r['units']}" if pd.notna(r['kpi_value_num']) and r['units'] else f"{r['kpi_value_num']:,.2f}" if pd.notna(r['kpi_value_num']) else "N/A", axis=1); df_kpi_disp['Date'] = df_kpi_disp['kpi_date'].dt.strftime('%Y-%m-%d').fillna('N/A');
        kpi_cols_cfg = {"id": st.column_config.NumberColumn("KPI ID"),"Date": st.column_config.TextColumn("Date"),"kpi_name": st.column_config.TextColumn("KPI Name"),"Value": st.column_config.TextColumn("Value"),"notes": st.column_config.TextColumn("Notes"),"kpi_value": None,"kpi_value_num": None,"units": None,"kpi_date": None}
        kpi_column_order = ["id", "Date", "kpi_name", "Value", "notes"]
        st.dataframe( df_kpi_disp, column_config=kpi_cols_cfg, hide_index=True, use_container_width=True, column_order=kpi_column_order)
        with st.expander("Manage KPIs"): # KPI CRUD
            kpi_opts = {f"ID {r['id']} ({pd.to_datetime(r['kpi_date']):%Y-%m-%d if pd.notna(r['kpi_date']) else 'N/A'}) {r['kpi_name']}": r['id'] for _, r in df_kpis_raw.iterrows()}; sel_kpi_lbl = st.selectbox("Select KPI:", [None]+list(kpi_opts.keys()), format_func=lambda x: x or "Select...", key=f"mng_kpi_{company_id}"); sel_kpi_id = kpi_opts.get(sel_kpi_lbl)
            if sel_kpi_id: # Update/Delete Forms
                 kpi_data = get_kpi_by_id(sel_kpi_id)
                 if kpi_data:
                    col1, col2 = st.columns(2)
                    with col1, st.form(f"kpi_upd_{sel_kpi_id}", clear_on_submit=False):
                         st.write(f"**Update KPI ID: {sel_kpi_id}**"); u_nm = st.text_input("Name*", value=kpi_data.get('kpi_name','')); u_val = st.number_input("Value*", value=kpi_data.get('kpi_value'), format="%.2f"); u_un = st.text_input("Units", value=kpi_data.get('units','')); u_dt = st.date_input("Date*", value=kpi_data.get('kpi_date')); u_nt = st.text_area("Notes", value=kpi_data.get('notes',''));
                         if st.form_submit_button("Save"): # Add validation...
                              update_kpi(sel_kpi_id, u_nm, u_val, u_dt, u_un, u_nt); st.rerun()
                    with col2:
                         st.write(f"**Delete KPI ID: {sel_kpi_id}**"); cfm_del = st.checkbox("Confirm", key=f"d_kpi_{sel_kpi_id}");
                         if st.button("Delete", type="primary", disabled=not cfm_del, key=f"d_kpi_b_{sel_kpi_id}"): delete_kpi(sel_kpi_id); st.rerun()
        # KPI Graph
        st.divider(); st.subheader("KPI Trend Graph")
        num_kpis = df_kpi_disp.dropna(subset=['kpi_value_num', 'kpi_date']); avail_kpis = sorted(num_kpis['kpi_name'].unique())
        if avail_kpis: sel_kpi_gr = st.selectbox("Select KPI:", ["Select..."]+avail_kpis, key=f"kpi_gr_{company_id}")
        else: st.caption("No numeric KPIs available."); sel_kpi_gr = None
        if sel_kpi_gr and sel_kpi_gr != "Select...":
             gr_df = num_kpis[num_kpis['kpi_name']==sel_kpi_gr].sort_values('kpi_date');
             if len(gr_df)>1: st.line_chart(gr_df.set_index('kpi_date')[['kpi_value_num']])
             else: st.caption(f"Only one point for {sel_kpi_gr}.")
    else: st.info("No KPI records found.")

    st.divider()
    # Event History & Management...
    st.subheader("Financial Events"); df_events_raw = get_events_by_company_name(company_data['portco_name'])
    if not df_events_raw.empty:
        df_evt_disp = df_events_raw.copy(); df_evt_disp['Date'] = df_evt_disp['event_date'].dt.strftime('%Y-%m-%d').fillna('N/A'); df_evt_disp['Cash Flow'] = df_evt_disp.apply(lambda r: format_currency(r['cash_flow_amount'], r.get('currency','$')), axis=1).fillna("-"); df_evt_disp['% Sold'] = df_evt_disp['percent_holding_sold'].apply(lambda x: f"{x:.1%}" if pd.notna(x) else "-"); df_evt_disp['Remaining Val'] = df_evt_disp.apply(lambda r: format_currency(r['fund_holding_valuation'], r.get('currency','$')), axis=1).fillna("-");
        evt_cols_cfg = {"id": st.column_config.NumberColumn("Event ID"),"Date": st.column_config.TextColumn("Date"),"event_type": st.column_config.TextColumn("Type"),"Cash Flow": st.column_config.TextColumn("Cash Flow (+/-)"),"% Sold": st.column_config.TextColumn("% Sold"),"Remaining Val": st.column_config.TextColumn("Post-Event Val"),"notes": st.column_config.TextColumn("Notes"),"event_date": None, "cash_flow_amount": None, "currency":None, "percent_holding_sold": None, "fund_holding_valuation": None}
        event_column_order = ["id", "Date", "event_type", "Cash Flow", "% Sold", "Remaining Val", "notes"]
        st.dataframe(df_evt_disp, column_config=evt_cols_cfg, hide_index=True, use_container_width=True, column_order=event_column_order)
        with st.expander("Manage Events"): # Event CRUD
             evt_opts = {f"ID {r['id']} ({pd.to_datetime(r['event_date']):%Y-%m-%d if pd.notna(r['event_date']) else 'N/A'}) {r['event_type']}": r['id'] for _, r in df_events_raw.iterrows()}; sel_evt_lbl = st.selectbox("Select Event:", [None]+list(evt_opts.keys()), format_func=lambda x: x or "Select...", key=f"mng_evt_{company_id}"); sel_evt_id = evt_opts.get(sel_evt_lbl)
             if sel_evt_id: # Update/Delete Forms
                  evt_data = get_event_by_id(sel_evt_id)
                  if evt_data:
                     col1, col2 = st.columns(2)
                     with col1: # Update Form (Conditional)
                          st.write(f"**Update Event ID: {sel_evt_id}**"); evt_type = evt_data.get('event_type')
                          if evt_type == 'Exit':
                              with st.form(f"evt_upd_ex_{sel_evt_id}", clear_on_submit=False): st.caption("Exit Event"); u_dt=st.date_input("Date*",evt_data.get('event_date')); u_cf=st.number_input("Proceeds*",value=evt_data.get('cash_flow_amount'),format="%.2f"); u_cr=st.selectbox("Curr",CURRENCY_OPTIONS,index=CURRENCY_OPTIONS.index(evt_data.get('currency','USD')) if evt_data.get('currency') in CURRENCY_OPTIONS else 0); u_phs=st.number_input("% Sold*",0.0,1.0,value=evt_data.get('percent_holding_sold'),format="%.4f"); u_fhv=st.number_input("Remain Val*",value=evt_data.get('fund_holding_valuation'),format="%.2f"); u_nt=st.text_area("Notes",evt_data.get('notes',''));
                              if st.form_submit_button("Save"): update_event(sel_evt_id, 'Exit', u_dt, u_cf, u_cr, u_phs, u_fhv, u_nt); st.rerun()
                          elif evt_type == 'Dividend':
                               with st.form(f"evt_upd_dv_{sel_evt_id}", clear_on_submit=False): st.caption("Dividend"); u_dt=st.date_input("Date*",evt_data.get('event_date')); u_cf=st.number_input("Amount*",value=evt_data.get('cash_flow_amount'),format="%.2f"); u_cr=st.selectbox("Curr",CURRENCY_OPTIONS,index=CURRENCY_OPTIONS.index(evt_data.get('currency','USD')) if evt_data.get('currency') in CURRENCY_OPTIONS else 0); u_nt=st.text_area("Notes",evt_data.get('notes','')); u_fhv=st.number_input("Post-Div Val (Opt)",value=evt_data.get('fund_holding_valuation'),format="%.2f");
                               if st.form_submit_button("Save"): update_event(sel_evt_id, 'Dividend', u_dt, u_cf, u_cr, None, u_fhv, u_nt); st.rerun()
                          elif evt_type == 'Valuation Update':
                               with st.form(f"evt_upd_vl_{sel_evt_id}", clear_on_submit=False): st.caption("Valuation Update"); u_dt=st.date_input("Date*",evt_data.get('event_date')); u_fhv=st.number_input("New Holding Val*",value=evt_data.get('fund_holding_valuation'),format="%.2f"); u_cr=st.selectbox("Curr",CURRENCY_OPTIONS,index=CURRENCY_OPTIONS.index(evt_data.get('currency','USD')) if evt_data.get('currency') in CURRENCY_OPTIONS else 0); u_nt=st.text_area("Notes",evt_data.get('notes',''));
                               if st.form_submit_button("Save"): update_event(sel_evt_id, 'Valuation Update', u_dt, None, u_cr, None, u_fhv, u_nt); st.rerun()
                          else: st.warning("Unknown event type")
                     with col2: # Delete Form
                          st.write(f"**Delete Event ID: {sel_evt_id}**"); cfm_del = st.checkbox("Confirm", key=f"d_evt_{sel_evt_id}");
                          if st.button("Delete", type="primary", disabled=not cfm_del, key=f"d_evt_b_{sel_evt_id}"): delete_event(sel_evt_id); st.rerun()
    else: st.info("No financial events recorded.")

    st.divider()
    # Delete Company Expander...
    with st.expander("🗑️ Delete Company", expanded=False):
        st.subheader("Danger Zone"); st.warning(f"⚠️ Deleting **{company_data['portco_name']}** (ID: {company_id}) and **ALL** its associated records cannot be undone.")
        confirm_delete = st.checkbox("I understand and wish to proceed.", key=f"delete_check_{company_id}")
        if st.button("Confirm Company Deletion", key=f"delete_confirm_{company_id}", type="primary", disabled=not confirm_delete):
            if delete_portco(company_id): st.success(f"'{company_data['portco_name']}' deleted."); time.sleep(2); st.query_params.clear(); st.rerun()


# --- Homepage Dashboard Rendering ---
def render_homepage_dashboard():
    """Displays the main dashboard with summary stats and recent activity."""
    st.title("📈 Yang Portfolio Dashboard")
    st.markdown("High-level overview of your portfolio.")

    # --- Summary Stats ---
    stats = get_portfolio_summary_stats()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Companies", f"{stats['total_companies']}")
    col2.metric("Active Companies", f"{stats['active_companies']}")
    col3.metric("Total Capital Invested", format_currency(stats['total_invested'], '$') or "$0.00")
    col4.metric("Estimated Current Value", format_currency(stats['current_value'], '$') or "$0.00")

    st.divider()

    # --- Recent Activity ---
    st.subheader("Recent Activity")
    recent_col1, recent_col2 = st.columns(2)

    with recent_col1:
        st.markdown("**Latest Investments**")
        df_recent_inv = get_recent_investments()
        if df_recent_inv.empty:
            st.caption("No investments found.")
        else:
            for _, row in df_recent_inv.iterrows():
                link = f"/?page=company&id={row['portco_id']}" if row['portco_id'] != -1 else "#"
                amount = format_currency(row['size_of_investment']) or "N/A"
                st.markdown(f"*   `{row['date_of_investment']:%Y-%m-%d}`: [{row['portco_name']}]({link}) - {amount}")

    with recent_col2:
        st.markdown("**Latest Financial Events**")
        df_recent_evt = get_recent_events()
        if df_recent_evt.empty:
            st.caption("No events found.")
        else:
            for _, row in df_recent_evt.iterrows():
                link = f"/?page=company&id={row['portco_id']}" if row['portco_id'] != -1 else "#"
                st.markdown(f"*   `{row['event_date']:%Y-%m-%d}`: [{row['portco_name']}]({link}) - **{row['event_type']}** {(' - '+row['notes']) if row['notes'] else ''}")

    st.divider()
    st.info("Use the sidebar to navigate to specific sections for detailed views and data entry.")


# --- Main Application Logic ---

# Initialize Database Tables Safely
create_portco_table()
create_investment_table()
create_kpi_table()
create_event_table()

# --- Routing based on Query Params ---
query_params = st.query_params.to_dict()
param_page = query_params.get("page")
param_id = query_params.get("id")

if param_page == "company" and param_id:
    try:
        company_id_int = int(param_id)
        render_company_page(company_id_int)
    except (ValueError, TypeError):
        st.title("Yang Portfolio Management")
        st.error("Invalid Company ID provided in the URL.")
        render_homepage_dashboard()
    except Exception as e:
        st.title("Yang Portfolio Management")
        st.error(f"An error occurred displaying the company page: {e}")
        render_homepage_dashboard()
else:
    # --- Render the Homepage Dashboard by default ---
    render_homepage_dashboard()