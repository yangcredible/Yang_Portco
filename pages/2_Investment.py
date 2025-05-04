import streamlit as st
import sqlite3
import pandas as pd
from datetime import date, datetime
# from urllib.parse import quote # Not needed if using ID

# -- Configuration --
DB_NAME = 'Yang.db'
INVESTMENT_TABLE_NAME = 'investments'
PORTCO_TABLE_NAME = 'list_of_portco' # Needed for join and getting portco ID/names

LIST_OF_FUNDS = sorted([f'Yang Fund {i+1}' for i in range(3)])
LIST_OF_TYPES_OF_INVESTMENTS = sorted(["Debt", "Equity", "SAFE Note", "Convertible Note", "Warrants"])
ROUND_STAGES = ['Pre-Seed', 'Seed', 'Series A', 'Series B', 'Series C', 'Series D+', 'Growth Equity', 'Mezzanine', 'Other']

#--------------------Database Functions--------------------

def db_connect():
    """Establishes database connection."""
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA foreign_keys = ON") # Ensure FK constraints are enforced
    return conn

def get_portco_names_from_db():
    """Retrieves a sorted list of unique *active* portfolio company names from the portco table."""
    conn = db_connect()
    try:
        # Ensure the portco table exists
        c = conn.cursor()
        c.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{PORTCO_TABLE_NAME}'")
        if not c.fetchone():
            st.error(f"Error: Portfolio company table '{PORTCO_TABLE_NAME}' not found. Please run the Portfolio Company page first.")
            return ["Error: Company table missing"]

        df = pd.read_sql_query(f"SELECT DISTINCT portco_name FROM {PORTCO_TABLE_NAME} WHERE status = 'Active' ORDER BY portco_name COLLATE NOCASE", conn)
        return df['portco_name'].tolist()
    except Exception as e:
        st.error(f"Error reading portfolio company names: {e}")
        return ["Error: Could not load companies"]
    finally:
        if conn: conn.close()

# Ensure tables exist (call necessary creation functions)
def ensure_tables_exist():
    """Checks and creates necessary tables if they don't exist."""
    # This function might run concurrently with the main script's checks.
    # It's generally safe due to `IF NOT EXISTS`, but be mindful of potential
    # minor race conditions during initial setup (unlikely to cause major issues).
    conn = db_connect()
    c = conn.cursor()
    try:
        # Create Portco Table (Idempotent)
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {PORTCO_TABLE_NAME}(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portco_name TEXT NOT NULL UNIQUE COLLATE NOCASE,
                year_founded INT, industry_classification TEXT, establishment_country TEXT,
                status TEXT DEFAULT 'Active' CHECK(status IN ('Active', 'Inactive')) ) ''')
        conn.commit()
        # Create Investment Table (Idempotent)
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {INVESTMENT_TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT, fund_name TEXT NOT NULL, portco_name TEXT NOT NULL,
                type_of_investment TEXT, investment_round_number INTEGER NOT NULL,
                round_stage TEXT, date_of_investment TEXT NOT NULL, size_of_investment REAL NOT NULL,
                total_round_size REAL, post_money_valuation REAL,
                FOREIGN KEY (portco_name) REFERENCES {PORTCO_TABLE_NAME}(portco_name) ON DELETE CASCADE ON UPDATE CASCADE ) ''')
        conn.commit()
         # Check and add round_stage column if missing
        c.execute(f"PRAGMA table_info({INVESTMENT_TABLE_NAME})")
        columns = {col[1] for col in c.fetchall()}
        if 'round_stage' not in columns:
            try:
                c.execute(f"ALTER TABLE {INVESTMENT_TABLE_NAME} ADD COLUMN round_stage TEXT")
                conn.commit()
            except sqlite3.Error as e_alter:
                 if "duplicate column name" not in str(e_alter):
                     print(f"Warning: Could not add 'round_stage' to {INVESTMENT_TABLE_NAME} (in Pages/2...). {e_alter}")
    except sqlite3.Error as e:
         print(f"Warning during table check/creation (in Pages/2...): {e}")
    finally:
        if conn: conn.close()


def add_investment(fund_name, portco_name, type_of_investment_str,
                   investment_round_number, round_stage, date_of_investment_obj, size_of_investment,
                   total_round_size, post_money_valuation):
    """Adds a new investment record to the database."""
    conn = db_connect()
    c = conn.cursor()
    date_str = date_of_investment_obj.strftime('%Y-%m-%d') if date_of_investment_obj else None
    total_round_size_db = total_round_size if total_round_size is not None else None
    post_money_valuation_db = post_money_valuation if post_money_valuation is not None else None
    try:
        c.execute(f'''
            INSERT INTO {INVESTMENT_TABLE_NAME} ( fund_name, portco_name, type_of_investment, investment_round_number,
                round_stage, date_of_investment, size_of_investment, total_round_size, post_money_valuation )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ''',
            ( fund_name, portco_name, type_of_investment_str, investment_round_number,
              round_stage, date_str, size_of_investment, total_round_size_db, post_money_valuation_db ) )
        conn.commit()
        st.toast(f"✅ Added investment in '{portco_name}' by '{fund_name}'!", icon="✅")
        return True
    except sqlite3.IntegrityError as e:
        if "FOREIGN KEY constraint failed" in str(e):
             st.error(f"Error: Could not add investment. Portfolio company '{portco_name}' may not exist or is not active in the list.")
        else:
             st.error(f"Database integrity error adding investment: {e}")
        return False
    except sqlite3.Error as e:
        st.error(f"Database error adding investment: {e}")
        return False
    finally:
        if conn: conn.close()


def get_all_investments_with_portco_id():
    """Retrieves all investments JOINED with portco table to get company ID."""
    conn = db_connect()
    try:
        query = f"""
            SELECT
                i.id as investment_id, i.fund_name, i.portco_name, i.type_of_investment,
                i.investment_round_number, i.round_stage, i.date_of_investment,
                i.size_of_investment, i.total_round_size, i.post_money_valuation,
                p.id as portco_id
            FROM {INVESTMENT_TABLE_NAME} i
            LEFT JOIN {PORTCO_TABLE_NAME} p ON i.portco_name = p.portco_name COLLATE NOCASE
            ORDER BY i.date_of_investment DESC, i.portco_name COLLATE NOCASE
            """
        df = pd.read_sql_query(query, conn)

        # Convert types
        df['date_of_investment'] = pd.to_datetime(df['date_of_investment'], errors='coerce')
        df['investment_round_number'] = pd.to_numeric(df['investment_round_number'], errors='coerce').astype(pd.Int64Dtype())
        df['size_of_investment'] = pd.to_numeric(df['size_of_investment'], errors='coerce')
        df['total_round_size'] = pd.to_numeric(df['total_round_size'], errors='coerce')
        df['post_money_valuation'] = pd.to_numeric(df['post_money_valuation'], errors='coerce')
        df['portco_id'] = df['portco_id'].fillna(-1).astype(int) # Handle missing join

        return df
    except Exception as e:
        st.error(f"Error reading investments with company ID: {e}")
        return pd.DataFrame(columns=['investment_id', 'fund_name', 'portco_name', 'type_of_investment',
                                     'investment_round_number', 'round_stage', 'date_of_investment',
                                     'size_of_investment', 'total_round_size', 'post_money_valuation', 'portco_id'])
    finally:
        if conn: conn.close()

# Function to format currency
def format_currency(value):
    """Formats a number as USD currency string with commas."""
    if pd.isna(value) or value is None: return None
    try: return "${:,.2f}".format(float(value))
    except (ValueError, TypeError): return "Invalid Format"

#--------------------Streamlit App--------------------

st.set_page_config(layout="wide", page_title="Investments") # Specific title

# --- Initialize tables ---
ensure_tables_exist()

def render_main_investment_dashboard():
    """Displays the main dashboard: Investment List (Markdown Table), Add Form."""
    st.title("Investment Management")

    df_investments = get_all_investments_with_portco_id()

    st.markdown(f"**{len(df_investments)}** investments recorded.")
    st.caption("Click on a company name link to view its details and full investment history.")

    if df_investments.empty:
         st.info("ℹ️ No investments found. Add one using the form below.")
    else:
        df_display = df_investments.copy()

        # Link points to the main app script with query params
        df_display['Company Link'] = df_display.apply(
            lambda row: f"[{row['portco_name']}](/?page=company&id={row['portco_id']})" if row['portco_id'] != -1 else row['portco_name'],
            axis=1
        )

        # Pre-format currency and date for Markdown
        money_cols = ['size_of_investment', 'total_round_size', 'post_money_valuation']
        for col in money_cols:
            df_display[col] = df_display[col].apply(format_currency)
        df_display['date_of_investment'] = df_display['date_of_investment'].dt.strftime('%Y-%m-%d').fillna('N/A')

        display_columns_map = {
            'Company Link': 'Company',
            'fund_name': 'Fund',
            'date_of_investment': 'Date',
            'investment_round_number': 'Round #',
            'round_stage': 'Round Stage',
            'size_of_investment': 'Investment (USD)',
            'type_of_investment': 'Type(s)',
            'total_round_size': 'Round Size (USD)',
            'post_money_valuation': 'Post-Money Val (USD)',
        }
        df_to_show = df_display[list(display_columns_map.keys())].rename(columns=display_columns_map)

        st.markdown(
            df_to_show.to_markdown(index=False),
            unsafe_allow_html=True
        )

    # --- Add Investment Form ---
    st.divider()
    with st.expander("➕ Add New Investment Record", expanded=False):
        available_portcos = get_portco_names_from_db() # Gets only active ones
        if not available_portcos or available_portcos[0].startswith("Error:"):
            st.warning("Cannot add investments: Ensure the Portfolio Company table exists and contains active companies.")
        else:
            available_funds = LIST_OF_FUNDS
            with st.form("investment_form_add", clear_on_submit=True):
                st.subheader("Enter Investment Details")
                fund_name = st.selectbox("Fund*", available_funds, index=None, placeholder="Select Fund...")
                portco_name = st.selectbox("Company*", available_portcos, index=None, placeholder="Select Active Company...")
                type_of_investment_list = st.pills( "Type(s)", LIST_OF_TYPES_OF_INVESTMENTS, selection_mode="multi") # Pills return tuple
                round_stage = st.selectbox("Stage*", ROUND_STAGES, index=None, placeholder="Select Stage...")
                investment_round = st.number_input( "Round #*", min_value=1, value=1, step=1)
                date_of_investment = st.date_input("Date*", value=None, max_value=date.today())
                size_of_investment = st.number_input("Investment (USD)*", min_value=0.01, value=None, format="%.2f", step=1000.00, placeholder="0.00")
                total_round_size = st.number_input("Round Size (USD)", min_value=0.00, value=None, format="%.2f", step=1000.00, placeholder="Optional: 0.00")
                post_money_valuation = st.number_input("Post-Money Val (USD)", min_value=0.00, value=None, format="%.2f", step=10000.00, placeholder="Optional: 0.00")

                submitted_add = st.form_submit_button("Add Investment Record", type="primary")

                if submitted_add:
                    is_valid = True
                    if not fund_name: st.error("Fund Name required."); is_valid = False
                    if not portco_name: st.error("Company Name required."); is_valid = False
                    if not round_stage: st.error("Stage required."); is_valid = False
                    if date_of_investment is None: st.error("Date required."); is_valid = False
                    if size_of_investment is None or size_of_investment <= 0: st.error("Investment Size required."); is_valid = False

                    if is_valid:
                        # Convert tuple from pills to list if needed, then join
                        investment_types_str = ", ".join(sorted(list(type_of_investment_list))) if type_of_investment_list else None

                        if add_investment(fund_name, portco_name, investment_types_str, investment_round,
                                          round_stage, date_of_investment, size_of_investment, total_round_size, post_money_valuation):
                            st.rerun()

# --- Main Execution Logic ---
render_main_investment_dashboard() # Always render the main dashboard for this page