import streamlit as st
import sqlite3
import pandas as pd
import numpy_financial as npf # For IRR calculation
from datetime import datetime, date

# --- Configuration ---
DB_NAME = 'Yang.db'
INVESTMENT_TABLE_NAME = 'investments'
EVENT_TABLE_NAME = 'events'
PORTCO_TABLE_NAME = 'list_of_portco'

# Get Fund List (Assuming it's defined consistently, maybe centralize later)
# If LIST_OF_FUNDS is defined in Yang_Portfolio.py, we might need to import it
# or redefine it here. For simplicity, let's redefine it.
LIST_OF_FUNDS = sorted([f'Yang Fund {i+1}' for i in range(3)])

# --- Database Functions ---

def db_connect():
    """Establishes database connection and enables Foreign Keys."""
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def get_all_investments_for_returns():
    """Retrieves all investments with necessary columns for return calculations."""
    conn = db_connect()
    try:
        # Select necessary columns: fund name, company name, date, amount
        query = f"""
            SELECT fund_name, portco_name, date_of_investment, size_of_investment
            FROM {INVESTMENT_TABLE_NAME}
            ORDER BY date_of_investment
            """
        df = pd.read_sql_query(query, conn)
        # Convert types
        df['date_of_investment'] = pd.to_datetime(df['date_of_investment'], errors='coerce')
        df['size_of_investment'] = pd.to_numeric(df['size_of_investment'], errors='coerce')
        # Drop rows with invalid data essential for calculation
        df.dropna(subset=['date_of_investment', 'size_of_investment', 'fund_name', 'portco_name'], inplace=True)
        return df
    except Exception as e:
        st.error(f"Error reading investments for returns: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()

def get_all_events_for_returns():
    """Retrieves all events with necessary columns for return calculations."""
    conn = db_connect()
    try:
        # Select necessary columns: company name, date, type, cash flow, valuation
        query = f"""
            SELECT portco_name, event_date, event_type, cash_flow_amount, fund_holding_valuation
            FROM {EVENT_TABLE_NAME}
            ORDER BY event_date
            """
        df = pd.read_sql_query(query, conn)
        # Convert types
        df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
        df['cash_flow_amount'] = pd.to_numeric(df['cash_flow_amount'], errors='coerce')
        df['fund_holding_valuation'] = pd.to_numeric(df['fund_holding_valuation'], errors='coerce')
        # Drop rows with invalid data essential for calculation
        df.dropna(subset=['event_date', 'portco_name', 'event_type'], inplace=True)
        return df
    except Exception as e:
        st.error(f"Error reading events for returns: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()

# --- Calculation Function ---

def calculate_fund_returns(fund_name, all_investments_df, all_events_df):
    """Calculates MOIC and IRR for a specific fund."""
    results = {
        'Fund': fund_name,
        'Total Invested': 0.0,
        'Total Realized': 0.0,
        'Total Unrealized': 0.0,
        'Total Value': 0.0,
        'MOIC': None,
        'IRR': None
    }
    cash_flows_irr = [] # List to store (date, amount) tuples for IRR

    # 1. Filter Investments for this fund
    fund_investments = all_investments_df[all_investments_df['fund_name'] == fund_name].copy()

    if fund_investments.empty:
        return results # No investments, so no returns

    # 2. Calculate Total Invested and prepare initial cash flows for IRR
    results['Total Invested'] = fund_investments['size_of_investment'].sum()
    for _, row in fund_investments.iterrows():
        cash_flows_irr.append((row['date_of_investment'], -row['size_of_investment'])) # Negative flow

    # 3. Identify unique companies invested in by this fund
    companies_in_fund = fund_investments['portco_name'].unique()

    # 4. Filter Events for these companies
    fund_events = all_events_df[all_events_df['portco_name'].isin(companies_in_fund)].copy()

    # 5. Calculate Total Realized and add positive cash flows for IRR
    realized_events = fund_events[fund_events['event_type'].isin(['Exit', 'Dividend'])]
    results['Total Realized'] = realized_events['cash_flow_amount'].sum()
    for _, row in realized_events.iterrows():
        if pd.notna(row['cash_flow_amount']):
             cash_flows_irr.append((row['event_date'], row['cash_flow_amount'])) # Positive flow

    # 6. Calculate Total Unrealized (Sum of latest valuations)
    latest_valuations = {}
    if not fund_events.empty:
        # Find the latest event date for each company within the filtered events
        latest_event_indices = fund_events.loc[fund_events.groupby('portco_name')['event_date'].idxmax()]
        latest_valuations = latest_event_indices.set_index('portco_name')['fund_holding_valuation'].to_dict()

    total_unrealized = 0.0
    for company in companies_in_fund:
        # Use the latest valuation if available, otherwise assume 0 for simplicity
        company_unrealized = latest_valuations.get(company, 0.0)
        # Ensure it's a valid number, default to 0 if not
        total_unrealized += company_unrealized if pd.notna(company_unrealized) else 0.0

    results['Total Unrealized'] = total_unrealized

    # 7. Calculate Total Value & MOIC
    results['Total Value'] = results['Total Realized'] + results['Total Unrealized']
    if results['Total Invested'] > 0:
        results['MOIC'] = results['Total Value'] / results['Total Invested']
    else:
        results['MOIC'] = 0.0 # Or None, depending on preference

    # 8. Finalize IRR Cash Flows & Calculate IRR
    # Add the total unrealized value as the final cash flow today
    if results['Total Unrealized'] > 0:
         today = pd.to_datetime(date.today())
         # Ensure today's date is after the last cash flow date
         last_flow_date = max([cf[0] for cf in cash_flows_irr]) if cash_flows_irr else today - pd.Timedelta(days=1)
         if today > last_flow_date:
            cash_flows_irr.append((today, results['Total Unrealized']))
         else:
            # If today is somehow not after the last flow, append with a slightly later date
            cash_flows_irr.append((last_flow_date + pd.Timedelta(days=1), results['Total Unrealized']))


    # Sort cash flows by date
    cash_flows_irr.sort(key=lambda x: x[0])

    # Prepare amounts for npf.irr function
    amounts = [cf[1] for cf in cash_flows_irr]

    # Calculate IRR (requires at least one positive and one negative flow)
    if len(amounts) > 1 and any(a > 0 for a in amounts) and any(a < 0 for a in amounts):
        try:
            # npf.irr requires just the amounts
            irr_value = npf.irr(amounts)
            # Check for NaN or Inf which can occur
            if pd.notna(irr_value) and abs(irr_value) != float('inf'):
                 results['IRR'] = irr_value
            else:
                 results['IRR'] = None # Indicate calculation didn't yield a sensible result
        except ValueError:
            results['IRR'] = None # Handle potential errors during IRR calculation
    else:
        results['IRR'] = None # Not enough data or no change in sign

    return results

# --- Helper Function ---
def format_currency(value, currency_symbol='$'):
    if pd.isna(value) or value is None: return "-" # Use dash for None in table
    try: return f"{currency_symbol}{float(value):,.2f}"
    except (ValueError, TypeError): return "Invalid Format"

def format_multiple(value):
    if pd.isna(value) or value is None: return "-"
    try: return f"{float(value):.2f}x"
    except (ValueError, TypeError): return "Invalid Format"

def format_percentage(value):
    if pd.isna(value) or value is None: return "-"
    try: return f"{float(value):.1%}"
    except (ValueError, TypeError): return "Invalid Format"

# --- Streamlit Page ---
st.set_page_config(layout="wide", page_title="Fund Returns")

def render_returns_dashboard():
    """Displays the Fund Returns dashboard."""
    st.title("Fund Investment Returns Overview")
    st.caption(f"Calculations as of: {date.today().strftime('%Y-%m-%d')}. IRR assumes unrealized value is realized today.")

    # Fetch all data needed
    all_investments = get_all_investments_for_returns()
    all_events = get_all_events_for_returns()

    if all_investments.empty:
        st.warning("No investment data found. Cannot calculate returns.")
        return

    # Calculate returns for each fund
    fund_return_data = []
    for fund in LIST_OF_FUNDS:
        returns = calculate_fund_returns(fund, all_investments, all_events)
        fund_return_data.append(returns)

    # Create DataFrame for display
    df_returns = pd.DataFrame(fund_return_data)

    # Format columns for display
    df_display = df_returns.copy()
    df_display['Total Invested'] = df_display['Total Invested'].apply(format_currency)
    df_display['Total Realized'] = df_display['Total Realized'].apply(format_currency)
    df_display['Total Unrealized'] = df_display['Total Unrealized'].apply(format_currency)
    df_display['Total Value'] = df_display['Total Value'].apply(format_currency)
    df_display['MOIC'] = df_display['MOIC'].apply(format_multiple)
    df_display['IRR'] = df_display['IRR'].apply(format_percentage)

    # Select and order columns for the final table
    display_columns = [
        'Fund', 'Total Invested', 'Total Realized', 'Total Unrealized',
        'Total Value', 'MOIC', 'IRR'
    ]
    df_display = df_display[display_columns]

    # Display the results table
    st.dataframe(
        df_display,
        hide_index=True,
        use_container_width=True
    )

    st.markdown("---")
    st.caption("""
    **Notes:**
    *   **Total Invested:** Sum of all investment sizes made by the fund.
    *   **Total Realized:** Sum of cash received by the fund from Exits and Dividends for companies the fund invested in.
    *   **Total Unrealized:** Sum of the latest 'Fund Holding Valuation' recorded in Events for each company the fund invested in. If no events, value is 0.
    *   **Total Value:** Total Realized + Total Unrealized.
    *   **MOIC (Multiple on Invested Capital):** Total Value / Total Invested.
    *   **IRR (Internal Rate of Return):** Calculated based on investment dates/amounts, realized cash flow dates/amounts, and the Total Unrealized value treated as a final cash inflow today. Calculation requires both positive and negative cash flows. 'N/A' indicates insufficient data or calculation error.
    """)


# --- Main Execution ---
# Ensure tables exist (optional, main script usually does this)
# create_investment_table()
# create_event_table()

render_returns_dashboard()