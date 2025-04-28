import sqlite3
import random
import os
import pandas as pd
from datetime import datetime, timedelta, date
from faker import Faker # Make sure to pip install Faker

# --- Configuration ---
DB_NAME = 'Yang.db'
PORTCO_TABLE_NAME = 'list_of_portco'
INVESTMENT_TABLE_NAME = 'investments'
KPI_TABLE_NAME = 'kpis'
EVENT_TABLE_NAME = 'events'

# --- Constants for Data Generation ---
NUM_FUNDS = 3
COMPANIES_PER_FUND = 10
TOTAL_COMPANIES = NUM_FUNDS * COMPANIES_PER_FUND

LIST_OF_FUNDS = sorted([f'Yang Fund {i+1}' for i in range(NUM_FUNDS)])
INDUSTRY_LIST = ['Information Technology', 'Health Care', 'Financials', 'Communication Services', 'Consumer Discretionary', 'Industrials', 'Real Estate', 'Energy', 'Materials', 'Consumer Staples', 'Utilities']
COUNTRY_LIST = ["Singapore", "United States", "China", "Germany", "United Kingdom", "India"]
STATUS_OPTIONS = ['Active', 'Active', 'Active', 'Inactive'] # Skew towards Active
KPI_NAMES = ['Monthly Recurring Revenue (MRR)', 'Annual Recurring Revenue (ARR)', 'Customer Acquisition Cost (CAC)', 'Customer Lifetime Value (LTV)', 'Active Users', 'Churn Rate']
KPI_UNITS = {'MRR': 'USD', 'ARR': 'USD', 'CAC': 'USD', 'LTV': 'USD', 'Active Users': 'Users', 'Churn Rate': '%'}
EVENT_TYPES = ['Exit', 'Dividend', 'Valuation Update']
CURRENCY_OPTIONS = ['USD', 'SGD', 'EUR']
ROUND_STAGES = ['Pre-Seed', 'Seed', 'Series A', 'Series B', 'Series C', 'Series D+', 'Growth Equity']
INVESTMENT_TYPES = ['Equity', 'SAFE Note', 'Convertible Note']

fake = Faker() # Initialize Faker

# --- Database Functions ---

def db_connect():
    """Establishes database connection and enables Foreign Keys."""
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def create_tables(conn):
    """Creates all necessary tables."""
    c = conn.cursor()
    print("Creating tables...")

    # Portco Table
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

    # Investment Table
    c.execute(f'''
        CREATE TABLE IF NOT EXISTS {INVESTMENT_TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_name TEXT NOT NULL,
            portco_name TEXT NOT NULL COLLATE NOCASE,
            type_of_investment TEXT,
            investment_round_number INTEGER NOT NULL,
            round_stage TEXT,
            date_of_investment TEXT NOT NULL, -- YYYY-MM-DD
            size_of_investment REAL NOT NULL,
            total_round_size REAL,
            post_money_valuation REAL,
            FOREIGN KEY (portco_name) REFERENCES {PORTCO_TABLE_NAME}(portco_name) ON DELETE CASCADE ON UPDATE CASCADE
        ) ''')

    # KPI Table
    c.execute(f'''
        CREATE TABLE IF NOT EXISTS {KPI_TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portco_name TEXT NOT NULL COLLATE NOCASE,
            kpi_name TEXT NOT NULL COLLATE NOCASE,
            kpi_value REAL,
            kpi_date TEXT NOT NULL, -- YYYY-MM-DD
            units TEXT,
            notes TEXT,
            FOREIGN KEY (portco_name) REFERENCES {PORTCO_TABLE_NAME}(portco_name) ON DELETE CASCADE ON UPDATE CASCADE
        ) ''')
    c.execute(f"CREATE INDEX IF NOT EXISTS idx_kpi_portco_date ON {KPI_TABLE_NAME} (portco_name, kpi_date);")
    c.execute(f"CREATE INDEX IF NOT EXISTS idx_kpi_name ON {KPI_TABLE_NAME} (kpi_name);")

    # Event Table
    c.execute(f'''
        CREATE TABLE IF NOT EXISTS {EVENT_TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portco_name TEXT NOT NULL COLLATE NOCASE,
            event_date TEXT NOT NULL, -- YYYY-MM-DD
            event_type TEXT NOT NULL CHECK(event_type IN {tuple(EVENT_TYPES)}),
            cash_flow_amount REAL, -- Positive for inflow to fund
            currency TEXT DEFAULT 'USD',
            percent_holding_sold REAL, -- 0.0 to 1.0
            fund_holding_valuation REAL, -- Value of REMAINING holding
            notes TEXT,
            FOREIGN KEY (portco_name) REFERENCES {PORTCO_TABLE_NAME}(portco_name) ON DELETE CASCADE ON UPDATE CASCADE
        )
    ''')
    c.execute(f"CREATE INDEX IF NOT EXISTS idx_event_portco_date ON {EVENT_TABLE_NAME} (portco_name, event_date);")

    conn.commit()
    print("Tables created successfully.")

# --- Data Generation Functions ---

def generate_companies():
    """Generates a list of unique company data."""
    companies = []
    generated_names = set()
    while len(companies) < TOTAL_COMPANIES:
        name = fake.company()
        # Ensure unique names (simple check, might need refinement for scale)
        if name in generated_names or len(name) > 50: # Basic check
             # Add suffix if simple name repeats often with Faker
            name = f"{name} {fake.word().capitalize()}"
            if name in generated_names:
                 name = f"{name} {random.randint(1,99)}" # Last resort
                 if name in generated_names:
                     continue # Skip if still colliding

        generated_names.add(name)
        year = random.randint(2010, 2021)
        # Ensure multiple industries
        num_industries = random.randint(1, 3)
        industries = random.sample(INDUSTRY_LIST, num_industries)
        industry_str = ", ".join(sorted(industries))
        country = random.choice(COUNTRY_LIST)
        status = random.choice(STATUS_OPTIONS)
        companies.append({
            "portco_name": name,
            "year_founded": year,
            "industry_classification": industry_str,
            "establishment_country": country,
            "status": status,
            "first_investment_date": None, # Placeholder
            "total_invested": 0.0 # Placeholder
        })
    print(f"Generated {len(companies)} companies.")
    return companies

def generate_investments(companies):
    """Generates investment data for companies."""
    investments = []
    current_date = datetime.now().date()

    for i, company in enumerate(companies):
        # Assign a primary fund, though can allow others later
        primary_fund = LIST_OF_FUNDS[i // COMPANIES_PER_FUND]
        num_rounds = random.randint(1, 5)
        last_date = date(company["year_founded"], random.randint(6,12) , random.randint(1,28)) # First inv after founding
        last_valuation = random.uniform(500_000, 3_000_000) # Starting valuation range
        total_invested_company = 0.0

        for round_num in range(1, num_rounds + 1):
            # Advance date
            days_to_next = random.randint(180, 730) # 6 months to 2 years
            investment_date = last_date + timedelta(days=days_to_next)
            # Stop if investment date goes beyond today
            if investment_date >= current_date:
                break

            # Determine round stage (simplistic)
            stage = ROUND_STAGES[min(round_num, len(ROUND_STAGES) - 1)] # Use index safely

            # Investment size increases
            size = random.uniform(0.5, 5.0) * last_valuation * 0.1 * (1 + round_num * 0.5) # Scale size
            size = round(size / 10000) * 10000 # Round to nearest 10k

            # Total round slightly larger
            total_round = size * random.uniform(1.5, 4.0)

            # Post-money valuation increases
            post_money = last_valuation * random.uniform(1.5, 3.5) # Increase valuation

            # Investment type (more equity in later rounds)
            inv_type = random.choice(INVESTMENT_TYPES) if round_num <= 2 else 'Equity'

            investments.append((
                primary_fund, # For simplicity, use primary fund
                company["portco_name"],
                inv_type,
                round_num,
                stage,
                investment_date.strftime('%Y-%m-%d'),
                size,
                round(total_round),
                round(post_money)
            ))
            last_date = investment_date
            last_valuation = post_money
            total_invested_company += size

            # Store first investment date for KPI/Event generation start
            if round_num == 1:
                company["first_investment_date"] = investment_date

        company["total_invested"] = total_invested_company # Store total invested

    print(f"Generated {len(investments)} investments.")
    return investments


def generate_kpis(companies):
    """Generates KPI data points."""
    kpis = []
    current_date = datetime.now().date()

    for company in companies:
        if company["status"] == 'Inactive' or not company["first_investment_date"]:
            continue # Skip KPIs for inactive or non-invested companies

        # Select 2-4 KPIs for this company
        num_kpis = random.randint(2, 4)
        selected_kpi_names = random.sample(KPI_NAMES, num_kpis)

        # Generate quarterly data points
        start_date = company["first_investment_date"] + timedelta(days=random.randint(60, 120)) # Start KPI after first inv
        current_kpi_date = start_date

        # Keep track of last value per KPI for trending
        last_values = {}

        while current_kpi_date < current_date:
            for kpi_name in selected_kpi_names:
                units = KPI_UNITS.get(kpi_name, None)
                last_val = last_values.get(kpi_name, random.uniform(1000, 100000) if units == 'USD' else random.uniform(50, 5000) if units == 'Users' else random.uniform(0.5, 5.0))

                # Simple trend simulation
                change_factor = random.uniform(0.85, 1.25) # Allow decrease/increase
                current_val = last_val * change_factor
                if units == '%': current_val = max(0.1, min(current_val, 15.0)) # Keep churn rate reasonable
                elif units == 'USD': current_val = max(100, current_val) # Min revenue/cost
                else: current_val = max(10, current_val) # Min users/LTV

                kpis.append((
                    company["portco_name"],
                    kpi_name,
                    round(current_val, 2),
                    current_kpi_date.strftime('%Y-%m-%d'),
                    units,
                    None # No notes for synthetic data
                ))
                last_values[kpi_name] = current_val

            # Move to next quarter (approx)
            current_kpi_date += timedelta(days=random.randint(85, 95))

    print(f"Generated {len(kpis)} KPI records.")
    return kpis


def generate_events(companies, investments_df):
    """Generates financial Event data points."""
    events = []
    current_date = datetime.now().date()

    # Ensure investments_df date is datetime
    investments_df['date_of_investment'] = pd.to_datetime(investments_df['date_of_investment'])

    for company in companies:
        if company["status"] == 'Inactive' or not company["first_investment_date"]:
            continue

        company_investments = investments_df[investments_df['portco_name'] == company['portco_name']]
        if company_investments.empty:
            continue

        last_valuation_from_inv = company_investments['post_money_valuation'].iloc[-1] # Last known PMV
        current_holding_valuation = last_valuation_from_inv if pd.notna(last_valuation_from_inv) else company['total_invested'] * 1.5 # Fallback initial valuation

        # Start event timeline after last investment
        last_inv_date = company_investments['date_of_investment'].max().date()
        current_event_date = last_inv_date + timedelta(days=random.randint(180, 365)) # Start events 6-12mo after last inv

        has_exited = False
        percent_holding = 1.0 # Assume fund starts with 100% of its relative stake

        while current_event_date < current_date and not has_exited:
            event_type = random.choice(['Valuation Update', 'Valuation Update', 'Valuation Update', 'Dividend', 'Exit']) # Skew towards valuations

            # --- Default values ---
            cash_flow = None
            percent_sold = None
            notes = f"Generated {event_type}"
            currency = random.choice(CURRENCY_OPTIONS)

            # Simulate valuation change between events
            current_holding_valuation *= random.uniform(0.9, 1.6) # Fluctuate valuation
            current_holding_valuation = max(0, current_holding_valuation) # Cannot be negative

            if event_type == 'Dividend' and percent_holding > 0:
                # Simple dividend calculation
                cash_flow = current_holding_valuation * random.uniform(0.01, 0.05) # 1-5% dividend yield
                notes = "Dividend distribution"
                # Valuation usually doesn't change drastically JUST from dividend paid out
                fund_holding_val_post = current_holding_valuation

            elif event_type == 'Exit' and percent_holding > 0:
                 # Decide partial or full exit
                 is_partial = random.random() < 0.6 # 60% chance of partial
                 exit_valuation_multiple = random.uniform(0.8, 3.0) # Exit at +/- current mark
                 exit_proceeds_valuation = current_holding_valuation * exit_valuation_multiple

                 if is_partial:
                     percent_sold = random.uniform(0.1, 0.6) # Sell 10-60% of current stake
                     cash_flow = exit_proceeds_valuation * percent_sold # Proceeds based on % sold
                     fund_holding_val_post = current_holding_valuation * (1 - percent_sold) # Remaining value based on original holding val
                     percent_holding *= (1 - percent_sold) # Update remaining holding %
                     notes = f"Partial Exit ({percent_sold:.1%})"
                 else: # Full Exit
                     percent_sold = percent_holding # Sell remaining stake
                     cash_flow = exit_proceeds_valuation * percent_holding # Proceeds for the whole remaining stake
                     fund_holding_val_post = 0.0 # No value left
                     percent_holding = 0.0
                     notes = "Full Exit"
                     has_exited = True

            elif event_type == 'Valuation Update':
                 # Valuation already updated above
                 fund_holding_val_post = current_holding_valuation
                 notes = "Periodic valuation update"

            # Only record if the event type logic was met (e.g., didn't try to exit if already exited)
            if event_type == 'Valuation Update' or (event_type == 'Dividend' and cash_flow is not None) or (event_type == 'Exit' and percent_sold is not None):
                events.append((
                    company["portco_name"],
                    current_event_date.strftime('%Y-%m-%d'),
                    event_type,
                    round(cash_flow, 2) if cash_flow is not None else None,
                    currency,
                    round(percent_sold, 4) if percent_sold is not None else None,
                    round(fund_holding_val_post, 2) if fund_holding_val_post is not None else None,
                    notes
                ))

            # Advance time for next potential event
            days_to_next = random.randint(150, 400) # ~5-13 months
            current_event_date += timedelta(days=days_to_next)

    print(f"Generated {len(events)} event records.")
    return events


# --- Main Population Function ---

def populate_database():
    """Generates data and populates the SQLite database."""
    # !! DANGER: Delete existing database file !!
    if os.path.exists(DB_NAME):
        print(f"Deleting existing database: {DB_NAME}")
        os.remove(DB_NAME)

    conn = db_connect()
    create_tables(conn)
    c = conn.cursor()

    # Generate Data
    companies_data = generate_companies()
    investments_data = generate_investments(companies_data)
    kpis_data = generate_kpis(companies_data)

    # Need investments DataFrame for event generation logic
    investments_df_for_events = pd.DataFrame(investments_data, columns=[
        'fund_name', 'portco_name', 'type_of_investment', 'investment_round_number',
        'round_stage', 'date_of_investment', 'size_of_investment',
        'total_round_size', 'post_money_valuation'
    ])
    events_data = generate_events(companies_data, investments_df_for_events)


    # Insert Data using executemany
    try:
        print("Inserting companies...")
        # Prepare data as tuples for insertion
        company_tuples = [(c['portco_name'], c['year_founded'], c['industry_classification'], c['establishment_country'], c['status']) for c in companies_data]
        c.executemany(f'''INSERT INTO {PORTCO_TABLE_NAME} (portco_name, year_founded, industry_classification, establishment_country, status)
                          VALUES (?, ?, ?, ?, ?)''', company_tuples)

        print("Inserting investments...")
        c.executemany(f'''INSERT INTO {INVESTMENT_TABLE_NAME} (fund_name, portco_name, type_of_investment, investment_round_number, round_stage, date_of_investment, size_of_investment, total_round_size, post_money_valuation)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', investments_data)

        print("Inserting KPIs...")
        c.executemany(f'''INSERT INTO {KPI_TABLE_NAME} (portco_name, kpi_name, kpi_value, kpi_date, units, notes)
                          VALUES (?, ?, ?, ?, ?, ?)''', kpis_data)

        print("Inserting events...")
        c.executemany(f'''INSERT INTO {EVENT_TABLE_NAME} (portco_name, event_date, event_type, cash_flow_amount, currency, percent_holding_sold, fund_holding_valuation, notes)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', events_data)

        conn.commit()
        print("Data inserted successfully!")

    except sqlite3.Error as e:
        print(f"Database error during insertion: {e}")
        conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")


# --- Run the population ---
if __name__ == "__main__":
    populate_database()