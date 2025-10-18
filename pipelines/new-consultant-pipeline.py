import requests
import json
from google.cloud.sql.connector import Connector
import pg8000

WEB_APP_URL = "https://script.google.com/macros/s/AKfycbweR5LRWcpM-SB8e_P7Ofk67zt_muND7mIAxUsy3kLBPK9QUYt5ghC9k1sBX7ozwgd3FQ/exec"
SHEET_NAME = "NCs"

SHEET_COLS_TO_SQL_COLS = {
    "Name": "name",
    "Email": "email",
    "Gender": "gender",
    "Race": "race",
    "US Citizen": "us_citizen",
    "Residency": "residency",
    "First Generation": "first_gen",
    "Current Role": "curr_role",
    "NetID": "netid",
    "Year": "year",
    "Major": "major",
    "Minor": "minor",
    "College": "college",
    "Consultant Score": "consultant_score",
    "Semesters in IBC": "semesters_in_ibc",
    "Time Zone": "time_zone",
    "Willing to Travel": "willing_to_travel",
    "Industry Interests": "industry_interests",
    "Functional Area Interests": "functional_area_interests",    
    "Status": "status",
    "Week Before Finals Availability": "week_before_finals_availability",         
}

USERS_COLS = {"name", "email", "gender", "race", "us_citizen", "residency", "first_gen", "curr_role", "netid"}
CONSULTANTS_COLS = {"year", "major", "minor", "college", "consultants_score", "semesters_in_ibc", "time_zone", "willing_to_travel", "industry_interests", "functional_area_interests", "status", "week_before_finals_availability", "user_id"}

def read_data_from_sheet():
    """
    Reads all data from the specified Google Sheet and returns it as a list of dicts.
    """
    print("\nAttempting to read all data from the sheet...")
    
    params = {
        "action": "read",
        "path": SHEET_NAME
    }
    
    try:
        response = requests.get(WEB_APP_URL, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"An HTTP error occurred: {e}")
    except json.JSONDecodeError:
        print(f"Failed to decode JSON. Raw response: {response.text}")
    
    return None

def build_availability_sql_columns(row_entry):
    
    time_slots = [key for key in sheet_data[0].keys() if "GMT-0600" in key]
    time_slots.sort()

    availability_sql_columns = ["availability_mon", "availability_tue", "availability_wed", "availability_thu", "availability_fri", "availability_sat", "availability_sun"]
    availabilities = {day: ['0'] * 30 for day in availability_sql_columns}

    for idx, slot in enumerate(time_slots):
        available_days_str = row_entry.get(slot, "")
        if not available_days_str or not available_days_str.strip():
            continue
        available_days = [day.strip().lower() for day in available_days_str.split(",")]
        for day in available_days:
            if day == "monday":
                availabilities["availability_mon"][idx] = '1'
            elif day == "tuesday":
                availabilities["availability_tue"][idx] = '1'
            elif day == "wednesday":
                availabilities["availability_wed"][idx] = '1'
            elif day == "thursday":
                availabilities["availability_thu"][idx] = '1'
            elif day == "friday":
                availabilities["availability_fri"][idx] = '1'
            elif day == "saturday":
                availabilities["availability_sat"][idx] = '1'
            elif day == "sunday":
                availabilities["availability_sun"][idx] = '1'

    output = {day: "".join(bits) for day, bits in availabilities.items()}
    return output

def parse_boolean(value):
    if isinstance(value, str):
        value_lower = value.strip().lower()
        if value_lower in ("yes", "true", "1"):
            return True
        elif value_lower in ("no", "false", "0"):
            return False
    elif isinstance(value, bool):
        return value
    return False  


def insert_into_users(cursor, row):
    user_cols = []
    user_vals = []
    
    boolean_cols = {"us_citizen", "residency", "first_gen", "week_before_finals_availability"}
    
    for sheet_col, sql_col in SHEET_COLS_TO_SQL_COLS.items():
        if sheet_col in row and sql_col in USERS_COLS:
            val = row[sheet_col]
            if sql_col in boolean_cols:
                val = parse_boolean(val)
            user_cols.append(sql_col)
            user_vals.append(val)
    
    user_cols_str = ", ".join(user_cols)
    user_vals_placeholders = ", ".join(["%s"] * len(user_vals))
    user_sql_query = f"INSERT INTO users ({user_cols_str}) VALUES ({user_vals_placeholders}) RETURNING user_id;"
    
    cursor.execute(user_sql_query, user_vals)
    user_id = cursor.fetchone()[0]
    return user_id



def insert_into_consultants(cursor, row, user_id):
    consultant_cols = []
    consultant_vals = []
    for sheet_col, sql_col in SHEET_COLS_TO_SQL_COLS.items():
        if sheet_col in row and sql_col in CONSULTANTS_COLS:
            consultant_cols.append(sql_col)
            consultant_vals.append(row[sheet_col])
    
    for avail_col in ["availability_mon", "availability_tue", "availability_wed", "availability_thu", "availability_fri", "availability_sat", "availability_sun"]:
        if avail_col in row:
            consultant_cols.append(avail_col)
            consultant_vals.append(row[avail_col])
    
    consultant_cols.append("user_id")
    consultant_vals.append(user_id)
    
    consultant_cols_str = ", ".join(consultant_cols)
    consultant_vals_placeholders = ", ".join(["%s"] * len(consultant_vals))
    consultant_sql_query = f"INSERT INTO consultants ({consultant_cols_str}) VALUES ({consultant_vals_placeholders});"
    
    cursor.execute(consultant_sql_query, consultant_vals)

if __name__ == "__main__":
    # Step 1: Read the sheet
    sheet_data = read_data_from_sheet()
    if not sheet_data:
        print("No data found in the sheet. Exiting.")
        exit(1)

    # Step 2: Build availability columns for each row
    for row in sheet_data:
        row.update(build_availability_sql_columns(row))

    # Step 3: Connect to your Cloud SQL instance
    connector = Connector()
    conn = connector.connect(
        "avid-influence-457813-t0:us-central1:ibc-postgres-db-dev",  # Replace with your Cloud SQL instance connection name
        "pg8000",
        user="postgres",
        password="magelli923",
        db="ibc"
    )
    cursor = conn.cursor()

    # Step 4: Insert each row into users and consultants
    for row in sheet_data:
        try:
            user_id = insert_into_users(cursor, row)
            insert_into_consultants(cursor, row, user_id)
        except Exception as e:
            print(f"Error inserting row {row.get('Name', '')}: {e}")

    # Step 5: Commit all changes and close connection
    conn.commit()
    cursor.close()
    conn.close()

    print("All rows inserted successfully.")

    
