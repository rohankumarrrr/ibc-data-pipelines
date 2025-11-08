import os
import json
import requests
import pg8000
from pg8000.dbapi import DatabaseError
import logging
import argparse
from google.cloud.sql.connector import Connector
from dotenv import load_dotenv
from errors import (
    PipelineError,
    DataConflictError,
    AuthorizationError,
    InvalidFormatError,
    DatabaseConnectionError,
    SheetReadError
)
load_dotenv()
import sys

logging.basicConfig(
    filename="pipeline.log",
    filemode="a",
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)

WEB_APP_URL = os.environ["WEB_APP_URL"]
SHEET_NAME = os.environ["SHEET_NAME"]

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

# Required sheet columns for a row to be considered valid for processing.
# Assumption: minimal required fields are Name, Email, Current Role, and NetID.
REQUIRED_SHEET_COLS = ["Name", "Email", "Current Role", "NetID", "Major"]

def row_is_valid_and_nc(row):
    """Return (True, None) if row has all required fields and Current Role is 'NC' (case-insensitive).
    Otherwise return (False, reason_string).
    """
    missing = []
    for col in REQUIRED_SHEET_COLS:
        val = row.get(col)
        if val is None or (isinstance(val, str) and val.strip() == ""):
            missing.append(col)
    if missing:
        return False, f"Missing required columns: {', '.join(missing)}"
    # Check Current Role equals NC
    curr_role = row.get("Current Role", "")
    if not isinstance(curr_role, str) or curr_role.strip().lower() != "nc":
        return False, f"Current Role is not 'NC' (value: '{curr_role}')"
    return True, None

def read_data_from_sheet():
    logging.info("Attempting to read all data from the Google Sheet...")
    params = {"action": "read", "path": SHEET_NAME}
    try:
        response = requests.get(WEB_APP_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Successfully read {len(data)} rows from the sheet.")
        return data
    except requests.exceptions.RequestException as e:
        raise SheetReadError(f"HTTP request failed: {e}")
    except json.JSONDecodeError:
        raise InvalidFormatError("Sheet returned invalid JSON format")

def build_availability_sql_columns(row_entry, sheet_data):
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
            user_vals.append(None if val == "" else val)
    if not user_cols:
        raise InvalidFormatError("No valid user columns found in row")
    query = f"INSERT INTO users ({', '.join(user_cols)}) VALUES ({', '.join(['%s'] * len(user_vals))}) RETURNING user_id;"
    try:
        cursor.execute(query, user_vals)
        user_id = cursor.fetchone()[0]
        return user_id
    except DatabaseError as e:
        err = e.args[0]
        if isinstance(err, dict) and err.get("C") == "23505":
            raise DataConflictError(f"Duplicate key violation: {err.get('M')}")
        else:
            raise

def insert_into_consultants(cursor, row, user_id):
    consultant_cols = []
    consultant_vals = []
    for sheet_col, sql_col in SHEET_COLS_TO_SQL_COLS.items():
        if sheet_col in row and sql_col in CONSULTANTS_COLS:
            consultant_cols.append(sql_col)
            val = row[sheet_col]
            consultant_vals.append(None if val == "" else val)
    for avail_col in ["availability_mon", "availability_tue", "availability_wed", "availability_thu", "availability_fri", "availability_sat", "availability_sun"]:
        if avail_col in row:
            consultant_cols.append(avail_col)
            consultant_vals.append(row[avail_col])
    consultant_cols.append("user_id")
    consultant_vals.append(user_id)
    query = f"INSERT INTO consultants ({', '.join(consultant_cols)}) VALUES ({', '.join(['%s'] * len(consultant_vals))});"
    cursor.execute(query, consultant_vals)

if __name__ == "__main__":
    try:
        sheet_data = read_data_from_sheet()
        if not sheet_data:
            raise SheetReadError("No data found in the sheet")
        for row in sheet_data:
            row.update(build_availability_sql_columns(row, sheet_data))

        # Validate required fields and ensure Current Role is 'NC'.
        valid_rows = []
        invalid_rows_info = []
        for row in sheet_data:
            ok, reason = row_is_valid_and_nc(row)
            if ok:
                valid_rows.append(row)
            else:
                row_name = row.get("Name", "(no name)")
                # Create structured PipelineError instances so the log message matches existing format
                if reason.startswith("Missing required columns"):
                    err = InvalidFormatError(reason)
                    logging.warning(f"Missing data for {row_name}: {err}")
                elif reason.startswith("Current Role is not 'NC'"):
                    err = AuthorizationError(reason)
                    logging.warning(f"Wrong role for {row_name}: {err}")
                else:
                    err = InvalidFormatError(reason)
                    logging.warning(f"Invalid row for {row_name}: {err}")
                invalid_rows_info.append({"row": row_name, "reason": reason})

        valid_count = len(valid_rows)
        invalid_count = len(invalid_rows_info)
        logging.info(f"Row validation complete: {valid_count} valid, {invalid_count} invalid/missing.")
        try:
            connector = Connector()
            conn = connector.connect(
                os.environ["CLOUD_SQL_CONNECTION_NAME"],
                "pg8000",
                user=os.environ["DB_USER"],
                password=os.environ["DB_PASSWORD"],
                db=os.environ["DB_NAME"]
            )
            logging.info("Successfully connected to Cloud SQL Postgres instance.")
        except Exception as e:
            raise DatabaseConnectionError(f"Database connection failed: {e}")
        cursor = conn.cursor()
        # Only process rows that passed validation
        for row in valid_rows:
            try:
                user_id = insert_into_users(cursor, row)
                insert_into_consultants(cursor, row, user_id)
                logging.info(f"Inserted user {row.get('Name', 'Unknown')} (user_id={user_id}).")
            except DataConflictError as e:
                logging.warning(f"Duplicate user detected for {row.get('Name', '')}: {e}")
                conn.rollback()
                continue
            except PipelineError as e:
                logging.error(f"Row {row.get('Name', '')} failed [{e.code}]: {e.message}")
                conn.rollback()
                continue
            except Exception as e:
                logging.error(f"Unexpected insert error for row {row.get('Name', '')}: {e}")
                conn.rollback()
                continue
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("All valid rows inserted successfully.")
        # Print a concise summary for callers/CI: number of valid and invalid rows
        summary = {"valid_rows": valid_count, "invalid_rows": invalid_count}
        print(json.dumps(summary))
    except PipelineError as e:
        logging.critical(f"Pipeline failed [{e.code}] {e.message}")
        print(f"Pipeline failed with error {e.code}: {e.message}")
    except Exception as e:
        logging.critical(f"Unexpected fatal error: {e}")
        print(f"Unexpected fatal error: {e}")
