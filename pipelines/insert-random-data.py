import random
import os
import json
from errors import (
    PipelineError,
    DataConflictError,
    InvalidFormatError,
    DatabaseConnectionError,
    SheetReadError
)
from staffing_roster_pipeline import (
    update_existing_consultant,
    build_availability_sql_columns,
    get_user_id_by_email,
    row_is_valid,
    update_existing_user,
    insert_into_consultants,
    insert_into_users
)
import logging
from dotenv import load_dotenv
from google.cloud.sql.connector import Connector

load_dotenv()


DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

def generate_test_rows(n=150, seed=42):
    if seed is not None:
        random.seed(seed)

    slot_cols = [f"Slot {i} GMT-0600" for i in range(30)]

    rows = []

    for i in range(n):
        row = {
            "Name": f"User {i}",
            "Email": f"user{i}@example.edu",
            "Gender": random.choice(["Male", "Female", "Unknown", "Prefer not to say"]),
            "Race": random.choice(["Asian", "White", "Black", "Hispanic", "Other"]),
            "US Citizen": random.choice(["Yes", "No"]),
            "Residency": random.choice(["Yes", "No"]),
            "First Generation": random.choice(["Yes", "No"]),
            "Current Role": random.choice(["SC", "SM", "PM", "EC", "NC"]),
            "NetID": f"netid{i}",

            "Year": random.choice(["Freshman", "Sophomore", "Junior", "Senior"]),
            "Major": random.choice([
                "Computer Science",
                "Economics",
                "Finance",
                "Mathematics",
                "Statistics",
                "Data Science",
                "Electrical Engineering",
                "Mechanical Engineering",
                "Industrial Engineering",
                "Business Analytics",
                "Information Systems",
                "Operations Research"
            ]),

            "Minor": random.choice([
                "",                # no minor
                "Statistics",
                "Psychology",
                "Business",
                "Economics",
                "Mathematics",
                "Data Science",
                "Philosophy",
                "Political Science",
                "Sociology"
            ]),

            "College": random.choice([
                "Engineering",
                "Arts & Sciences",
                "Business",
                "Information",
                "Computing",
                "Public Policy"
            ]),


            # populate consultant scores for ~95% rows
            "Consultant Score": round(random.uniform(65, 98), 2) if random.random() < 0.95 else "",

            "Semesters in IBC": random.randint(0, 8),
            "Time Zone": "Central",
            "Willing to Travel": random.choice(["Yes", "No", "Unknown"]),
            "Industry Interests": random.choice(["Tech", "Finance", "Healthcare"]),
            "Functional Area Interests": random.choice(["Strategy", "Data", "Ops"]),
            "Status": random.choice(["New", "Returning"]),
            "Week Before Finals Availability": random.choice(["Yes", "No"]),
        }

        # ---- CRITICAL PART ----
        # Mostly-filled slot values with *multiple* days
        for col in slot_cols:
            if random.random() < 0.9:  # 90% of slots filled
                k = random.choice([3, 4, 5])  # many days per slot
                row[col] = ", ".join(random.sample(DAYS, k))
            else:
                row[col] = ""  # empty slot

        rows.append(row)

    return rows

if __name__ == "__main__":
    try:
        sheet_data = generate_test_rows(150)
        if not sheet_data:
            raise SheetReadError("No data found in the sheet")
        for row in sheet_data:
            row.update(build_availability_sql_columns(row, sheet_data))

        # Validate required fields and ensure Current Role is 'NC'.
        valid_rows = []
        invalid_rows_info = []
        for row in sheet_data:
            ok, reason = row_is_valid(row)
            if ok:
                valid_rows.append(row)
            else:
                row_name = row.get("Name", "(no name)")
                # Create structured PipelineError instances so the log message matches existing format
                if reason.startswith("Missing required columns"):
                    err = InvalidFormatError(reason)
                    logging.warning(f"Missing data for {row_name}: {err}")
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
                # Try to find existing user by email
                email = row.get("Email")
                existing_user_id = get_user_id_by_email(cursor, email) if email else None
                
                if existing_user_id:
                    # Update existing records
                    update_existing_user(cursor, row, existing_user_id)
                    update_existing_consultant(cursor, row, existing_user_id)
                    logging.info(f"Updated existing user {row.get('Name', 'Unknown')} (user_id={existing_user_id}).")
                else:
                    # Insert new records
                    user_id = insert_into_users(cursor, row)
                    insert_into_consultants(cursor, row, user_id)
                    logging.info(f"Inserted new user {row.get('Name', 'Unknown')} (user_id={user_id}).")
            except DataConflictError as e:
                # Should rarely happen now as we check for existing users first
                logging.error(f"Unexpected duplicate conflict for {row.get('Name', '')}: {e}")
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
