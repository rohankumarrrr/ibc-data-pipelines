import os
import json
import requests
import pg8000
from pg8000.dbapi import DatabaseError
import logging
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

logging.basicConfig(
    filename="projects_pipeline.log",
    filemode="a",
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)

WEB_APP_URL = os.environ["WEB_APP_URL"]
PROJECTS_SHEET_NAME = os.environ["PROJECT_SHEET_NAME"]

REQUIRED_PROJECT_FIELDS = [
    "Project Name"
]

# -------------------------------------------------------
#  Helpers: Sheet → Data
# -------------------------------------------------------

def read_project_sheet():
    logging.info("Reading projects sheet...")
    params = {"action": "read", "path": PROJECTS_SHEET_NAME}

    try:
        res = requests.get(WEB_APP_URL, params=params, timeout=10)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        raise SheetReadError(str(e))
    except json.JSONDecodeError:
        raise InvalidFormatError("Invalid JSON formatting from sheet")


def project_row_valid(row):
    # Only Project Name must be present
    name = row.get("Project Name")
    if name is None or (isinstance(name, str) and name.strip() == ""):
        return False, "Project Name is required"

    return True, None


# -------------------------------------------------------
#  Database helpers
# -------------------------------------------------------

def optional_user_for_role(cursor, netid, role_code):
    """
    Returns user_id or None if netid is missing.
    Ensures role only if netid is not empty.
    """
    if netid is None or (isinstance(netid, str) and netid.strip() == ""):
        return None

    user_id = get_user_id_by_netid(cursor, netid)
    if not user_id:
        raise InvalidFormatError(f"Invalid NetID '{netid}' → user not found")

    update_user_role_if_needed(cursor, user_id, role_code)
    return user_id

def get_user_id_by_netid(cursor, netid):
    cursor.execute("SELECT user_id FROM users WHERE netid = %s;", (netid,))
    result = cursor.fetchone()
    return result[0] if result else None


def update_user_role_if_needed(cursor, user_id, expected_role):
    """If curr_role != expected_role, update it."""
    cursor.execute("SELECT curr_role FROM users WHERE user_id = %s;", (user_id,))
    row = cursor.fetchone()

    if not row:
        raise PipelineError("User disappeared mid-transaction")

    curr = row[0]
    if curr != expected_role:
        cursor.execute(
            "UPDATE users SET curr_role = %s WHERE user_id = %s;",
            (expected_role, user_id)
        )
        logging.info(f"Updated role for user_id={user_id}: {curr} → {expected_role}")


def ensure_user_for_role(cursor, netid, role_code):
    """
    role_code in {"em","sm","pm","sc"}
    Returns the user_id.
    Throws error if user doesn't exist.
    """
    user_id = get_user_id_by_netid(cursor, netid)
    if not user_id:
        raise InvalidFormatError(f"Invalid NetID '{netid}' → no matching user in database")

    update_user_role_if_needed(cursor, user_id, role_code)
    return user_id

def mark_consultant_returning(cursor, user_id):
    """
    Sets consultant.status = 'returning' for the given user_id.
    Does nothing if user_id is None.
    """
    if user_id is None:
        return

    cursor.execute(
        """
        UPDATE consultants
        SET status = 'returning'
        WHERE user_id = %s;
        """,
        (user_id,)
    )
    logging.info(f"Consultant {user_id} marked as returning")



# -------------------------------------------------------
#  Insert Project
# -------------------------------------------------------

def insert_project(cursor, row):

    # Handle optional net-ids for roles
    em_id  = optional_user_for_role(cursor, row.get("EM net-id"),  "EM")
    sm_id  = optional_user_for_role(cursor, row.get("SM net-id"),  "SM")
    pm_id  = optional_user_for_role(cursor, row.get("PM net-id"),  "PM")
    sc1_id = optional_user_for_role(cursor, row.get("SC 1 net-id"), "SC")
    sc2_id = optional_user_for_role(cursor, row.get("SC 2 net-id"), "SC")

    query = """
        INSERT INTO projects (
            project_name,
            project_semester,
            client_name,
            em_id,
            sm_id,
            pm_id,
            sc1_id,
            sc2_id
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING project_id;
    """

    vals = (
        row.get("Project Name"),
        row.get("Semester"),
        row.get("Client Name"),
        em_id,
        sm_id,
        pm_id,
        sc1_id,
        sc2_id
    )

    cursor.execute(query, vals)
    project_id = cursor.fetchone()[0]
    logging.info(f"Inserted project '{row['Project Name']}' (ID={project_id})")

    # -------------------------------
    # Mark SM, PM, and SCs as returning
    # -------------------------------
    mark_consultant_returning(cursor, sm_id)
    mark_consultant_returning(cursor, pm_id)
    mark_consultant_returning(cursor, sc1_id)
    mark_consultant_returning(cursor, sc2_id)

    return project_id

# -------------------------------------------------------
#  Main pipeline execution
# -------------------------------------------------------

if __name__ == "__main__":
    try:
        sheet_data = read_project_sheet()

        if not sheet_data:
            raise SheetReadError("No rows found in project sheet")

        valid_rows = []
        invalid = []

        for row in sheet_data:
            ok, reason = project_row_valid(row)
            if ok:
                valid_rows.append(row)
            else:
                logging.warning(f"Invalid project row: {reason}")
                invalid.append({"row": row.get("Project Name", "(no name)"), "reason": reason})

        # Connect to Cloud SQL
        try:
            connector = Connector()
            conn = connector.connect(
                os.environ["CLOUD_SQL_CONNECTION_NAME"],
                "pg8000",
                user=os.environ["DB_USER"],
                password=os.environ["DB_PASSWORD"],
                db=os.environ["DB_NAME"]
            )
        except Exception as e:
            raise DatabaseConnectionError(str(e))

        cursor = conn.cursor()

        # Insert each project
        for row in valid_rows:
            try:
                insert_project(cursor, row)

            except PipelineError as e:
                logging.error(f"PipelineError for '{row.get('Project Name', '?')}': {e}")
                conn.rollback()
            except Exception as e:
                logging.error(f"Unexpected error inserting '{row.get('Project Name', '?')}': {e}")
                conn.rollback()

        conn.commit()
        cursor.close()
        conn.close()

        print(json.dumps({
            "valid_rows": len(valid_rows),
            "invalid_rows": len(invalid)
        }))

    except Exception as e:
        logging.critical(f"Fatal pipeline error: {e}")
        print(f"Fatal pipeline error: {e}")
