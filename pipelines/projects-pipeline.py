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

PROJECT_WEB_APP_URL = os.environ["PROJECT_WEB_APP_URL"]
PROJECTS_SHEET_NAME = os.environ["PROJECT_SHEET_NAME"]

REQUIRED_PROJECT_FIELDS = [
    "project_name"
]

# -------------------------------------------------------
#  Helpers: Sheet → Data
# -------------------------------------------------------

def read_project_sheet():
    logging.info("Reading projects sheet...")
    params = {"action": "read", "path": PROJECTS_SHEET_NAME}

    try:
        res = requests.get(PROJECT_WEB_APP_URL, params=params, timeout=10)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        raise SheetReadError(str(e))
    except json.JSONDecodeError:
        raise InvalidFormatError("Invalid JSON formatting from sheet")


def normalize_project_row(row):
    """Normalize incoming row keys to the underscore-style labels used by this pipeline.
    This supports both human-readable headers (e.g. 'Project Name', 'EM net-id') and
    already-normalized keys (e.g. 'project_name', 'em_netid')."""
    out = {}
    # map of target_key -> list of possible source keys (in order of preference)
    KEY_MAP = {
        "project_name": ["project_name", "Project Name"],
        "project_semester": ["project_semester", "Semester"],
        "client_name": ["client_name", "Client Name"],
        "em_id": ["em_id", "EM id", "EM ID"],
        "sm_id": ["sm_id", "SM id", "SM ID"],
        "pm_id": ["pm_id", "PM id", "PM ID"],
        "sc1_id": ["sc1_id", "SC1 id", "SC 1 id", "SC 1 ID"],
        "sc2_id": ["sc2_id", "SC2 id", "SC 2 id", "SC 2 ID"],
    }

    for target, candidates in KEY_MAP.items():
        for c in candidates:
            if c in row and row.get(c) is not None and str(row.get(c)).strip() != "":
                out[target] = row.get(c)
                break
        # if not found, set to None to make downstream logic simpler
        if target not in out:
            out[target] = None

    # Preserve any other keys present in the row (so we don't lose unexpected data)
    for k, v in row.items():
        if k not in out:
            out[k] = v

    return out


def project_row_valid(row):
    # Only project_name must be present
    name = row.get("project_name")
    if name is None or (isinstance(name, str) and name.strip() == ""):
        return False, "project_name is required"

    return True, None


# -------------------------------------------------------
#  Database helpers
# -------------------------------------------------------

def user_id_exists(cursor, user_id):
    cursor.execute("SELECT 1 FROM users WHERE user_id = %s;", (user_id,))
    return cursor.fetchone() is not None

def optional_user_for_role(cursor, user_id, role_code):
    """
    Returns user_id or None if user_id is missing.
    Updates user role if user_id is provided and exists.
    """
    if user_id is None or (isinstance(user_id, str) and user_id.strip() == ""):
        return None

    try:
        user_id = int(user_id) if isinstance(user_id, str) else user_id
    except (ValueError, TypeError):
        raise InvalidFormatError(f"Invalid user ID '{user_id}' → must be numeric")

    if not user_id_exists(cursor, user_id):
        raise InvalidFormatError(f"User ID '{user_id}' does not exist in database")

    update_user_role_if_needed(cursor, user_id, role_code)
    return user_id

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


def link_consultant_to_project(cursor, project_id, user_id, role_code):
    """
    Links a consultant to a project in the consultant_projects table.
    Does nothing if user_id is None.
    """
    if user_id is None:
        return

    query = """
        INSERT INTO consultant_projects (project_id, user_id, role)
        VALUES (%s, %s, %s);
    """
    cursor.execute(query, (project_id, user_id, role_code))
    logging.info(f"Linked consultant {user_id} to project {project_id} as {role_code}")



# -------------------------------------------------------
#  Insert Project
# -------------------------------------------------------

def project_exists(cursor, project_name):
    """Check if a project with this name already exists."""
    cursor.execute("SELECT project_id FROM projects WHERE project_name = %s;", (project_name,))
    result = cursor.fetchone()
    return result[0] if result else None

def get_project_data(cursor, project_id):
    """Fetch current project data."""
    cursor.execute(
        """
        SELECT project_name, project_semester, client_name, em_id, sm_id, pm_id, sc1_id, sc2_id
        FROM projects WHERE project_id = %s;
        """,
        (project_id,)
    )
    return cursor.fetchone()

def update_project(cursor, project_id, row):
    """Update project fields that have changed."""
    current = get_project_data(cursor, project_id)
    
    new_vals = (
        row.get("project_semester"),
        row.get("client_name"),
        row.get("em_id"),
        row.get("sm_id"),
        row.get("pm_id"),
        row.get("sc1_id"),
        row.get("sc2_id"),
    )
    
    # Check if any values changed (skip project_name as it's the key)
    if current[1:] == new_vals:
        logging.info(f"Project '{row.get('project_name')}' (ID={project_id}) unchanged, skipping")
        return None
    
    query = """
        UPDATE projects
        SET project_semester = %s, client_name = %s, em_id = %s, sm_id = %s, pm_id = %s, sc1_id = %s, sc2_id = %s
        WHERE project_id = %s;
    """
    cursor.execute(query, new_vals + (project_id,))
    logging.info(f"Updated project '{row.get('project_name')}' (ID={project_id})")
    return project_id

def insert_project(cursor, row):

    # Check if project already exists
    project_id = project_exists(cursor, row.get("project_name"))
    
    if project_id:
        return update_project(cursor, project_id, row)

    # Handle optional user IDs for roles (normalized labels)
    em_id  = optional_user_for_role(cursor, row.get("em_id"),  "EM")
    sm_id  = optional_user_for_role(cursor, row.get("sm_id"),  "SM")
    pm_id  = optional_user_for_role(cursor, row.get("pm_id"),  "PM")
    sc1_id = optional_user_for_role(cursor, row.get("sc1_id"), "SC")
    sc2_id = optional_user_for_role(cursor, row.get("sc2_id"), "SC")

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
        row.get("project_name"),
        row.get("project_semester"),
        row.get("client_name"),
        em_id,
        sm_id,
        pm_id,
        sc1_id,
        sc2_id
    )

    cursor.execute(query, vals)
    project_id = cursor.fetchone()[0]
    logging.info(f"Inserted project '{row.get('project_name')}' (ID={project_id})")

    # -------------------------------
    # Mark SM, PM, and SCs as returning
    # -------------------------------
    mark_consultant_returning(cursor, sm_id)
    mark_consultant_returning(cursor, pm_id)
    mark_consultant_returning(cursor, sc1_id)
    mark_consultant_returning(cursor, sc2_id)

    # -------------------------------
    # Link consultants to project
    # -------------------------------
    link_consultant_to_project(cursor, project_id, em_id, "EM")
    link_consultant_to_project(cursor, project_id, sm_id, "SM")
    link_consultant_to_project(cursor, project_id, pm_id, "PM")
    link_consultant_to_project(cursor, project_id, sc1_id, "SC")
    link_consultant_to_project(cursor, project_id, sc2_id, "SC")

    return project_id

# -------------------------------------------------------
#  Main pipeline execution
# -------------------------------------------------------

if __name__ == "__main__":
    try:
        sheet_data = read_project_sheet()

        if not sheet_data:
            raise SheetReadError("No rows found in project sheet")

        # Normalize headers for each incoming row so either human or normalized headers work
        sheet_data = [normalize_project_row(r) for r in sheet_data]

        valid_rows = []
        invalid = []

        for row in sheet_data:
            ok, reason = project_row_valid(row)
            if ok:
                valid_rows.append(row)
            else:
                logging.warning(f"Invalid project row: {reason}")
                invalid.append({"row": row.get("project_name", "(no name)"), "reason": reason})

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
                logging.error(f"PipelineError for '{row.get('project_name', '?')}': {e}")
                conn.rollback()
            except Exception as e:
                logging.error(f"Unexpected error inserting '{row.get('project_name', '?')}': {e}")
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
