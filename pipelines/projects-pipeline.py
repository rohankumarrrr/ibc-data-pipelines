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
        "em_netid": ["em_netid", "EM net-id", "EM NetID"],
        "sm_netid": ["sm_netid", "SM net-id", "SM NetID"],
        "pm_netid": ["pm_netid", "PM net-id", "PM NetID"],
        "sc1_netid": ["sc1_netid", "SC1 net-id", "SC 1 net-id", "SC 1 NetID"],
        "sc2_netid": ["sc2_netid", "SC2 net-id", "SC 2 net-id", "SC 2 NetID"],
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

def get_user_id_by_netid(cursor, netid):
    """Find a user_id by their netid."""
    if not netid or not isinstance(netid, str) or netid.strip() == "":
        return None
    cursor.execute("SELECT user_id FROM users WHERE netid = %s;", (netid,))
    result = cursor.fetchone()
    return result[0] if result else None


def get_user_id_for_role_by_netid(cursor, netid, role_code):
    """
    Finds a user_id by netid and updates their role if needed.
    Returns the user_id or None if the netid is missing.
    Raises an error if the netid is not found in the database.
    """
    if not netid:
        return None

    user_id = get_user_id_by_netid(cursor, netid)

    if user_id is None:
        raise InvalidFormatError(f"NetID '{netid}' for role {role_code} not found in database")

    update_user_role_if_needed(cursor, user_id, role_code)
    return user_id


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
    current_data_query = """
        SELECT p.project_semester, p.client_name, 
               u_em.netid, u_sm.netid, u_pm.netid, u_sc1.netid, u_sc2.netid
        FROM projects p
        LEFT JOIN users u_em ON p.em_id = u_em.user_id
        LEFT JOIN users u_sm ON p.sm_id = u_sm.user_id
        LEFT JOIN users u_pm ON p.pm_id = u_pm.user_id
        LEFT JOIN users u_sc1 ON p.sc1_id = u_sc1.user_id
        LEFT JOIN users u_sc2 ON p.sc2_id = u_sc2.user_id
        WHERE p.project_id = %s;
    """
    cursor.execute(current_data_query, (project_id,))
    current = cursor.fetchone()

    # Get user IDs for new netids
    em_id  = get_user_id_for_role_by_netid(cursor, row.get("em_netid"),  "EM")
    sm_id  = get_user_id_for_role_by_netid(cursor, row.get("sm_netid"),  "SM")
    pm_id  = get_user_id_for_role_by_netid(cursor, row.get("pm_netid"),  "PM")
    sc1_id = get_user_id_for_role_by_netid(cursor, row.get("sc1_netid"), "SC")
    sc2_id = get_user_id_for_role_by_netid(cursor, row.get("sc2_netid"), "SC")
    
    new_vals = (
        row.get("project_semester"),
        row.get("client_name"),
        row.get("em_netid"),
        row.get("sm_netid"),
        row.get("pm_netid"),
        row.get("sc1_netid"),
        row.get("sc2_netid"),
    )
    
    # Check if any values changed
    if current == new_vals:
        logging.info(f"Project '{row.get('project_name')}' (ID={project_id}) unchanged, skipping")
        return None
    
    query = """
        UPDATE projects
        SET project_semester = %s, client_name = %s, em_id = %s, sm_id = %s, pm_id = %s, sc1_id = %s, sc2_id = %s
        WHERE project_id = %s;
    """
    cursor.execute(query, (row.get("project_semester"), row.get("client_name"), em_id, sm_id, pm_id, sc1_id, sc2_id, project_id))
    logging.info(f"Updated project '{row.get('project_name')}' (ID={project_id})")
    return project_id

def insert_project(cursor, row):

    # Check if project already exists by name
    cursor.execute("SELECT project_id FROM projects WHERE project_name = %s;", (row.get("project_name"),))
    project_id_tuple = cursor.fetchone()
    
    if project_id_tuple:
        # Project exists, so update it
        return update_project(cursor, project_id_tuple[0], row)

    # Handle optional user IDs for roles by looking up netids
    em_id  = get_user_id_for_role_by_netid(cursor, row.get("em_netid"),  "EM")
    sm_id  = get_user_id_for_role_by_netid(cursor, row.get("sm_netid"),  "SM")
    pm_id  = get_user_id_for_role_by_netid(cursor, row.get("pm_netid"),  "PM")
    sc1_id = get_user_id_for_role_by_netid(cursor, row.get("sc1_netid"), "SC")
    sc2_id = get_user_id_for_role_by_netid(cursor, row.get("sc2_netid"), "SC")

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
