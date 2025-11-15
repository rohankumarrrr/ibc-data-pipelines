import os
import logging
from google.cloud.sql.connector import Connector
from dotenv import load_dotenv
import sys

# Add the project root directory to the Python path for utils and errors
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from errors import DatabaseConnectionError, PipelineError

# Load environment variables from .env file for local development
load_dotenv()

# Configure logging to write to the same pipeline.log file
logging.basicConfig(
    filename="pipeline.log",
    filemode="a",
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)

def run_end_of_semester_updates():
    """
    Connects to the database and performs end-of-semester updates.
    - Sets status to 'Deferred' for all consultants.
    - Increments semesters_in_ibc by 1 for all consultants.
    """
    conn = None
    try:
        logging.info("Starting end-of-semester pipeline...")

        # Establish database connection using environment variables
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

        # SQL query to update status and increment semesters_in_ibc
        update_query = """
        UPDATE consultants
        SET
            status = 'Deferred',
            semesters_in_ibc = semesters_in_ibc + 1;
        """

        logging.info("Executing update query on consultants table...")
        cursor.execute(update_query)
        
        # Get the number of rows affected
        updated_rows = cursor.rowcount
        
        conn.commit()
        logging.info(f"Successfully updated {updated_rows} rows in the consultants table.")
        print(f"End-of-semester pipeline complete. Updated {updated_rows} consultants.")

    except PipelineError as e:
        logging.critical(f"Pipeline failed [{e.code}] {e.message}")
        print(f"Pipeline failed with error {e.code}: {e.message}")
    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}")
        if conn:
            conn.rollback()
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    run_end_of_semester_updates()