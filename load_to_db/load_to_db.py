import os
import sys
import git

# Adding the repository root to the sys.path.
repo = git.Repo(".", search_parent_directories=True)
ROOT_DIR = repo.working_tree_dir
sys.path.append(ROOT_DIR)

from library.user_input_utils import (
    ensure_file_exists,
    enter_for_default,
    yes_true_else_false,
    ensure_lastpass_entry_exists,
    ensure_schema_exists,
)
from library.database_utils import (
    check_if_table_exists,
    connect_to_db_with_psycopg2,
    connect_to_db_with_sqlalchemy,
    get_table_row_count,
    insert_df_to_db,
)
from dotenv import load_dotenv

from sqlalchemy.exc import ProgrammingError
import pandas as pd

# Load environmental file
from library.log_config import get_logger

load_dotenv()

LASTPASS_USERNAME = os.environ.get("LASTPASS_USERNAME")
MSP_STAGING = os.environ.get("MSP_STAGING_LASTPASS_ENTRY")
DEFAULT_CSV_LOCATION = os.environ.get("DEFAULT_CSV_LOCATION")
DEFAULT_SCHEMA = os.environ.get("DEFAULT_SCHEMA")


if __name__ == "__main__":
    # Initiate logging
    log = get_logger(__name__)

    # Ensure the LastPass Entry exists
    lpass_manager = ensure_lastpass_entry_exists(MSP_STAGING)

    # Connect to Db through psycopg (allows querying)
    conn_psy2 = connect_to_db_with_psycopg2(lpass_manager)

    # Ensure file exists
    filename, directory, filepath = ensure_file_exists(
        f"What is the name of the csv you would like to load to the '{lpass_manager.database}' database? ",
        "Where is the file located?",
        DEFAULT_CSV_LOCATION,
    )

    # File to dataframe
    file_as_df = pd.read_csv(filepath)
    file_length = len(file_as_df.index)
    log.info(f"{file_length} rows exist in '{filename}'")

    # Ensure schema exists in database
    schema, df_tables_in_schema = ensure_schema_exists(DEFAULT_SCHEMA, conn_psy2)

    # Connect to Db through sequelalchemy (allows pd.to_sql)
    conn_sa = connect_to_db_with_sqlalchemy(lpass_manager)

    # Loop until the user determines the desired table to import to, and the import succeeds.

    while True:
        # Default table name = same as filename (without .csv)
        if filename[-4:] == ".csv":
            table_guess = filename[:-4]
            table = enter_for_default("What is the table name?", table_guess)
        else:
            table = enter_for_default("What is the table name?", filename)

        # Check if table exists
        if check_if_table_exists(schema, table, df_tables_in_schema):
            intended = yes_true_else_false(
                f"The table `{table}` already exists. Was this expected?",
            )

            # The table exists AND they knew it existed
            if intended:
                # Loop until they properly indicate they want to append or overwrite the existing data
                while True:
                    append_replace = input(
                        "Do you want to (O)verwrite, or (A)ppend to the existing table? "
                    )
                    table_exists = True
                    # They want to drop the existing table
                    if append_replace.lower() in ("overwrite", "o"):
                        append_replace = "replace"
                        break
                    # They want to append to the existing table.
                    elif append_replace.lower() in ("append", "a"):
                        append_replace = "append"
                        break

                if append_replace == "append":
                    # Check if the table columns match the csv columns
                    try:
                        table_rows_pre = get_table_row_count(schema, table, conn_psy2)
                        result = insert_df_to_db(
                            file_as_df, conn_sa, table, append_replace
                        )
                        if result is not None:
                            table_rows = get_table_row_count(schema, table, conn_psy2)
                            log.info(
                                f"{table_rows - table_rows_pre} rows appended and {table_rows} now exist in '{schema}'.'{table}',"
                            )
                            # SUCCESS! If we get here, the rows in the csv existed in the table, and new rows were appended.
                        break
                    except ProgrammingError as e:
                        # At least one of the rows in the csv did not exist in the table. Go back to the start of the loop.
                        print("\n")
                        log.error(
                            f"Appending FAILED.\nEnsure the csv columns exist in the table.\nConsider overwriting existing table, using a new table name, or renaming the csv columns.\n"
                        )
                # Replace the existing table with the new data, regardless of the columns.
                if append_replace == "replace":
                    result = insert_df_to_db(file_as_df, conn_sa, table, append_replace)
                    if result is not None:
                        table_rows = get_table_row_count(schema, table, conn_psy2)
                        log.info(
                            f"'{schema}'.'{table}' was dropped, and {table_rows} rows were written in its place."
                        )
                    # SUCCESS! The existing table was dropped and new data inserted.
                    break

        # If the table name is blank, start over
        elif table == "":
            pass
        # A table with that name does not yet exist
        else:
            # Did they intend to create a new table? If not, go back to start of loop.
            intended = yes_true_else_false(
                f"A new table, '{table}' will be created. Ready to continue?"
            )
            table_exists = False

            # They intended to create a new table, create new table.
            if intended:
                result = insert_df_to_db(file_as_df, conn_sa, table)

                if result is not None:
                    table_rows = get_table_row_count(schema, table, conn_psy2)
                    log.info(f"{table_rows} rows were written to '{schema}'.'{table}'")
                    # SUCCESS! A new table was created and rows inserted.
                break

    conn_sa.close()
