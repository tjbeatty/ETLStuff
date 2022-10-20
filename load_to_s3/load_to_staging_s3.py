import os
import sys
import git
import shutil
import json
import pandas as pd
import numpy as np
import pandera as pa
from pandera import Column, DataFrameSchema, Check, Index
from pandera.errors import SchemaErrors
from sqlalchemy import JSON
from datetime import datetime as dt
from typing import Tuple
from dotenv import load_dotenv
from pandas import DataFrame as DF
from datetime import date, datetime

# Adding the repository root to the sys.path.
repo = git.Repo(".", search_parent_directories=True)
ROOT_DIR = repo.working_tree_dir
sys.path.append(ROOT_DIR)
sys.path.append(ROOT_DIR + "/library/")


from library.connection_utils import connect_to_aws_service
from library.file_utils import ensure_file_slash, make_dir_if_not_exists
from library.s3_utils import (
    check_if_folder_exists_in_s3_bucket,
    move_local_file_to_s3,
    pull_file_from_s3,
    create_directory_in_s3,
)
from library.user_input_utils import (
    ensure_file_exists,
    enter_for_default,
    ensure_not_blank,
    yes_true_else_false,
)

load_dotenv()

AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_NUM")
AWS_ROLE_NAME = os.environ.get("AWS_ROLE_NAME")
S3_BUCKET = os.environ.get("DEFAULT_S3_BUCKET")
DEFAULT_CSV_LOCATION = os.environ.get("DEFAULT_CSV_LOCATION")
DEFAULT_CSV_LOCATION = ROOT_DIR + "/" + DEFAULT_CSV_LOCATION
ALLOWED_DATA_TYPES = ["boolean", "date", "datetime", "float", "int", "varchar"]
TYPE_MAP = {
    "boolean": bool,
    "date": date,
    "datetime": datetime,
    "float": float,
    "int": "Int64",
    "varchar": str,
}


def create_config_json_from_df(
    config_df: DF,
    create_file: bool = False,
    directory: str = None,
    out_filename: str = "config.json",
):
    """Creates the config.json file from a dataframe"""

    try:
        # If the DF has not yet had the 'field' column set as index, set it
        config_df.columns = ["field", "type", "null", "primary"]
        config_df.set_index(list(config_df)[0], inplace=True)
    except:
        # If the 'field' column is already the index.
        config_df.columns = ["type", "null", "primary"]
        config_df.index.name = "field"

    # Replace FALSE with NaN for 'primary' column
    config_df["primary"].replace(False, np.nan, inplace=True)
    # Split out the schema portion of the CSV and create a JSON blob
    schema_json = json.loads(config_df.iloc[:, 0:2].to_json(orient="index"))
    # Split out the primary fields into a list
    primary_list = config_df.iloc[:, [2]].dropna().index.to_list()

    # Combine schema JSON and primary list into a single JSON blob
    config_dict = {}
    config_dict["columns"] = schema_json
    config_dict["primaryKeys"] = primary_list
    config_json = json.dumps(config_dict)

    # If we want to create a file containing the JSON
    if create_file:
        make_dir_if_not_exists(directory)
        filepath = ensure_file_slash(directory) + out_filename
        with open(filepath, "w") as outfile:
            outfile.write(config_json)
        log.info(f"Created '{out_filename}' in '{filepath}'")

    return config_json


def create_relational_config_from_json(
    config_json: JSON,
    create_file: bool = False,
    out_directory: str = None,
    out_filename: str = "config.csv",
) -> DF:
    "Takes config.json as an input and creates a dataframe"
    # If it's a string, make it a JSON object
    if type(config_json) is str:
        config_json = json.loads(config_json)

    # Config JSON to DF
    column_json = config_json["columns"]
    config_df = DF.from_dict(column_json, orient="index")

    # Primary list to DF
    primary_list = config_json["primaryKeys"]
    primary_df = DF(primary_list)
    primary_df.columns = ["field"]
    primary_df["primary"] = True
    primary_df.set_index("field", inplace=True)

    # Combine Config JSON and Primary List into DF
    joined_df = config_df.join(primary_df, how="left")
    joined_df = format_config_df(joined_df)

    # If they want to create a file, write a file
    if create_file:
        make_dir_if_not_exists(out_directory)
        filepath = ensure_file_slash(out_directory) + out_filename
        joined_df.to_csv(filepath, header=True, index=True, index_label="field")

    return joined_df


def compare_config_to_data_cols(config_df: DF, data_as_df: DF) -> Tuple:
    """Compares the 'fields' column in config to the column headers in the data."""
    config_cols = DF(config_df.index)
    data_cols = DF(data_as_df.columns)

    config_cols.columns = ["field"]
    log.info(f"Fields retreived from config")
    data_cols.columns = ["field"]
    log.info(f"Fields retreived from {data_filename}")

    # Merge DFs so we know which side has extra columns
    merged = config_cols.merge(data_cols, on="field", how="outer", indicator=True)

    config_only_cols = merged[merged["_merge"] == "left_only"]["field"].to_list()
    new_data_only_cols = merged[merged["_merge"] == "right_only"]["field"].to_list()

    return config_only_cols, new_data_only_cols


def ensure_valid_datatype(field: str):
    while True:
        dtype = input(f"\t\t What is the datatype for '{field}'?: ").lower()
        if dtype in ALLOWED_DATA_TYPES:
            return dtype
        else:
            print(f"\t\t Please enter one of: {ALLOWED_DATA_TYPES}")


def force_boolean_series(col: pd.Series):
    """Read in a series and change from string to boolean"""
    col = col.astype(str).str.lower()
    col.replace(["true", "false", "nan"], [True, False, np.nan], inplace=True)

    return col


def format_config_df(config_df: DF) -> DF:
    """Function to format the config_df regardless of where it comes from"""
    try:
        # If the DF has not yet had the 'field' column set as index, set it
        config_df.columns = ["field", "datatype", "accepts_nulls", "part_of_primary"]
        config_df.set_index(list(config_df)[0], inplace=True)
    except:
        # If the 'field' column is already the index.
        config_df.columns = ["datatype", "accepts_nulls", "part_of_primary"]
        config_df.index.name = "field"

    config_df["accepts_nulls"] = force_boolean_series(config_df["accepts_nulls"])
    config_df["part_of_primary"] = force_boolean_series(config_df["part_of_primary"])
    config_df["part_of_primary"].replace(np.nan, False, inplace=True)

    return config_df


def choose_config_from_local(config_csv: str = None):
    # Added to allow for faster testing
    if config_csv:
        config_filepath = DEFAULT_CSV_LOCATION + config_csv
    else:
        config_filename, config_directory, config_filepath = ensure_file_exists(
            f"What is the config csv you would like to use?",
            "Where is the config csv located?",
            DEFAULT_CSV_LOCATION,
        )

    config_df = pd.read_csv(config_filepath, index_col="field")
    config_df = format_config_df(config_df)

    return config_df


def choose_config_csv_and_compare_to_data_cols(data_as_df: DF):
    while True:

        config_df = choose_config_from_local()

        config_only_cols, data_only_cols = compare_config_to_data_cols(
            config_df, data_as_df
        )

        # If either list is not empty, the config fields don't match the data fields
        if config_only_cols or data_only_cols:
            log.warning(
                f"""The config you are attempting to use and your data do not match.
            Fields in config not in data upload: {config_only_cols}
            Fields in data upload not in config: {data_only_cols}
                """
            )
        else:
            log.info(f"Fields from config and {data_filename} match")
            return config_df


def find_invalid_config_rows(config_df: DF):
    """Validates the configuration files and returns a DF of the failures if its not valid"""

    config_schema = DataFrameSchema(
        index=Index(str),
        columns={
            "datatype": Column(str, Check.isin(ALLOWED_DATA_TYPES)),
            "accepts_nulls": Column(
                bool, Check(lambda x: type(x) == bool, element_wise=True)
            ),
            "part_of_primary": Column(
                bool, Check(lambda x: type(x) == bool, element_wise=True)
            ),
        },
        strict=True,
    )

    try:
        config_schema.validate(config_df, lazy=True)
        # Return empty datafreame if config is valid
        log.info("Config is valid")
        return DF()
    except SchemaErrors as e:
        log.error("Configuration file appears to be invalid")
        failure_df = e.failure_cases

        # If there's an issue with the fieldname (i.e. Index) throw an exception, casue they need to fix in ther file
        if "Index" in failure_df["schema_context"].values:
            raise AssertionError(
                "Missing field name. Please fix in your config file and try again."
            )
        # Remove index = "None" because that doesn't give useful info to correct mistakes
        failure_df = failure_df[~failure_df["index"].isna()].sort_values(
            ["index", "column"], ascending=True
        )

        return failure_df


def fix_invalid_config_rows(config_df: DF, failure_df: DF):
    field_list = list(config_df.index)

    def informational_message(message, field, failure_value):
        """Format informational message for user"""
        print(f"\t {field} = '{failure_value}' - ({message})")

    def fix_cell(field, column, failure_value):
        if column == "datatype":
            informational_message(
                f"Valid datatypes = {ALLOWED_DATA_TYPES}", field, failure_value
            )
            fixed_cell_value = ensure_valid_datatype(field)
        elif column == "accepts_nulls":
            informational_message(
                "Must be boolean value [True/False]", field, failure_value
            )
            fixed_cell_value = yes_true_else_false(f"\t\t Can '{field}' be null?")
        elif column == "part_of_primary":
            informational_message(
                "Must be boolean value [True/False]", field, failure_value
            )
            fixed_cell_value = yes_true_else_false(
                f"\t\t Does '{field}' make up part of the primary key?"
            )

        config_copy.at[field, column] = fixed_cell_value

    def select_field():
        while True:
            field = input("What field needs fixing? ")
            if field in field_list:
                return field
            else:
                print(
                    f"That's not in the field list. Please enter one of {field_list}."
                )

    def select_column():
        column_list = [
            "datatype",
            "accepts_nulls",
            "part_of_primary",
        ]
        while True:
            column = input("What column needs fixing? ")
            if column in column_list:
                return column
            else:
                print(f"That's not a column. Please enter one of: {column_list}.")

    config_copy = config_df

    prev_field = ""
    # Loop through all the failures and ask to fix
    for i in range(0, len(failure_df)):
        row = failure_df.iloc[i]
        field, column, failure_value = row["index"], row["column"], row["failure_case"]

        # Only log the error once per field
        if field != prev_field:
            log.error(f"Error(s) in field: '{field}'")

        fix_cell(field, column, failure_value)

        prev_field = field

    # Check with the user if the configuration looks correct and give them an opportunity to fix individual cells.
    while True:
        print(f"Updated Config:\n{config_copy}")

        if yes_true_else_false("Does this look correct?"):
            break
        else:
            field = select_field()
            column = select_column()

            fix_cell(field, column, config_copy.at[field, column])

    return config_copy


def find_invalid_config_rows_and_fix(
    config_df: DF, table: str, config_directory: str = DEFAULT_CSV_LOCATION
):
    """Finds bad config rows and gives the user a chance to fix them"""
    invalid_config_rows = find_invalid_config_rows(config_df)

    if not invalid_config_rows.empty:
        fixed_config = fix_invalid_config_rows(config_df, invalid_config_rows)
        new_config_path = (
            ensure_file_slash(config_directory) + f"config_{table}_UPDATE.csv"
        )

        config_df.to_csv(new_config_path)
        log.info(f"Updated config saved to {new_config_path}")
        return fixed_config
    else:
        return config_df


def generate_pandera_schema_for_data(config_df: DF) -> DataFrameSchema:
    """Programatically generates the schema to be used by Pandera to validate the data"""
    data_schema = DataFrameSchema(strict=True, unique_column_names=True)

    new_cols = {}
    for index, row in config_df.iterrows():
        field = index
        datatype = row["datatype"]
        mapped_type = TYPE_MAP[datatype]
        nullable = row["accepts_nulls"]
        if datatype in ("int", "date", "datetime"):
            coerce = True
        else:
            coerce = False

        new_cols[field] = Column(
            name=field, dtype=mapped_type, nullable=nullable, coerce=coerce
        )

    data_schema = data_schema.add_columns(new_cols)

    return data_schema


def validate_data_dtypes(
    data_df: DF,
    data_schema: DataFrameSchema,
    table,
    data_directory=DEFAULT_CSV_LOCATION,
):
    """Validates the dataframe datatypes vs those defined in the config"""
    try:
        data_schema.validate(data_df, lazy=True)
        # Return empty datafreame if config is valid
        log.info("Datatypes align with config")
        return DF()
    except SchemaErrors as e:
        failure_df = e.failure_cases
        if len(failure_df.index) > 5:
            log.error(f"Failures [First 5 rows]:\n{failure_df.head()}")
        else:
            log.error(f"Failures:\n{failure_df}")

        # Save errors to a file so the user can debug
        directory = ensure_file_slash(data_directory) + ensure_file_slash(table)
        make_dir_if_not_exists(directory)
        filepath = directory + "data_validation_errors.csv"
        failure_df.to_csv(filepath)
        log.error(
            """
------------------------------------------------------------------------
Datatypes of file might not align with datatypes defined in config.
Please check error output for problematic columns/values.
NOTE: Data files still uploaded as this check is not 100% accurate.
------------------------------------------------------------------------"""
        )
        log.info(f"Validation errors saved to {filepath}")

        return failure_df


def create_data_schema_and_validate_data_dtypes(
    data_df: DF, config_df: DF, table, data_directory
) -> DF:
    """Programatically generate the Pandera Schema and then check data vs the schema."""
    data_schema = generate_pandera_schema_for_data(config_df)
    failure_df = validate_data_dtypes(data_df, data_schema, table, data_directory)

    return failure_df


if __name__ == "__main__":
    # Initiate logging
    from library.log_config import get_logger

    log = get_logger(__name__)
    config_update = False

    staging_s3 = yes_true_else_false(
        "Do you want to load the default AWS profile to load a CSV that will populate an MSP table?"
    )
    if staging_s3:
        s3_connection = connect_to_aws_service(AWS_ACCOUNT_ID, AWS_ROLE_NAME)
    else:
        aws_account_id = ensure_not_blank("Enter the AWS Account ID: ")
        aws_role_name = ensure_not_blank("Enter the AWS Role Name: ")
        s3_connection = connect_to_aws_service(aws_account_id, aws_role_name)

    # Ensure file exists
    data_filename, data_directory, data_filepath = ensure_file_exists(
        f"What is the name of the csv of data you would like to load to the database?",
        "Where is the file located?",
        DEFAULT_CSV_LOCATION,
    )
    data_as_df = pd.read_csv(data_filepath)

    # Add temp folder to store config.json
    temp_folder = ensure_file_slash(data_directory + "temp")
    make_dir_if_not_exists(temp_folder)

    # Append date to end of file for name in the S3
    date_appendix = dt.now().strftime("_%Y%m%d.csv")
    new_filename = data_filename[:-4] + date_appendix

    # Loop until the user determines the desired table to import to, and the import succeeds.
    while True:
        # Default table name = same as filename (without .csv)
        if data_filename[-4:] == ".csv":
            table_guess = data_filename[:-4]
            table = enter_for_default("What is the table name?", table_guess)
        else:
            table = enter_for_default("What is the table name?", data_filename)

        # Check if directory exists in s3
        if check_if_folder_exists_in_s3_bucket(s3_connection, S3_BUCKET, table):
            intended = yes_true_else_false(
                f"The table `{table}` already exists. Was this expected?",
            )

            # The table exists AND they knew it existed
            if intended:
                # Loop until config.json matches and is loaded to S3
                while True:
                    try:
                        # Pull config.json from S3
                        config_exists = pull_file_from_s3(
                            s3_connection, S3_BUCKET, "config.json", table, temp_folder
                        )
                        # If config.json doesn't exist in the directory for some reason, raise error
                        if not config_exists:
                            raise AssertionError(
                                f"config.json does not exist for '{table}'"
                            )

                        # Expand config.json into DF
                        with open(temp_folder + "config.json", "r") as json_file:
                            config_json = json.load(json_file)

                        # Convert JSON to DF for easier comparison
                        config_df = create_relational_config_from_json(
                            config_json, True, temp_folder
                        )

                        # Show the current configuration so the user knows if it needs to be updated
                        log.info(
                            f"The current configuration pulled from S3:\n{config_df}"
                        )
                        # Does the user want to update the config?
                        update_config = yes_true_else_false(
                            "Do you have an updated config file?"
                        )

                        if update_config:
                            # Read in config.csv as config_df and compare to data
                            config_df = choose_config_csv_and_compare_to_data_cols(
                                data_as_df
                            )
                            break
                        else:
                            # Compare config fields to data fields
                            (
                                config_only_cols,
                                data_only_cols,
                            ) = compare_config_to_data_cols(config_df, data_as_df)

                            # The config and existing data definitions did not line up one or more lists won't be empty
                            if config_only_cols or data_only_cols:
                                # Fields exist in config but not in data
                                print("\nThe table config needs to be updated!")

                                while True:
                                    edit_replace = input(
                                        "Do you want to (R)eplace the config by uploading a new one, or (E)dit the existing config in-place? "
                                    )

                                    # They want to replace the existing config.json entirely
                                    if edit_replace.lower() in ("replace", "r"):
                                        edit_replace = "replace"
                                        break
                                    # They want to edit the existing config.json.
                                    elif edit_replace.lower() in ("edit", "e"):
                                        edit_replace = "edit"
                                        break

                                if edit_replace == "edit":
                                    # Fields exist in config but not data
                                    if config_only_cols:
                                        log.info(
                                            f"Fields in existing config not in current data upload: {config_only_cols}"
                                        )
                                        # Can only remove fields, or leave them.
                                        for field in config_only_cols:
                                            remove = yes_true_else_false(
                                                f"Do you want to remove '{field}' from the config?"
                                            )
                                            if remove:
                                                config_df.drop(
                                                    axis=0, index=field, inplace=True
                                                )

                                    # Fields exist in data but not in config
                                    if data_only_cols:
                                        log.info(
                                            f"Fields in data upload not in existing config: {data_only_cols}"
                                        )
                                        # Define the config for the new fields
                                        for field in data_only_cols:
                                            print(
                                                f"Enter the details for the field {field}: "
                                            )
                                            # Ensure it's an allowed datatype
                                            while True:
                                                datatype = input("Datatype: ").lower()
                                                if datatype in ALLOWED_DATA_TYPES:
                                                    break
                                                else:
                                                    log.warning(
                                                        f"The only allowed datatypes are: {ALLOWED_DATA_TYPES}"
                                                    )
                                            nulls_allowed = yes_true_else_false(
                                                "Are nulls allowed?: "
                                            )
                                            is_primary = yes_true_else_false(
                                                "Is this part of the primary key?: "
                                            )
                                            new_row = {
                                                "type": datatype,
                                                "null": nulls_allowed,
                                                "primary": is_primary,
                                            }
                                            # Add row to the existing config DF
                                            config_df.loc[field] = new_row

                                    # List primary fields
                                    primary_df = config_df[config_df["primary"] == True]
                                    print(
                                        f"The current primary field(s) are:\n{primary_df} "
                                    )
                                    # Does the user want to change the primary fields?
                                    change_primaries = yes_true_else_false(
                                        "Do you want to make changes to the primary field(s)?"
                                    )
                                    if change_primaries:
                                        print(
                                            "Please type the desired primary fields one at a time.\nUse Ctrl+C to exit when you're done. "
                                        )
                                        primary_options = config_df.index.tolist()
                                        print(
                                            f"Here are your options for primary fields:\n{primary_options}"
                                        )
                                        new_primaries = []
                                        # Continue looping until keyboard interrupt
                                        while True:
                                            try:
                                                new_prim = input("Primary field: ")
                                                if new_prim in primary_options:
                                                    new_primaries.append(new_prim)
                                                else:
                                                    log.warning(
                                                        f"{new_prim} is not in the fields list"
                                                    )
                                            except KeyboardInterrupt:
                                                break

                                        config_df.drop("primary", axis=1)
                                        config_df["primary"] = False
                                        config_df.loc[
                                            config_df.index.isin(new_primaries),
                                            "primary",
                                        ] = True
                                        print(
                                            f"The now-current primary field(s) are:\n{primary_df} "
                                        )
                                # Replace the existing table with the new data, regardless of the columns.
                                elif edit_replace == "replace":
                                    config_df = (
                                        choose_config_csv_and_compare_to_data_cols(
                                            data_as_df
                                        )
                                    )
                                break
                            else:
                                log.info(
                                    f"Fields from config.json and {data_filename} match"
                                )
                                break
                    except AssertionError as e:
                        # There was no config.json in the S3 directory
                        log.error(
                            f"There was no config.json in '{table}' in '{S3_BUCKET}'"
                        )
                        have_config = yes_true_else_false(
                            "Do you have a config to upload?"
                        )
                        # They have a config.csv that they want to upload
                        if have_config:
                            config_df = choose_config_csv_and_compare_to_data_cols(
                                data_as_df
                            )
                            break
                        else:
                            raise FileNotFoundError(
                                "Please create a config.csv file that matches your data schema and then try again."
                            )
                    except Exception as e:
                        print(f"{type(e)}: {e}")
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

            # They intended to create a new table, create new table.
            if intended:
                # Create directory in S3 (needs to be done due to flat file structure)
                create_directory_in_s3(s3_connection, S3_BUCKET, table)

                # Read in config.csv as config_df
                config_df = choose_config_csv_and_compare_to_data_cols(data_as_df)
                break

    # Do validation on the config and give them an opportunity to fix schema
    config_df = find_invalid_config_rows_and_fix(config_df, table, data_directory)

    # Check the datatypes in the config file vs. what exists in the table data.
    create_data_schema_and_validate_data_dtypes(
        data_as_df, config_df, table, data_directory
    )

    # Create new config.json from config_df
    config_json = create_config_json_from_df(config_df, True, temp_folder)

    # Load config.json to S3
    move_local_file_to_s3(
        s3_connection,
        "config.json",
        temp_folder,
        S3_BUCKET,
        s3_path=table,
    )

    # Load table data to S3
    move_local_file_to_s3(
        s3_connection,
        data_filename,
        data_directory,
        S3_BUCKET,
        new_filename,
        table,
    )
    # Delete temp_folder
    shutil.rmtree(temp_folder)
