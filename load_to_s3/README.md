# MSP-ETL

### Extract, Transform, and Load processes for MSP designated beneficiary database

## Setup

## Python Versioning
The BlueLabs MSP team is standardizing on Python3.8. To ensure you have Python3.8 available on your local, perform the following from your command line:

### Install Homebrew (if you don't already have it) 
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```
Follow the instructions during the install...

### Install Python3.8
```bash
brew install python@3.8
```

### Launch Python3.8 in a Virtual Environment
When you launch a virtual environment to use the tool, make sure you specify Python3.8 with the following:
```bash
python3.8 -m venv .venv
```
And then you can activate the virtualenv with the following:
```
source .venv/bin/activate
```

## How to Load Local Data Into MSP Staging

### Design Decisions
1. In the S3 bucket, the directory will match the name of the table. 
2. The filename of the data CSV does not matter. The table will update upon load, using data from the most recently updated file in the directory. 
3. The directory will contain the data CSVs as well as a config.json file that defines the table schema. 
4. The user will create a configuration CSV instead of JSON, which will be translated to `config.json` when the program is run. 


### Preparing the data
When you are creating a new table, you will need two files - a CSV contained the data, and a CSV containing the table configuration information (i.e. DDL). 

If you are updating a table you will only need the data CSV, unless you are changing the schema (i.e. adding a column, removing a column, etc.)  
If you are loading a CSV with modified schema, you will have the option to load a new configuraiton CSV, or to edit the schema through the command line. 

#### Data CSV
The data CSV must contain the field names in the first row. 

#### Configuration CSV
The configration CSV will contain four columns - `field`, `datatype`, `null`, and `primary`.  
**Field:** Field name, as seen in the table.   
**Data Type:** High-level data type of the field. Currently accepted: "varchar", "int", "date", "boolean", "float", "datetime"  
**Null:** Whether the field can be null or not (`True` or `False`)  
**Primary:** If the field makes up part of the primary key (`True`, `False`, or Blank = `False`)  

**Note:** The *ORDER* of the columns matters, not the column names. Order = `field`, `datatype`, `null`, `primary`

*Ex:* 
| field | datatype | null | primary |
|-------|----------|------|---------|
| field1 | varchar | True | True |
| field2 | int | False | |
| field3 | date | True | |

**Note:** A Google Sheet template that performs data validation for the configuration CSV is located [here](https://docs.google.com/spreadsheets/d/1v-4vBjQz--rOz0PWJfogMP5DmjXT4UNRnmibeB3tcGU/edit?usp=sharing). If you choose to use it:
1) Make a copy of the Google Sheet   
*File > Make a copy*
2) Rename the Sheet something relevant to your table (`<table>_config`, perhaps ¯\\\_(ツ)\_/¯)
3) Fill out the Sheet with all the fields
4) Download as CSV  
*File > Download > Comma Separated Values (.csv)*
5) Move the CSV to your working folder 

#### Step One: 
In the root of the repo, copy the `.env_example` file as `.env`.
```bash
cp .env_example .env
```

Fill out the `.env` file with your information. 


#### Step Two 
Open a clean Python virtual enironment in the root of the repo:

```bash
python3.8 -m venv .venv
source .venv/bin/activate
```

#### Step Three
Install the requirements to the Python environment from `./load_to_staging_s3/requirements.txt`
```bash
pip install -r requirements.txt
```

#### Step Four
Make sure you are connected to VPN

#### Step Five:
Update your AWS Access Key, Secret Key, and Session Token in your terminal  

1. Go to [Kion/Cloudtamer](https://cloudtamer.cms.gov/portal) and Log In
2. Click the dropdown with a "Cloud" icon to the right of "MAX Dev"
3. Click "Cloud Access Roles"
4. Click "Max Developer Admin"
5. Click "Short-term Access Keys" to generate new access keys
6. Click the text under "Set AWS environment variables" to copy the keys to your clipboard
7. Paste the environmental variables to the terminal in which you are running the program. 

**Note:** Hopefully some day we have a more streamlined system, but this is necessary as of now. 

#### Step Six
Move the CSV (and config CSV, if relevant) you would like to loaded into S3 to `csvs_to_load/` (or another folder you have defined as your `DEFAULT_CSV_LOCATION` in `.env`)  
Ensure what you want to be the column names in the table are in the first row of the CSV. 

#### Step Seven
To load data from a csv to the S3 bucket, run the program from `load_to_staging_s3.py`, and follow the instructions. In the root: 
```bash
python3 ./load_to_staging_s3/load_to_staging_s3.py
```
