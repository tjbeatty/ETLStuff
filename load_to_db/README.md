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

### Step One:
Ensure you have an entry in your LastPass vault with the details to create a connection MSP Staging. Name this entry `MSP_STAGING` 

If you don't already have this entry, follow these steps:

1. Open your LastPass vault in your browser
2. Click the (+) symbol in the bottom, right hand corner. 
3. Click "MORE ITEMS" to get more options of LastPass entry types
4. Click "DATABASE"
5. Enter "MSP_STAGING" in the `Name` field and fill in the rest of the fields with the connection details. 
6. Click "Save" 


### Step Two: 
In the root of the repo, copy the `.env_example` file as `.env`.
```bash
cp .env_example .env
```

Fill out the `.env` file with your information. 


### Step Three 
Open a clean Python virtual enironment in the root of the repo:

```bash
python3.8 -m venv .venv
source .venv/bin/activate
```

### Step Four
Install the requirements to the Python environment from `requirements.txt`
```bash
pip install -r requirements.txt
```

### Step Five
Make sure you are connected to VPN

### Step Six
Move the CSV you would like to loaded into the Db to `./csvs_to_load/`  
Ensure what you want to be the column names in the table are in the first row of the CSV. 

### Step Seven
To load data from a csv to the MSP Staging Db, run the program from `load_to_db.py`, and follow the instructions. In the root: 
```bash
python3.8 load_to_db.py
```