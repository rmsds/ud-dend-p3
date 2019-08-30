This repository contains my submission for the Cloud Data Wharehouses Project (section 3) of the Udacity Data Engineering Nano Degree.

# Summary
In this project a set of logs and song information are loaded into staging tables and are then transformed and loaded into five final tables to allow for easier analytics.


# Preqrequisites to run this code
* spin-up a Amazon Redshift cluster
  * with an associated IAM role with _S3ReadAccess_
* a `dwh.cfg` file with the information generated in the previous step (see `dwh.cfg.orig` for a template)
* Python 3 with the dependencies listed in `requirements.txt` installed


# Running the code
## (Re)Create the tables
```
python create_tables.py
```

## Extract data into staging tables, Transform and Load it into the final tables
```
python etl.py
``` 
**Warning**: this script could take a long time to load the initial data into the staging tables. This depends on several factors, but 50 minutes to 1h30 was observed for 8 and 4-node cluster of type *dc2.large*.


## An example (using a Python virtual environment)
```
pyvenv-3.5 venv
$ pip install -r requirements.txt
$ python create_tables.py
$ python etl.py
``` 


# Infrastructure As Code (extra)
To spin up the infrastructure needed to complete this project one can use the `iac.py` script which, when provided with a completed `iac.cfg` file (see `iac.cfg.orig` for an example) will create the required infrastructure and generate the `dwh.cfg` file. 
