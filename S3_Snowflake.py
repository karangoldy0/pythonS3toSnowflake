import pandas as pd
import snowflake.connector as sf
import json
import boto3
from botocore.exceptions import NoCredentialsError

path_to_json = r"C:\Users\kalya\PycharmProjects\aws\data_file.json"
with open(path_to_json, "r") as handler:
    info = json.load(handler)

con=sf.connect(user=info["user"],password=info["password"],account=info["account"])
con.cursor().execute("CREATE WAREHOUSE IF NOT EXISTS sample1")
con.cursor().execute("CREATE DATABASE IF NOT EXISTS COVID")
con.cursor().execute("USE DATABASE COVID")
con.cursor().execute("CREATE SCHEMA IF NOT EXISTS sampleschema")
con.cursor().execute("USE WAREHOUSE sample1")
con.cursor().execute("USE SCHEMA COVID.sampleschema")
'''--------------------TABLE CREATION------------'''
con.cursor().execute(
    """CREATE OR REPLACE TABLE covid(
id INTEGER,
NAME VARCHAR(225),
confirmed integer,
curse integer,
  death integer
);
""")
'''--------CREATION OF FILE FORMAT-----------'''
con.cursor().execute("""create or replace file format CSV_SKIP_HEADER
  type = 'CSV'
  field_delimiter = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  skip_header = 1;""")
'''---------STAGE CREATION----------------'''
'''con.cursor().execute("""create stage my_s3_stage
  url = 's3://kd2practise/snowflake/'
  file_format = CSV_SKIP_HEADER""")'''
'''copying into snowflake'''
con.cursor().execute("""
COPY INTO "COVID"."SAMPLESCHEMA"."COVID"
 FROM '@"COVID"."SAMPLESCHEMA"."MY_S3_STAGE"/01'
  FILE_FORMAT = '"COVID"."SAMPLESCHEMA"."CSV_SKIP_HEADER"' ON_ERROR = 'CONTINUE' PURGE = FALSE;
""")
'''--------------getting sample result---------'''
cur = con.cursor()
try:
    cur.execute("SELECT id,NAME  FROM covid")
    for (col1, col2) in cur:
        print('{0}, {1}'.format(col1, col2))
finally:
    cur.close()