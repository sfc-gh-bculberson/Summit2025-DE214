# Build a Streaming Data Pipeline in Python

In this hands-on lab, you'll build a production-ready streaming data pipeline using Python and Snowflake's new Rowset API. Learn how to efficiently ingest real-time data streams into Snowflake, create automated aggregations, and visualize insights through an interactive Streamlit application. You'll walk away with practical experience in modern data engineering techniques and a functional end-to-end pipeline that can handle continuous data flows at scale.

Docs for Snowpipie Streaming Rowset API Private Preview: https://docs.snowflake.com/en/LIMITEDACCESS/snowpipe-streaming-rowset-api/rowset-api-intro

## Database Setup

Create a database, schema, warehouse, role, and user called INGEST in your Snowflake account.

```sql
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS STREAMING_INGEST;
CREATE ROLE IF NOT EXISTS STREAMING_INGEST;
GRANT USAGE ON WAREHOUSE STREAMING_INGEST TO ROLE STREAMING_INGEST;
GRANT OPERATE ON WAREHOUSE STREAMING_INGEST TO ROLE STREAMING_INGEST;
CREATE DATABASE IF NOT EXISTS STREAMING_INGEST;
USE DATABASE STREAMING_INGEST;
CREATE SCHEMA IF NOT EXISTS STREAMING_INGEST;
USE SCHEMA STREAMING_INGEST;
GRANT OWNERSHIP ON DATABASE STREAMING_INGEST TO ROLE STREAMING_INGEST;
GRANT OWNERSHIP ON SCHEMA STREAMING_INGEST.STREAMING_INGEST TO ROLE STREAMING_INGEST;

CREATE USER STREAMING_INGEST LOGIN_NAME='STREAMING_INGEST' DEFAULT_WAREHOUSE='STREAMING_INGEST', DEFAULT_NAMESPACE='STREAMING_INGEST.STREAMING_INGEST', DEFAULT_ROLE='STREAMING_INGEST', TYPE=SERVICE;
GRANT ROLE STREAMING_INGEST TO USER STREAMING_INGEST;
GRANT ROLE STREAMING_INGEST TO USER <YOUR_USERNAME>;

```

To generate a key pair for the INGEST user, run the following in your shell:

```sh
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
PUBK=`cat ./rsa_key.pub | grep -v KEY- | tr -d '\012'`
echo "ALTER USER STREAMING_INGEST SET RSA_PUBLIC_KEY='$PUBK';"
```

Run the sql from the output to set the RSA_PUBLIC_KEY for the INGEST user.

```sql
USE ROLE ACCOUNTADMIN;
ALTER USER STREAMING_INGEST SET RSA_PUBLIC_KEY=...;
```

Create the table and stream to land data from the API.

```sql
USE ROLE STREAMING_INGEST;
USE DATABASE STREAMING_INGEST;
USE SCHEMA STREAMING_INGEST;

CREATE OR REPLACE TABLE LIFT_TICKETS(TXID varchar(255), RFID varchar(255), RESORT varchar(255), PURCHASE_TIME datetime, EXPIRATION_TIME date, DAYS number, NAME varchar(255), ADDRESS variant, PHONE varchar(255), EMAIL varchar(255), EMERGENCY_CONTACT variant);

CREATE OR REPLACE PIPE LIFT_TICKETS_PIPE AS
COPY INTO LIFT_TICKETS
FROM TABLE (
      DATA_SOURCE (
      TYPE => 'STREAMING_ROWSET'
  )
)
MATCH_BY_COLUMN_NAME=CASE_SENSITIVE;

```

To get the private key for this user run the following in your shell:

```sh
PRVK=`awk '$1=$1' ORS='\\n' ./rsa_key.p8 `
echo "PRIVATE_KEY=$PRVK"
```

Copy the .env.example to .env and modify the variables in your project

```
SNOWFLAKE_ACCOUNT=<ACCOUNT_HERE>
SNOWFLAKE_USER=STREAMING_INGEST
PRIVATE_KEY=<PRIVATE_KEY_HERE>
```

## Building the generator

```sh
docker build -t generator .
```

## Starting the generator

```sh
docker run generator
```