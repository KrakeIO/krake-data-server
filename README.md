# Krake's Data Export API 

## Overview
Provides RESTFUL API for Postgresql HSTORE database used in [Krake's Data Harvesting Engine] (https://krake.io)

#### Server maintenance

Procedures after restart
```
# Check disk
lsblk

# Mounting disks to locations
sudo mount /dev/xvdg /krake_data_cache_2
sudo mount /dev/xvdf /krake_export_dump
sudo ln -s /krake_export_dump/krake_export_dump_2/ /tmp/krake_data_cache

# Granting permissions to ubuntu user
sudo chgrp -R postgres /krake_export_dump
sudo chown -R postgres /krake_export_dump
sudo usermod -a -G postgres ubuntu
sudo chmod -R 770 /krake_export_dump
```

Procedures to clear export cache
```
mkdir /krake_export_dump/krake_export_dump_XXX
sudo chgrp -R postgres /krake_export_dump/krake_export_dump_XXX
sudo chown -R postgres /krake_export_dump/krake_export_dump_XXX
sudo ln -s /krake_export_dump/krake_export_dump_XXX/ /tmp/krake_data_cache_XXX
sudo mv /tmp/krake_data_cache_XXX /tmp/krake_data_cache
sudo rm /tmp/krake_data_cache_org
```

# Query Operators
HTTP GET Request to query Krake data server for data. 
Important: Operators made available are restricted to ones that are indempotent of nature

```console
http://krake-data-server/:table_name/:format?q=query_object
```

- :format
    - html
    - json
    - csv

## QueryObject
A JSON object that contains the following clauses 
  - $select : the columns to return in each row of record
  - $where : the conditions to be used for selecting the records
  - $order : the order in which to order the records returned
  - $limit : the total number of records to return
  - $offset : the number of records to discard before returning the first set of records
  - $refresh : a boolean that tells the Krake server to clear the cache and fetch fresh data-set

```json
http://krake-data-server/:table_name/:format?q={ 
    $select : [...],
    $where : [...],
    $order : [...],
    $limit : 10,
    $offset : 20
  }
```


### $select clause
```json
$select : [col_name1, col_name2, col_name3,..., col_nameN]
```

#### operator simple
Returns all the values in col_name
```json
$select : [col_name]
```

#### operator $count
Returns a count of the rows
```json
$select : [{ $count: col_name }]
```

#### operator $distinct
Returns distinct values from col_name
```json
$select : [{ $distinct: col_name }]
```
#### operator $max
Returns the max value in col_name
```json
$select : [{ $max: col_name }]
```

#### operator $min
Returns the max value in col_name
```json
$select : [{ $max: col_name }]
```


### $where clause
```json
$where : [{condition1}, {condition2}, {condition3}... {conditionN}]
```

#### operator =
Select records where **col_name** = **value**
```json
$where : [{ col_name : value }]

```

#### operator $contains
Select records where **col_name** contains **value**
```json
$where : [{ 
  col_name : { 
    $contains : value
  } 
}]
```

#### operator $gt
Select records where **col_name** is greater than **value**
```json
$where : [{ 
  col_name : { 
    $gt : value
  }
}]
```

#### operator $gte
Select records where **col_name** is greater than or equal to **value**
```json
$where : [{ 
  col_name : { 
    $gte : value
  }
}]
```

#### operator $lt
Select records where **col_name** is lesser than **value**
```json
$where : [{ 
  col_name : { 
    $lt : value
  }
}]
```

#### operator $lte
Select records where **col_name** is lesser than or equal to **value**
```json
$where : [{ 
  col_name : { 
    $lte : value
  }
}]
```

#### operator $ne
Select records where **col_name** is not equal to **value**
```json
$where : [{ 
  col_name : { 
    $ne : value
  }
}]
```

#### operator $exist
Ensures records returned do not have col_name value that is NULL
```json
$where : [{ 
  col_name: { $exist: true } 
}]
```

### Compound operators
#### operator $or
Select records where either of the expressions are true
```json
$where : [{ 
  $or : [ 
    { <expression1> }, 
    { <expression2> }, ... , 
    { <expressionN> } 
  ] 
}]
```

#### operator $and
Select records where col1 != 1.99 and col2 not Null
```json
$where : [{ 
  $and: [ 
    { col1: { $ne: 1.99 } }, 
    { col2: { $exists: true } } 
  ] 
}]

```

### Multi-nested compound operators mixture 
#### operator $and and $or
Select records where col1 = "value for col1" and (col2 = "value for col2" or col3 = "value for col3") 
```json
$where : [{ 
  $and: [ 
    { col1: "value for col1" },
    {  
      $or: [
        { col2: "value for col2" },
        { col3: "value for col3" }
      ]
    }
  ] 
}]

```

### $order clause
```json
$order : [{condition1}, {condition2}, {condition3}... {conditionN}]
```

#### operator $asc
Select records in ascending order by col_name1
```json
$order : [{ 
  $asc: "col_name1"
}]

```

#### operator $desc
Select records in ascending order by col_name1
```json
$order : [{ 
  $desc: "col_name1"
}]

```

### $limit clause
Select the first 10 records that match the condition
```json
$limit : 10
```

#### $offset clause
Skip the first 10 records and select the rest
```json
$offset : 10
```

# Setup

### Pre-requisities
- NodeJS — v0.8.8
- Coffee-Script
- PostGresql 9.2.2 - HSTORE

### Installation
```console
git clone git@github.com:KrakeIO/krake-toolkit.git
cd krake-toolkit
npm install
npm install coffee-script -g
```

### Configuration
Write the following lines in your ~/.bashrc file
```console
# Postgresql Database connection
export KRAKE_PG_USERNAME='your_username'
export KRAKE_PG_PASSWORD='your_password'
export KRAKE_PG_HOST='your_host_location'

# AWS S3 credentials
export AWS_ACCESS_KEY=YOUR_ACCESS_KEY
export AWS_SECRET_KEY=YOUR_SECRET_KEY
export AWS_S3_REGION=YOUR_AWS_REGION
export AWS_S3_BUCKET=YOUR_BUCKET_NAME
```

### Create databases
Testing environment
```console
# Where your scraped data is stored
scraped_data_repo_test

# Where your data definitions will be stored is stored
dev_panel_test
```

Development environment
```console
# Where your scraped data is stored
scraped_data_repo_development

# Where your data definitions will be stored is stored
dev_panel_development
```

Production environment
```console
# Where your scraped data is stored
scraped_data_repo

# Where your data definitions will be stored is stored
dev_panel
```

Make sure to run the following command to install HStore in all the databases that will be storing your scraped data
```console
CREATE EXTENSION hstore;
```

#### tables that exist in <code>DATABASE::dev_panel</code>
There is only one table in this database and its name is <code>krakes</code>
```console
CREATE TABLE krakes (
    id bigint,
    name text,
    content text,
    frequency text,
    handle text,
    last_ran timestamp with time zone,
    status text,
    "createdAt" timestamp with time zone NOT NULL,
    "updatedAt" timestamp with time zone NOT NULL
);
```

#### sample of how a data table in <code>DATABASE::scraped_data_repo<code> looks like
An example of how a dynamically generated scraped data repository table looks like
```console
CREATE TABLE data_repository_table (
    properties hstore,
    "pingedAt" timestamp with time zone,
    id integer NOT NULL,
    "createdAt" timestamp with time zone NOT NULL,
    "updatedAt" timestamp with time zone NOT NULL
);
```

### Create data cache
production environment
```console
mkdir /tmp/krake_data_cache
chgrp postgres krake_data_cache
chown postgres krake_data_cache
chmod 770 krake_data_cache
usermod -a -G postgres NODE_USER
```

Make sure current owner daemon has permissions to read/write from folder
Make sure postgres user has permissions to read/write from folder

### Unit test 
Run the following comming in root location of your project's repository to ensure your setup is working properly
```console
jasmine-node --coffee test
```

## Start server
```console
coffee krake_data_server.coffee
```

# Design thoughts and guiding principals
- This API is not an attempting at describing fully the entire SQL grammar
- This API should support only subsets of the SQL grammar that could be utilized without much impact of database performance in a fully sharded environment 
- OPERATOR like GROUP BY has thus been deliberately left out of this API's grammar
- <code>JOINS</code> across tables are evil
- Databases slow down drastically when computing JOIN operations 
- <code>JOIN</code> to be done at the application layer if at all required
- This approach allows for easy horizontal scaling of Krake's Database Infrastructure moving forward

### Proposed architecture to horizontal Scaling of database
- Scraped data repositories will be allocated to specific Krake Data Servers.
- Record of corresponding location of the Krake Data Server for each scraped data repository will be maintained at the application layer
