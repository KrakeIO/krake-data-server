# Krake's Data Export API 

Provides RESTFUL API for Postgresql HSTORE database used in [Krake's Data Harvesting Engine] (https://krake.io)

## Goals
- This API is not an attempting at describing fully the entire SQL grammar
- This API should support only subsets of the SQL grammar that could be utilized without much impact of database performance in a fully sharded environment 
- OPERATORS like GROUP BY and ORDER BY has thus been deliberately left out of this API's grammar

## Thoughts and guiding principals
- <code>JOINS</code> across tables are evil
- Databases slow down drastically when computing JOIN operations 
- SORTING and JOINING to be done at the application layer
- This approach allows for easy Database replication and sharding moving forward

## Pre-requisities
- NodeJS — v0.8.8
- Coffee-Script
- PostGresql 9.2.2

## Setup

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
export KRAKE_PG_USERNAME='your_username'
export KRAKE_PG_PASSWORD='your_password'
export KRAKE_PG_HOST='your_host_location'
```

## Run server
```console
coffee krake_data_server.coffee
```

# Query Operators
HTTP GET Request to query Krake data server for data. 
Important: Operators made available are restricted to ones that are indempotent of nature

```console
http://krake-data-server/:table_name/search/:format?q=query_object
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
  - $skip : the number of records to discard before returning the first set of records

```json
http://krake-data-server/:table_name/search/:format?q={ 
    $select : [...],
    $where : [...],
    $order : [...],
    $limit : 10,
    $skip : 20
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
$select : [{ $count: true }]
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