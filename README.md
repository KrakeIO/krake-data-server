# Krake's Data Server

The system that interfaces the exporting of harvested data from Krake's web scraping engine
used in [Krake's Web Scraping Engine] (https://krake.io)

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
    $select : [{ 
        $int: col_name1 
      },{ 
        $string: col_name2
      },{ 
        $float: col_name3 
    }],
    $where : { 
      col_name1 : value 
    },
    $order : [{
      $asc : col_name1
    }],
    $limit : 10,
    $skip : 20
  }
```


### $select clause

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
#### operator =
Select records where **col_name** = **value**
```json
{ col_name : value }

```

#### operator $contains
Select records where **col_name** contains **value**
```json
{ col_name : { 
    $contains : value
  } 
}
```

#### operator $gt
Select records where **col_name** is greater than **value**
```json
{ col_name : { 
    $gt : value
  }
}
```

#### operator $gte
Select records where **col_name** is greater than or equal to **value**
```json
{ col_name : { 
    $gte : value
  }
}
```

#### operator $lt
Select records where **col_name** is lesser than **value**
```json
{ col_name : { 
    $lt : value
  }
}
```

#### operator $lte
Select records where **col_name** is lesser than or equal to **value**
```json
{ col_name : { 
    $lte : value
  }
}
```

#### operator $ne
Select records where **col_name** is not equal to **value**
```json
{ col_name : { 
    $ne : value
  }
}
```

#### operator between
Select records where **col_name** is between greater than **value1** and lesser than **value2** 
```json
{ col_name : { 
    $gt : value1,
    $lt : value2
  }
}
```

#### operator $or
Select records where either of the expressions are true
```json
{ $or : [ { <expression1> }, { <expression2> }, ... , { <expressionN> } ] }
```

#### operator $and
Select records where all of the expressions are true
```json
{ $and: [ { col_name: { $ne: 1.99 } }, { col_name: { $exists: true } } ] }
{ col_name: { $ne: 1.99, $exists: true } }
```

#### operator $exist
Ensures records returned do not have col_name value that is NULL
```json
{ col_name: { $exist: true } }
```
