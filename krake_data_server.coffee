# @Description : this is a data server where users will call to consume the data
#   Sample:
#     http://data.krake.io/1_grouponsgs/json?q={"limit"":10,"offset"":10,"parent_cat_id":"45","status":"Active","createdAt":{"gt":"2013-04-15"}}
async = require 'async'
express = require 'express'
fs = require 'fs'
kson = require 'kson'
path = require 'path'
Hstore = require 'pg-hstore' #https://github.com/scarney81/pg-hstore
ktk = require 'krake-toolkit'
QueryValidator = ktk.query.validator
QueryHelper = ktk.query.helper
Sequelize = require 'sequelize'
CacheManager = require './helper/cache_manager'

CONFIG = null
ENV = (process.env['NODE_ENV'] || 'development').toLowerCase()
try 
  CONFIG = kson.parse(fs.readFileSync(__dirname + '/config/config.js').toString())[ENV];
catch error
  console.log('cannot parse config.js')
  process.exit(1)

options = {}
options.host = process.env['KRAKE_PG_HOST'] || CONFIG.postgres.host
options.port = CONFIG.postgres.port
options.dialect = 'postgres'
options.logging = false
pool = {}
pool.maxConnections = 5
pool.maxIdleTime = 30
options.pool = pool

userName = process.env['KRAKE_PG_USERNAME'] || CONFIG.postgres.username
password = process.env['KRAKE_PG_PASSWORD'] || CONFIG.postgres.password

dbHandler = new Sequelize CONFIG.postgres.database, userName, password, options
db_dev = new Sequelize CONFIG.userDataDB, userName, password, options
krakeSchema = ktk.schema.krake
Krake = db_dev.define 'krakes', krakeSchema

modelBody = {}
modelBody["properties"] = 'hstore'
modelBody["pingedAt"] = 'timestamp'
qv = new QueryValidator()
cm = new CacheManager CONFIG.cachePath, dbHandler, modelBody

# @Description: get krake columns given a unique_handle
# @param: handle:string
# @param: callback:function(is_valid:boolean, err_message:string || schema_array:array)
getKrakeColumns = (handle, callback)=>
  gotKrakes = (krakes)=>
    for x in [0... krakes.length]
      current_krake = krakes[x]
      curr_qh = new QueryHelper(current_krake.content)
      columns = curr_qh.getFilteredColumns() || curr_qh.getColumns() 
      url_columns = curr_qh.getUrlColumns() 
      callback curr_qh.is_valid, columns, url_columns
  
  couldNotGetKrakes = (error_msg)->
    callback false, error_msg

  # Ensures only 1 Krake definition is retrieved given a krake handle
  Krake.findAll({ where : { handle : handle }, limit: 1 }).success(gotKrakes).error(couldNotGetKrakes)
  


# @Descriptions: get the columns part of a postgresql query given a set of columns
# @param: columns:array
# @return: columns_in_query:string
getColumnsQuery = (columnArray)->
  
  # properties::hstore-> ARRAY['price', 'title']
  columns_in_query = ""
  for x in [0...columnArray.length]
    if x < columnArray.length - 1
      columns_in_query += "properties::hstore->'" + columnArray[x].replace(/'/, '\\\'') + "' as \"" + columnArray[x].replace(/""/, '\\\""') + "\" , \r\n"
    else
      columns_in_query += "properties::hstore->'" + columnArray[x].replace(/'/, '\\\'') + "' as \"" + columnArray[x].replace(/""/, '\\\""') + "\" "

  columns_in_query



# Converts raw query input value to actual stuff
queryValue = (raw_input)=>

  switch typeof(raw_input)
    when 'object'
      operators = Object.keys raw_input
      for x in [0... operators.length]
        switch operators[x]
          when 'gt'
            " > '" + raw_input[operators[x]] + "' "
          when 'gte'
            " >= '" + raw_input[operators[x]] + "' "
          when 'lt'
            " < '" + raw_input[operators[x]] + "' "
          when 'lte'          
            " <= '" + raw_input[operators[x]] + "' "
      
    when 'string'
      " = '" + raw_input + "' " 
    when 'number'     
      " = '" + raw_input + "' "     



# @Description: Returns an array of records given a query string and table name
# @param: table_name:string
# @param: columns:array
# @param: query_string:string
# @param: callback:function(err:string, results:array)
getRecords = (table_name, columns, query_string, callback)=>

  model = dbHandler.define table_name, modelBody
  
  cbSuccess = ()=>
    console.log 'pg_handler: callback successful'
    dbHandler.query(query_string).success(
      (rows)=>
        console.log 'Records successfully retrieved'
        results = []
        for x in [0... rows.length]
          row = {}
          
          for y in [0...columns.length]
            row[columns[y]] = rows[x][columns[y]]
            
          row['createdAt'] = rows[x]['createdAt']
          row['updatedAt'] = rows[x]['updatedAt']
          row['pingedAt'] = rows[x]['pingedAt']            
          results.push row

        callback null, results
    ).error(
      (e)=>
        console.log "Error occured while fetching records\nError: %s ", e
        callback e, null
    )
      
  cbFailure = (error)=>
    console.log "rational db connection failure.\n Error message := " + error  
    callback error, null

  model.sync().success(cbSuccess).error(cbFailure)



# @Description: Get list of batches ever ran
# @param: table_name:string
# @param: callback:function(array[])
getBatches = (table_name, callback)=>

  model = dbHandler.define table_name, modelBody
  cbSuccess = ()=>

    query_string = 'select distinct("pingedAt") from "' + table_name + '" order by "pingedAt" desc'
    dbHandler.query(query_string).success(
      (rows)=>
        results = []
    
        for x in [0...rows.length]
          if d = rows[x].pingedAt        
            formated_datetime = d.getFullYear() + "-"  +  (d.getMonth() + 1)  + "-" + d.getDate()  +
              " " + d.getHours() + ":"  +  d.getMinutes()  + ":" + d.getSeconds() + "." + d.getMilliseconds()
              
            rows[x].pingedAt && results.push formated_datetime
        callback results
    
    ).error(
      (e)=>
        console.log "Error occured while fetching batches \nError: " + e
        callback []
    )

  cbFailure = (error)=>
    console.log "getBatches : table does not exist \nError: " + e
    callback []
  model.sync().success(cbSuccess).error(cbFailure)

  
  
# @Description: Get previous batch given current batch
# @param: table_name:string
# @param: current_batch:string
# @param: callback:function(string)
getPreviousBatch = (table_name, current_batch, callback)=>
  query_string = 'select distinct("pingedAt") from "' + table_name + '" ' + 
    ' where "pingedAt" <  \'' + current_batch + '\'' + 
    ' order by "pingedAt" desc limit 1 '

  console.log query_string

  dbHandler.query(query_string).success(
    (rows)=>
      results = []
  
      for x in [0...rows.length]
        if d = rows[x].pingedAt        
          formated_datetime = d.getFullYear() + "-"  +  (d.getMonth() + 1)  + "-" + d.getDate()  +
            " " + d.getHours() + ":"  +  d.getMinutes()  + ":" + d.getSeconds() + "." + d.getMilliseconds()
            
          rows[x].pingedAt && results.push formated_datetime
    
      callback results
  
  ).error(
    (e)=>
      console.log "Error occured while fetching batches \nError: " + e
      callback []
  )
   
  

# @Description : Returns in HTML table format an array of objects
# @param : objArray:array[obj1,obj2,obj3]
# @param : col_indexes:array[string1,string2,string3]
# @param : url_indexes:array[string1,string2,string3]
# @return: str:string - html table format
convertToHTML = (results, col_indexes, url_indexes)=>

  if typeof results != "object" 
    results = kson.parse(results) || []
  else 
    results = results || []
  
  str = "<table class='table' id='data_table'>\r\n"
  
  # adds column headers to just the first row
  str += "\t<tr id='data-table-header'>\r\n"    
  for y in [0...col_indexes.length]
    str += "\t\t<th>" + col_indexes[y] + "</th>\r\n"
  str += "\t</tr>\r\n"

  if results
    # iterates through all the columns
    for i in [0...results.length]
      col_indexes = col_indexes || Object.keys array[i]

      line = "\t<tr>\r\n";
      # adds a new row of record
      for y in [0...col_indexes.length]
        index = col_indexes[y]
        if url_indexes.indexOf(index) < 0
          line += "\t\t<td>" + results[i][index] + "</td>\r\n"
        else
          line += "\t\t<td><a target='_blank' rel='nofollow' href='" + results[i][index] + "'>" + results[i][index] + "</a></td>\r\n"
      line + "\t</tr>\r\n"
  
      str += line

  str += "</table>"
  return str


# @Description : Returns in CSV format an array of objects
# @param : objArray:array[obj1,obj2,obj3]
# @param : col_indexes:array[string1,string2,string3]
# @return: str:string - csv format
convertToCSV = (objArray, col_indexes)=>
    
  if typeof objArray != 'object' 
    array = kson.parse(objArray)
  else 
    array = objArray
  str = ''
  
  # iterates through all the columns
  for i in [0...array.length]
    line = '';
    col_indexes = col_indexes || Object.keys array[i]
    
    # adds column headers to just the first row
    if i == 0
      for y in [0...col_indexes.length]
        if (y > 0) 
          str += ','
        str += kson.stringify col_indexes[y]
      str += '\r\n'

    # adds a new row of record
    for y in [0...col_indexes.length]
      index = col_indexes[y]
      if (y > 0) 
        line += ','
      line += kson.stringify array[i][index];
    str += line + '\r\n'
  
  console.log str
  return str



# Web Server section of system
app = module.exports = express.createServer();



app.configure ()->
  app.set('views', __dirname + '/views');
  app.set('view engine', 'ejs');
  app.use(express.cookieParser());
  app.use(express.bodyParser());
  app.use(app.router);
  return app.use(express["static"](__dirname + "/public"))



# @Description : Redirects users to our documentation page if they come here directly via a GET request
app.get '/', (req, res)->
  console.log '[API_SERVER] / -> Redirecting to krake.io/docs'
  # res.redirect 'http://krake.io/docs'
  res.send 'this is it'



# @Description : get the list of batches ran for a data sources
app.get '/:table_name/batches', (req, res)=>
  getBatches req.params.table_name, (batches)=>
    res.send batches
  


# @Description : Returns an object with two arrays 1) records updated today, 2) records deleted today
app.get '/:table_name/diff/:format/:date', (req, res)=>

  getKrakeColumns req.params.table_name, (is_valid, columns)=>
  
    columns_in_query = getColumnsQuery columns

    async.parallel [
    
      # records updated & created today    
      (callback)=>
        query_string1 = 'SELECT ' + 
          columns_in_query +
          ' ,\"createdAt\", \"updatedAt\", \"pingedAt\" ' + 
          ' FROM "' + req.params.table_name + '" ' +
          ' WHERE "updatedAt" = \'' + req.params.date + '\' '
          
        getRecords req.params.table_name, columns, query_string1, (err, results)=>
          callback err, results
      
      # records deleted yesterday            
      , (callback)=>
        curr_day = new Date(req.params.date).getTime()
        prev_day = curr_day - (1 * 24 * 60 * 60 * 1000) # 1 day ago
        prev_d = new Date(prev_day)
        
        getPreviousBatch req.params.table_name, req.params.date, (previous_batch)=>
        
          if previous_batch
            query_string2 = 'SELECT ' + 
              columns_in_query +
              ' ,\"createdAt\", \"updatedAt\", \"pingedAt\" ' + 
              ' FROM "' + req.params.table_name + '" ' +
              ' WHERE '+ 
              ' "pingedAt" = \'' + previous_batch + '\'  '      

        
            getRecords req.params.table_name, columns, query_string2, (err, results)=>
              callback err, results
              
          else
            callback err, []
            
    ], (err, results_array)=>
    
      switch req.params.format
        when 'json' 
          console.log err
          results_obj = {}
          results_obj['updated'] = results_array[0]
          results_obj['deleted'] = results_array[1]
          res.send results_obj

        when 'csv' 
          res.header 'Content-Disposition', 'attachment;filename=' + req.params.table_name + '.csv'
          csv_results = kson.stringify('updated') + '\r\n'
          csv_results += convertToCSV results_array[0], columns
          csv_results += '\r\n\r\n'
          csv_results += kson.stringify('deleted') + '\r\n'
          csv_results += convertToCSV results_array[1], columns
           
          res.send csv_results



# @Description : Returns an array of JSON/CSV results based on query parameters
app.get '/:table_name/search/:format', (req, res)=>

  getKrakeColumns req.params.table_name, (is_valid, columns, url_columns)=>
    try
      q = kson.parse req.query.q
    catch e
      q = {
      }
  
    params = Object.keys q
  
    # Handles the where clause
    where_clause = ''
    for x in [0...params.length]
      switch params[x]
        when 'offset'
          offset = q[params[x]]
        when 'limit'
          limit = q[params[x]]
        when 'createdAt'
          where_clause += ' "createdAt" ' + queryValue( q[params[x]] ) + ' and '
        when 'updatedAt'
          where_clause += ' "updatedAt" ' + queryValue( q[params[x]] ) + ' and '
        when 'pingedAt'
          where_clause += ' "pingedAt" ' + queryValue( q[params[x]] ) + ' and ' 
        else
          where_clause += " properties->'" + params[x] + "' " + queryValue( q[params[x]] ) + ' and '

    where_clause.length > 0 && where_clause  = ' where ' + where_clause + ' true '
  
    # params.length == 0 && !offset && offset = 0
    # params.length == 0 && !limit && limit = 1000
    
    columns_in_query = getColumnsQuery columns    
  
    query_string = 'SELECT ' + 
      columns_in_query +
      ' ,\"createdAt\", \"updatedAt\", \"pingedAt\" ' + 
      ' FROM "' + req.params.table_name + '" ' +
      where_clause
    
    limit && query_string += 'LIMIT ' + limit + ' '
    offset && query_string += 'OFFSET ' + offset  
    
    # getRecords req.params.table_name, columns, url_columns, query_string, (err, results)=>    
    switch req.params.format
      when 'json'
        cm.getCache req.params.table_name, columns, url_columns, query_string, req.params.format, (err, pathToCache)->
          fs.createReadStream(pathToCache).pipe res
          
      when 'csv'
        res.header 'Content-Disposition', 'attachment;filename=' + req.params.table_name + '.csv'
        cm.getCache req.params.table_name, columns, url_columns, query_string, req.params.format, (err, pathToCache)->
          fs.createReadStream(pathToCache).pipe res

      when 'html'
        cm.getCache req.params.table_name, columns, url_columns, query_string, req.params.format, (err, pathToCache)->
          fs.createReadStream(pathToCache).pipe res      
          
          

# Start api server
port = process.argv[2] || 9803
app.listen port

console.log "Connections Established " + 
  "\n    Environment : %s" + 
  "\n    User Name : %s" + 
  "\n    Password : %s" + 
  "\n    Port : %s" + 
  "\n    Host : %s" + 
  "\n    Krake Definition DB : %s" +   
  "\n    Krake Scraped Data DB : %s" + 
  "\n    Data server listening at port : %s",
  ENV, userName, password, options.port, options.host, CONFIG.postgres.database, CONFIG.userDataDB, port