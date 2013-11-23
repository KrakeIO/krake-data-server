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
Sequelize = require 'sequelize'
CacheController = require './controllers/cache_controller'
KrakeModel = require './models/krake_model'

CONFIG = null
ENV = (process.env['NODE_ENV'] || 'development').toLowerCase()
try 
  CONFIG = kson.parse(fs.readFileSync(__dirname + '/config/config.js').toString())[ENV]
catch error
  console.log 'cannot parse config.js, %s', error
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

recordBody = require './schema/record'
cm = new CacheController CONFIG.cachePath, dbHandler, recordBody

# @Description : Converts raw query input value to actual stuff
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



# @Description : Converts the query object into where clause
# @param : queryObj:Object
# @return : whereString:String
whereClause = (queryString)->
  try
    q = kson.parse queryString
  catch e
    q = {}

  params = Object.keys q

  # Handles the where clause
  where_clause = ''
  for x in [0...params.length]
    switch params[x]
      when 'offset'
        offset = q[params[x]]
      when 'limit'
        limit = q[params[x]]
      when 'createdAt', 'updatedAt', 'pingedAt'
        where_clause += ' "' + params[x] + '" ' + queryValue( q[params[x]] ) + ' and '
      else
        where_clause += " properties->'" + params[x] + "' " + queryValue( q[params[x]] ) + ' and '

  where_clause.length > 0 && where_clause = ' where ' + where_clause + ' true '

  limit && where_clause += 'LIMIT ' + limit + ' '
  offset && where_clause += 'OFFSET ' + offset

  where_clause



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
  res.send 'Krake Data Server'



# @Description : get the list of batches ran for a data sources
app.get '/:table_name/batches', (req, res)=>
  cm.getBatches req.params.table_name, (batches)=>
    res.send batches
  


# @Description : Returns an object with two arrays 1) records updated today, 2) records deleted today
app.get '/:table_name/diff/:format/:date', (req, res)=>
  km = new KrakeModel db_dev, req.params.table_name, ()=>
    cm.getDiffCache km, req.params.date, req.params.format, (err, pathToCache)=>  
      if req.params.format == 'csv' then res.header 'Content-Disposition', 'attachment;filename=' + req.params.table_name + '.csv'
      fs.createReadStream(pathToCache).pipe res



# @Description : Returns an array of JSON/CSV results based on query parameters
app.get '/:table_name/search/:format', (req, res)=>
  km = new KrakeModel db_dev, req.params.table_name, ()=>
    query_string = 'SELECT ' + km.getColumnsQuery() + ' ,\"createdAt\", \"updatedAt\", \"pingedAt\" ' + 
      ' FROM "' + req.params.table_name + '" ' + whereClause(req.query.q)

    switch req.params.format
      when 'json'
        cm.getCache req.params.table_name, km.columns, km.url_columns, query_string, req.params.format, (err, pathToCache)->
          fs.createReadStream(pathToCache).pipe res
          
      when 'csv'
        res.header 'Content-Disposition', 'attachment;filename=' + req.params.table_name + '.csv'
        cm.getCache req.params.table_name, km.columns, km.url_columns, query_string, req.params.format, (err, pathToCache)->
          fs.createReadStream(pathToCache).pipe res

      when 'html'
        cm.getCache req.params.table_name, km.columns, km.url_columns, query_string, req.params.format, (err, pathToCache)->
          fs.createReadStream(pathToCache).pipe res
          
          
# @Description : Gets total records harvested in batch
app.get '/:table_name/count', (req, res)=>
  km = new KrakeModel db_dev, req.params.table_name, ()=>
    queryString = 'SELECT count(*) FROM "' + req.params.table_name + '" ' + whereClause (req.query.q)
    console.log queryString  
    dbHandler.query(queryString).success(
      (rows)=>
        if rows.length > 0 
          res.send rows[0]
        else
          res.send { "count" : 0 }
          
    ).error(
      (e)=>
        console.log "Error occured while fetching count \nError: " + e
        res.send { "count" : 0 }
    )    
    



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


