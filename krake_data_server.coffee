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

dbRepo = new Sequelize CONFIG.postgres.database, userName, password, options
dbSystem = new Sequelize CONFIG.userDataDB, userName, password, options
krakeSchema = ktk.schema.krake
Krake = dbSystem.define 'krakes', krakeSchema

recordBody = require './schema/record'
cm = new CacheController CONFIG.cachePath, dbRepo, recordBody

# Web Server section of system
app = module.exports = express.createServer();

app.configure ()->
  app.set('views', __dirname + '/views');
  app.set('view engine', 'ejs');
  app.use(express.cookieParser());
  app.use(express.bodyParser());
  app.use(app.router);
  return app.use(express["static"](__dirname + "/public"))

# @Description : Indicates to the user that this is a Krake data server
app.get '/', (req, res)->
  res.send 'Krake Data Server'

# @Description : Indicates the current environment this Krake data server is running in
app.get '/env', (req, res)->
  res.send ENV

# @Description : clears all the cache generated for table
app.get '/:data_repository/clear_cache', (req, res)=>
  cm.clearCache req.params.data_repository, (err, status)=>
    if err
      res.send {status: "failed", error: err}
    else
      res.send {status: "success"}

# @Description : Returns an array of JSON/CSV results based on query parameters
app.get '/:data_repository/:format', (req, res)=>
  data_repository = req.params.data_repository
  km = new KrakeModel dbSystem, data_repository, (status, error_message)=>
    query_obj = req.query.q && JSON.parse(req.query.q) || {}
    cm.getCache data_repository, km, query_obj, req.params.format, (error, path_to_cache)=>
      if req.params.format == 'csv'
        res.header 'Content-Disposition', 'attachment;filename=' + req.params.data_repository + '.csv'
      fs.createReadStream(path_to_cache).pipe res

module.exports = 
  app : app
  dbRepo : dbRepo
  dbSystem : dbSystem
  krakeSchema : krakeSchema
  Krake :  Krake
  recordBody : recordBody
  CacheController : CacheController

if !module.parent
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


