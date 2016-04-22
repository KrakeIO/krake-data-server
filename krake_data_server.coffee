# @Description : this is a data server where users will call to consume the data
#   Sample:
#     http://data.krake.io/1_grouponsgs/json?q={"limit"":10,"offset"":10,"parent_cat_id":"45","status":"Active","createdAt":{"gt":"2013-04-15"}}
async             = require 'async'
express           = require 'express'
fs                = require 'fs'
kson              = require 'kson'
path              = require 'path'
Hstore            = require 'pg-hstore' #https://github.com/scarney81/pg-hstore
Sequelize         = require 'sequelize'
recordBody        = require('krake-toolkit').schema.record
recordSetBody     = require('krake-toolkit').schema.record_set
UnescapeStream    = require 'unescape-stream'

KrakeModel              = require './models/krake_model'
KrakeSetModel           = require './models/krake_set_model'
ModelFactoryController  = require './controllers/model_factory_controller'
S3Backup                = require './helpers/s3_backup'

CacheController         = require './controllers/cache_controller'
DataSetController       = require './controllers/data_set_controller'

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

access_key  = process.env['AWS_ACCESS_KEY']
secret_key  = process.env['AWS_SECRET_KEY']
region      = process.env['AWS_S3_REGION'] 
bucket_name = process.env['AWS_S3_BUCKET']

dbRepo = new Sequelize CONFIG.postgres.database, userName, password, options

options["define"]=
  underscored: true
  
dbSystem = new Sequelize CONFIG.userDataDB, userName, password, options
krakeSchema = require('krake-toolkit').schema.krake
Krake = dbSystem.define 'krakes', krakeSchema


sb = new S3Backup access_key, secret_key, region, bucket_name
cm = new CacheController CONFIG.cachePath, dbRepo, recordBody, sb
csm = new CacheController CONFIG.cachePath, dbRepo, recordSetBody
mfc = new ModelFactoryController dbSystem

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
  console.log "[DATA_SERVER] #{new Date()} clear cache — #{req.params.data_repository}"
  cm.clearCache req.params.data_repository, (err, status)=>
    if err
      res.send {status: "failed", error: err}
    else
      res.send {status: "success"}

app.get '/:data_repository/directory', (req, res)=>
  data_repository = req.params.data_repository
  console.log "[DATA_SERVER] #{new Date()} data source overview — #{data_repository}"

  km = new KrakeModel dbSystem, data_repository, [], (status, error_message)=>

  cm.getLatestCount(km).then (response)=>
    total_pages = Math.ceil(response.count / 1000)
    page_urls = []

    [0...total_pages].forEach (page_num)=>
      offset = page_num * 1000
      query = 
        "$where": [{
          "pingedAt": response.batch
        }]
        "$fresh": true
        "$limit": 1000
        "$offset": page_num

      url = "/#{data_repository}/html?q=" + JSON.stringify(query)
      page_urls.push url

    data = 
      count: response.count
      total_pages: total_pages
      page_urls: page_urls
      limit: 1000

    res.render 'overview', locals: data


# @Description : Returns an array of JSON/CSV results based on query parameters
app.get '/:data_repository/schema', (req, res)=>
  data_repository = req.params.data_repository  

  mfc.getModel data_repository, (FactoryModel)=>
    km = new FactoryModel dbSystem, data_repository, [], (status, error_message)=>
      console.log "[DATA_SERVER] #{new Date()} data source schema — #{req.params.data_repository}"
      response = 
        columns:       km.columns || []
        url_columns:   km.url_columns || []
        index_columns: km.index_columns || []      
      res.send response

# @Description : Returns an array of JSON/CSV results based on query parameters
app.get '/:data_repository/:format', (req, res)=>
  
  if !cm.isValidFormat( req.params.format )
    res.status(400).send { 
      status: "failed", 
      message: "'#{req.params.format}' is not a recognized format, only the following formats are recognized: json, csv, html" 
    }
    return

  data_repository = req.params.data_repository  
  mfc.getModel data_repository, (FactoryModel)=>
    km = new FactoryModel dbSystem, data_repository, [], (status, error_message)=>
      console.log "[DATA_SERVER] #{new Date()} data source query — #{data_repository}"
      query_obj = req.query.q && JSON.parse(req.query.q) || {}
      cm.getCacheStream data_repository, km, query_obj, req.params.format
        .then ( down_stream )=>
          
          res.header "Content-Type", cm.getContentType( req.params.format )
          res.header 'Content-Disposition', 'inline; filename=' + data_repository + '.' + req.params.format
          if req.params.format in [ 'csv' ]
            res.header 'Content-disposition', 'attachment; filename=' + data_repository + '.' + req.params.format

          down_stream.pipe res

        .catch ( err )=>
          console.log err
          res.send err

# @Description : Copies all records from data_repository over to dataset_repository
app.get '/connect/:data_repository/:dataset_repository', (req, res)=>
  console.log "[DATA_SERVER] #{new Date()} data set connect — #{req.params.dataset_repository}"
  dsc = new DataSetController dbSystem, dbRepo, req.params.dataset_repository, ()=>  
    dsc.consolidateBatches req.params.data_repository, null, ()=>
      res.send {status: "success", message: "connected" }

# @Description : Updates the records from data_repository over to dataset_repository
app.get '/synchronize/:data_repository/:dataset_repository', (req, res)=>
  console.log "[DATA_SERVER] #{new Date()} data set synchronize: \r\n\tkrake_handle: #{req.params.data_repository},\r\n\tdata_set_handle: #{req.params.dataset_repository}"

  dsc = new DataSetController dbSystem, dbRepo, req.params.dataset_repository, ()=>  
    dsc.consolidateBatches req.params.data_repository, 2, ()=>
      res.send {status: "success", message: "synchronized" }

# @Description : Removes all records belonging to data_repository from dataset_repository
app.get '/disconnect/:data_repository/:dataset_repository', (req, res)=>
  console.log "[DATA_SERVER] #{new Date()} data set disconnect"
  dsc = new DataSetController dbSystem, dbRepo, req.params.dataset_repository, ()=>  
    dsc.clearAll req.params.data_repository, ()=>
      res.send {status: "success", message: "disconnected" }



module.exports = 
  app : app
  dbRepo : dbRepo
  dbSystem : dbSystem
  krakeSchema : krakeSchema
  Krake :  Krake
  recordBody : recordBody
  recordSetBody : recordSetBody
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
    "\n    AWS_ACCESS_KEY : %s" + 
    "\n    AWS_SECRET_KEY : %s" + 
    "\n    AWS_S3_REGION : %s" + 
    "\n    AWS_S3_BUCKET : %s" + 
    "\n    Krake Definition DB : %s" +   
    "\n    Krake Scraped Data DB : %s" + 
    "\n    Data server listening at port : %s",
    ENV, userName, password, options.port, options.host, 
    access_key, secret_key, region, bucket_name,
    CONFIG.postgres.database, CONFIG.userDataDB, port


