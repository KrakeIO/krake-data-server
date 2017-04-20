# @Description : this is a data server where users will call to consume the data
#   Sample:
#     http://data.krake.io/1_grouponsgs/json?q={"limit"":10,"offset"":10,"parent_cat_id":"45","status":"Active","createdAt":{"gt":"2013-04-15"}}
AWS               = require 'aws-sdk'
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
Q                 = require 'q'

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

AWS.config.update { 
  accessKeyId : access_key
  secretAccessKey : secret_key
  region : region
}

dbRepo = new Sequelize CONFIG.postgres.database, userName, password, options

options["define"]=
  underscored: true
  
dbSystem = new Sequelize CONFIG.userDataDB, userName, password, options
krakeSchema = require('krake-toolkit').schema.krake
Krake = dbSystem.define 'krakes', krakeSchema


sb = new S3Backup bucket_name
cm = new CacheController CONFIG.cachePath, dbRepo, recordBody, sb
csm = new CacheController CONFIG.cachePath, dbRepo, recordSetBody, sb
mfc = new ModelFactoryController dbSystem

getQueryObject = (req)->
  req.query.page = 1 if !req.query.page? || req.query.page < 1
  req.query.per_page = 100 if !req.query.per_page?
  req.query.per_page = 1000 if req.query.per_page? && req.query.per_page? > 1000

  query_obj = 
    $limit: 100
    $offset: 0

  query_obj.$limit = req.query.per_page
  query_obj.$offset = (req.query.page - 1) * query_obj.$limit

  if req.query.timestamp? && typeof req.query.timestamp == "string"
    if req.query.timestamp.match(/^([0-9]{10})$/)
      tzoffset = (new Date()).getTimezoneOffset() * 60000
      database_date = new Date( new Date(req.query.timestamp*1000) - tzoffset )
      query_obj.$where = [{ 
        pingedAt: database_date.toISOString().replace("T", " ").replace(".000Z", "")
      }]
    else
      query_obj.$where = [{ 
        pingedAt: req.query.timestamp
      }]

  query_obj



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
        domain:        km.domain
        columns:       km.columns || []
        url_columns:   km.url_columns || []
        index_columns: km.index_columns || []      
      res.send response


# @Description : Returns an array of JSON/CSV information
#   
#    Params:
#      data_repository:String
#      format:String
#        csv, json, html
#    
#    Additional Params:
#      timestamp:int(13)
#        E.g. 1492393690000
#
#      iso_string:string
#        E.g. 2017-04-16 18:48:10
#
app.get '/:data_repository/batches', (req, res)=>
  query_obj = 
    $select: ["pingedAt",
    {
      $count: "count"
    }],
    $order: [{
      $desc: "pingedAt"
    }],
    $groupBy : "pingedAt"
    $fresh: true    

  data_repository = req.params.data_repository

  mfc.getModel data_repository, (FactoryModel)=>
    km = new FactoryModel dbSystem, data_repository, [], (status, error_message)=>
      console.log "[DATA_SERVER] #{new Date()} data source query — #{data_repository}"
      console.log req.params.format
      console.log query_obj

      cm.getCachedRecords km, query_obj, "json"
        .then ( found_records )=>
          res.header "Content-Type", cm.getContentType( "json" )
          res.header 'Content-Disposition', 'inline; filename=' + data_repository + '_page_' + req.query.page + '.json'

          for record in found_records
            record.timestamp = record.pingedAt
            timestamp_int = new Date(record.pingedAt).getTime() / 1000
            record.total = record.count
            record.url = "#{CONFIG.serverPath}/#{data_repository}/batch_data?timestamp=#{timestamp_int}"
            delete record.pingedAt     
            delete record.count            

          res.send found_records

        .catch ( err )=>
          console.log err
          res.send err




# @Description : Returns an array of JSON/CSV results given a batch date
#   
#    Params:
#      data_repository:String
#      format:String
#        csv, json, html
#    
#    Additional Params:
#      timestamp:String - in either of the following format
#        int(10) - 1492393690
#        string - 2017-04-16 18:48:10
#      per_page:Integer
#      page:Integer
#  
app.get '/:data_repository/batch_data', (req, res)=>  
  query_obj = getQueryObject req
  data_repository = req.params.data_repository

  mfc.getModel data_repository, (FactoryModel)=>
    km = new FactoryModel dbSystem, data_repository, [], (status, error_message)=>
      console.log "[DATA_SERVER] #{new Date()} data source query — #{data_repository}"
      Q.all([
        cm.getCachedRecords(km, query_obj, "json"),
        cm.getCountForBatchFromQuery(km, query_obj)
      ])
        .then ( results )=>
          found_records = results[0]
          count_records = results[1]

          res.header "Content-Type", cm.getContentType( "json" )
          res.header 'Content-Disposition', 'inline; filename=' + data_repository + '_page_' + req.query.page + '.json'

          pagination = 
            current_page: parseInt(req.query.page)
            per_page: parseInt(req.query.per_page)

          total = parseInt(count_records.count)
          pagination.total_pages = Math.ceil(total / pagination.per_page)

          timestamp_int = new Date(count_records.batch).getTime() / 1000

          if total > pagination.page * pagination.per_page
            pagination.next_page = "#{CONFIG.serverPath}/#{data_repository}/batch_data?timestamp=#{timestamp_int}&page=#{pagination.page+1}&per_page=#{pagination.per_page}"

          if pagination.page > 1
            pagination.prev_page = "#{CONFIG.serverPath}/#{data_repository}/batch_data?timestamp=#{timestamp_int}&page=#{pagination.page-1}&per_page=#{pagination.per_page}"

          res.send 
            total: total
            timestamp: count_records.batch
            pagination: pagination
            results: found_records

        .catch ( err )=>
          console.log err
          res.send err


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
      console.log req.params.format
      console.log query_obj
      cm.getCacheStream km, query_obj, req.params.format
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


