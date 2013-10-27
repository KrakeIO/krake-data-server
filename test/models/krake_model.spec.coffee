fs = require 'fs'
kson = require 'kson'
Sequelize = require 'sequelize'

CONFIG = null
ENV = (process.env['NODE_ENV'] || 'development').toLowerCase()
try 
  CONFIG = kson.parse(fs.readFileSync(__dirname + '/../../config/config.js').toString())[ENV];
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

recordBody = require '../../schema/record'
CacheController = require '../../controllers/cache_controller'
KrakeModel = require '../../models/krake_model'

km = new KrakeModel db_dev