fs = require 'fs'
rimraf = require 'rimraf'
kson = require 'kson'
Sequelize = require 'sequelize'
ktk = require 'krake-toolkit'
krakeSchema = ktk.schema.krake

CONFIG = null
ENV = "test"
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

dbRepo = new Sequelize CONFIG.postgres.database, userName, password, options
dbSystem = new Sequelize CONFIG.userDataDB, userName, password, options

recordBody = require '../../schema/record'
CacheController = require '../../controllers/cache_controller'
KrakeModel = require '../../models/krake_model'
fixture = fs.readFileSync(__dirname + '/../fixtures/krake_definition.json').toString()


describe "CacheManager", ()->
  beforeEach (done) ->
    @test_folder = "/tmp/test_folder/"
    rimraf.sync @test_folder
    fs.rmdirSync(@test_folder) if fs.existsSync(@test_folder)
    @repo_name = "test_tables"
    
    @Krake = dbSystem.define 'krakes', krakeSchema 
    @Krake.sync({force: true}).success ()=>
      @Krake.create({ content: fixture, handle: @repo_name})   
      @cm = new CacheController @test_folder, dbRepo, recordBody
      @Records = dbRepo.define @repo_name, recordBody  
      @Records.sync({force: true}).success ()=>
        @km = new KrakeModel dbSystem, @repo_name, (success, error_msg)->
          done()

  it "should create a cache folder if it does not exist", (done)->
    fs.rmdirSync(@test_folder) if fs.existsSync(@test_folder)
    expect(fs.existsSync @test_folder).toBe false
    cm = new CacheController @test_folder, dbRepo, recordBody
    expect(fs.existsSync @test_folder).toBe true
    done()

  it "should clear cache folder of all files", (done)->
    expect(false).toBe true
    done()