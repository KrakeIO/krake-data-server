fs = require 'fs'
kson = require 'kson'
Sequelize = require 'sequelize'
ktk = require 'krake-toolkit'
recordBody = require('krake-toolkit').schema.record
recordSetBody = require('krake-toolkit').schema.record_set
krakeSchema = require('krake-toolkit').schema.krake

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

DataSetController = require '../../controllers/data_set_controller'
KrakeModel = require '../../models/krake_model'
KrakeSetModel = require '../../models/krake_set_model'
krake_definition = fs.readFileSync(__dirname + '/../fixtures/krake_definition.json').toString()

describe "DataSetController", ->

  beforeEach (done)->
    @dbRepo = new Sequelize CONFIG.postgres.database, userName, password, options
    @dbSystem = new Sequelize CONFIG.userDataDB, userName, password, options
    @set_name = "1_data_set_111111111111es"
    @repo1_name = "1_data_source_1111111es"
    @Krake = @dbSystem.define 'krakes', krakeSchema
    @test_cols = ["drug bank", "drug name"]

    # Force reset dataSchema table in test database
    promise1 = @Krake.sync({force: true})
    promise2 = promise1.then ()=>
      @Krake.create({ content: krake_definition, handle: @repo1_name})

    promise3 = promise2.then ()=>
      # Force reset dataRepository table in test database
      @Records = @dbRepo.define @repo1_name, recordBody  
      @Records.sync({force: true}).success ()=>

        @RecordSets = @dbRepo.define @set_name, recordSetBody
        @RecordSets.sync({force: true}).success ()=>        

          @km = new KrakeModel @dbSystem, @repo1_name, ()=>
            @ksm = new KrakeSetModel @dbSystem, @set_name, @test_cols, ()=>
              @dsc = new DataSetController @dbSystem, @dbRepo, @set_name, ()=>
              done()

  describe "getRepoBatches", ->
    beforeEach (done)->
      data_obj1 = 
        "drug bank" : "drug day 1"
        "drug name" : "drug name day 1"
        "pingedAt"  : "2015-03-22 00:00:00"
        "createdAt" : "2015-03-22 00:00:00"
        "updatedAt" : "2015-03-22 00:00:00"

      data_obj2 = 
        "drug bank" : "drug day 2"
        "drug name" : "drug name day 2"
        "pingedAt"  : "2015-03-23 00:00:00"
        "createdAt" : "2015-03-23 00:00:00"
        "updatedAt" : "2015-03-23 00:00:00"

      insert_query1 = @km.getInsertStatement(data_obj1)
      insert_query2 = @km.getInsertStatement(data_obj2)

      promise1 = @dbRepo.query(insert_query1)
      promise2 = promise1.then ()=>
        @dbRepo.query(insert_query2)

      promise2.then ()=>
        done()

    it "should return a list of all batches belonging to repo", (done)->
      @dsc.getRepoBatches @repo1_name, (results)=>
        expect(results.length).toEqual 2
        expect(results[0]).toEqual "2015-03-23 00:00:00+00"
        expect(results[1]).toEqual "2015-03-22 00:00:00+00"
        done()

  describe "clearMostRecent2Batches", ->
    beforeEach (done)->
      d1 = 
        "drug bank"         : "drug day 1"
        "drug name"         : "drug name day 1"
        "pingedAt"          : "2015-03-22 00:00:00"
        "createdAt"         : "2015-03-22 00:00:00"
        "updatedAt"         : "2015-03-22 00:00:00"

      d2 = 
        "drug bank"         : "drug day 2"
        "drug name"         : "drug name day 2"
        "pingedAt"          : "2015-03-23 00:00:00"
        "createdAt"         : "2015-03-23 00:00:00"
        "updatedAt"         : "2015-03-23 00:00:00"

      d3 = 
        "drug bank"         : "drug day 3"
        "drug name"         : "drug name day 3"
        "pingedAt"          : "2015-03-24 00:00:00"
        "createdAt"         : "2015-03-24 00:00:00"
        "updatedAt"         : "2015-03-24 00:00:00"

      ds1 = 
        "drug bank"         : "drug day 1"
        "drug name"         : "drug name day 1"
        "pingedAt"          : "2015-03-22 00:00:00"
        "createdAt"         : "2015-03-22 00:00:00"
        "updatedAt"         : "2015-03-22 00:00:00"
        "datasource_handle" : "1_data_source_1111111es"

      ds2 = 
        "drug bank"         : "drug day 2"
        "drug name"         : "drug name day 2"
        "pingedAt"          : "2015-03-23 00:00:00"
        "createdAt"         : "2015-03-23 00:00:00"
        "updatedAt"         : "2015-03-23 00:00:00"
        "datasource_handle" : "1_data_source_1111111es"

      ds3 = 
        "drug bank"         : "drug day 3"
        "drug name"         : "drug name day 3"
        "pingedAt"          : "2015-03-24 00:00:00"
        "createdAt"         : "2015-03-24 00:00:00"
        "updatedAt"         : "2015-03-24 00:00:00"
        "datasource_handle" : "1_data_source_1111111es"

      insert_query1 = @km.getInsertStatement(d1)
      insert_query2 = @km.getInsertStatement(d2)
      insert_query3 = @km.getInsertStatement(d3)

      insert_query4 = @ksm.getInsertStatement(ds1)
      insert_query5 = @ksm.getInsertStatement(ds2)
      insert_query6 = @ksm.getInsertStatement(ds3)

      promise1 = @dbRepo.query insert_query1
      promise2 = promise1.then ()=>
        @dbRepo.query insert_query2

      promise3 = promise2.then ()=>
        @dbRepo.query insert_query3

      promise4 = promise3.then ()=>
        @dbRepo.query insert_query4

      promise5 = promise4.then ()=>
        @dbRepo.query insert_query5

      promise6 = promise4.then ()=>
        @dbRepo.query insert_query6

      promise6.then ()=>
        done()

    it "should return a list of all batches belonging to repo", (done)->
      @dbRepo.query(@ksm.getSelectStatement {}).success (records)=>
        expect(records.length).toEqual 3
        @dsc.clearMostRecent2Batches @repo1_name, ()=>
          @dbRepo.query(@ksm.getSelectStatement {}).success (records)=>
            expect(records.length).toEqual 1
            expect(records[0].pingedAt).toEqual "2015-03-22 00:00:00"
            done()
