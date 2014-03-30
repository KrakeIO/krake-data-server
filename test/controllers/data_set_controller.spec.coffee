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
  
userName = process.env['KRAKE_PG_USERNAME'] || CONFIG.postgres.username
password = process.env['KRAKE_PG_PASSWORD'] || CONFIG.postgres.password

DataSetController = require '../../controllers/data_set_controller'
KrakeModel = require '../../models/krake_model'
KrakeSetModel = require '../../models/krake_set_model'
krake_definition = fs.readFileSync(__dirname + '/../fixtures/krake_definition.json').toString()

describe "DataSetController", ->

  beforeEach (done)->

    options = {}
    options.host = process.env['KRAKE_PG_HOST'] || CONFIG.postgres.host
    options.port = CONFIG.postgres.port
    options.dialect = 'postgres'
    options.logging = false
    pool = {}
    pool.maxConnections = 5
    pool.maxIdleTime = 30
    options.pool = pool
    
    @dbRepo = new Sequelize CONFIG.postgres.database, userName, password, options

    options["define"]=
      underscored: true
    @dbSystem = new Sequelize CONFIG.userDataDB, userName, password, options
    @set_name = "1_data_set_111111111111es"
    @repo1_name = "1_data_source_1111111es"
    @Krake = @dbSystem.define 'krakes', krakeSchema
    @Records = @dbRepo.define @repo1_name, recordBody
    @RecordSets = @dbRepo.define @set_name, recordSetBody

    chainer = new Sequelize.Utils.QueryChainer()
    chainer
      .add(@Krake.sync({force: true}))
      .add(@Records.sync({force: true}))
      .add(@RecordSets.sync({force: true}))
      .run()
      .success ()=>
        @Krake.create({ content: krake_definition, handle: @repo1_name}).then ()=>
            @km = new KrakeModel @dbSystem, @repo1_name, ()=>
              @ksm = new KrakeSetModel @dbSystem, @set_name, @km.columns, ()=>
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

      queries = []
      queries.push @km.getInsertStatement(data_obj1)
      queries.push @km.getInsertStatement(data_obj2)
      @dbRepo.query(queries.join(";")).then ()=>
        done()

    it "should return a list of all batches belonging to repo", (done)->
      @dsc.getRepoBatches @repo1_name, (results)=>
        expect(results.length).toEqual 2
        expect(results[0]).toEqual "2015-03-23 00:00:00+00"
        expect(results[1]).toEqual "2015-03-22 00:00:00+00"
        done()

  describe "clearBatches", ->

    describe "from same data source", ->

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

        queries = []

        queries.push @km.getInsertStatement(d1)
        queries.push @km.getInsertStatement(d2)
        queries.push @km.getInsertStatement(d3)

        queries.push @ksm.getInsertStatement(ds1)
        queries.push @ksm.getInsertStatement(ds2)
        queries.push @ksm.getInsertStatement(ds3)

        @dbRepo.query(queries.join(";")).then ()=>
          done()

      it "should clear all records belonging to data source", (done)->
        @dbRepo.query(@ksm.getSelectStatement {}).success (records)=>
          expect(records.length).toEqual 3
          @dsc.clearBatches @repo1_name, null, ()=>
            @dbRepo.query(@ksm.getSelectStatement {}).success (records)=>
              expect(records.length).toEqual 0
              done()

      it "should clear records belonging to the two most recent batches", (done)->
        @dbRepo.query(@ksm.getSelectStatement {}).success (records)=>
          expect(records.length).toEqual 3
          @dsc.clearBatches @repo1_name, 2, ()=>
            @dbRepo.query(@ksm.getSelectStatement {}).success (records)=>
              expect(records.length).toEqual 1
              expect(records[0].pingedAt).toEqual "2015-03-22 00:00:00"
              done()

    describe "from one data source in many", ->

      beforeEach (done)->
        @repo2_name = "2_data_source_2222222es"

        d1 = 
          "drug bank"         : "drug day 1"
          "drug name"         : "drug name day 1"
          "pingedAt"          : "2015-03-22 00:00:00"
          "createdAt"         : "2015-03-22 00:00:00"
          "updatedAt"         : "2015-03-22 00:00:00"

        ds1 = 
          "drug bank"         : "drug day 1"
          "drug name"         : "drug name day 1"
          "pingedAt"          : "2015-03-22 00:00:00"
          "createdAt"         : "2015-03-22 00:00:00"
          "updatedAt"         : "2015-03-22 00:00:00"
          "datasource_handle" : @repo1_name

        ds2 = 
          "drug bank"         : "drug day 3"
          "drug name"         : "drug name day 3"
          "pingedAt"          : "2015-03-22 00:00:00"
          "createdAt"         : "2015-03-22 00:00:00"
          "updatedAt"         : "2015-03-22 00:00:00"
          "datasource_handle" : @repo2_name

        queries = []
        queries.push @km.getInsertStatement(d1)
        queries.push @ksm.getInsertStatement(ds1)
        queries.push @ksm.getInsertStatement(ds2)
        @dbRepo.query(queries.join(";")).then ()=>
          done()     

      it "should clear all records belonging to one data source only", (done)->
        @dbRepo.query(@ksm.getSelectStatement {}).success (records)=>
          expect(records.length).toEqual 2
          @dsc.clearBatches @repo1_name, null, ()=>
            @dbRepo.query(@ksm.getSelectStatement {}).success (records)=>
              expect(records.length).toEqual 1
              expect(records[0].datasource_handle).toEqual @repo2_name
              done() 

  describe "copyBatches", ->

    it "should copy the all records over", (done)->
      d1 = 
        "drug bank"         : "drug day 1 funky"
        "drug name"         : "drug name day 1"
        "pingedAt"          : "2015-03-22 00:00:00"
        "createdAt"         : "2015-03-22 00:00:00"
        "updatedAt"         : "2015-03-22 00:00:00"

      d2 = 
        "drug bank"         : "drug day 2 funky"
        "drug name"         : "drug name day 2"
        "pingedAt"          : "2015-03-23 00:00:00"
        "createdAt"         : "2015-03-23 00:00:00"
        "updatedAt"         : "2015-03-23 00:00:00"

      d3 = 
        "drug bank"         : "drug day 3 funky"
        "drug name"         : "drug name day 3"
        "pingedAt"          : "2015-03-24 00:00:00"
        "createdAt"         : "2015-03-24 00:00:00"
        "updatedAt"         : "2015-03-24 00:00:00"

      queries = []
      queries.push @km.getInsertStatement(d1)
      queries.push @km.getInsertStatement(d2)
      queries.push @km.getInsertStatement(d3)
      @dbRepo.query(queries.join(";")).then ()=>
        @dbRepo.query(@ksm.getSelectStatement {}).success (records)=>
          expect(records.length).toEqual 0
          @dsc.copyBatches @repo1_name, null, ()=>
            @dbRepo.query(@ksm.getSelectStatement { $order : [{ $desc : "pingedAt" }] }).success (records)=>
              expect(records.length).toEqual 3
              expect(records[0].pingedAt).toEqual "2015-03-24 00:00:00"
              expect(records[0]["drug bank"]).toEqual "drug day 3 funky"
              expect(records[1].pingedAt).toEqual "2015-03-23 00:00:00"
              expect(records[1]["drug bank"]).toEqual "drug day 2 funky"
              expect(records[2].pingedAt).toEqual "2015-03-22 00:00:00"
              expect(records[2]["drug bank"]).toEqual "drug day 1 funky"              
              done()

    it "should copy the two most recent batch of records over", (done)->
      d1 = 
        "drug bank"         : "drug day 1 funky"
        "drug name"         : "drug name day 1"
        "pingedAt"          : "2015-03-22 00:00:00"
        "createdAt"         : "2015-03-22 00:00:00"
        "updatedAt"         : "2015-03-22 00:00:00"

      d2 = 
        "drug bank"         : "drug day 2 funky"
        "drug name"         : "drug name day 2"
        "pingedAt"          : "2015-03-23 00:00:00"
        "createdAt"         : "2015-03-23 00:00:00"
        "updatedAt"         : "2015-03-23 00:00:00"

      d3 = 
        "drug bank"         : "drug day 3 funky"
        "drug name"         : "drug name day 3"
        "pingedAt"          : "2015-03-24 00:00:00"
        "createdAt"         : "2015-03-24 00:00:00"
        "updatedAt"         : "2015-03-24 00:00:00"

      queries = []
      queries.push @km.getInsertStatement(d1)
      queries.push @km.getInsertStatement(d2)
      queries.push @km.getInsertStatement(d3)

      @dbRepo.query(queries.join(";")).then ()=>
        @dbRepo.query(@ksm.getSelectStatement {}).success (records)=>
          expect(records.length).toEqual 0
          @dsc.copyBatches @repo1_name, 2, ()=>
            @dbRepo.query(@ksm.getSelectStatement { $order : [{ $desc : "pingedAt" }] }).success (records)=>
              expect(records.length).toEqual 2
              expect(records[0].pingedAt).toEqual "2015-03-24 00:00:00"
              expect(records[0]["drug bank"]).toEqual "drug day 3 funky"
              expect(records[1].pingedAt).toEqual "2015-03-23 00:00:00"
              expect(records[1]["drug bank"]).toEqual "drug day 2 funky"
              done()

    it "should copy the recent batch of record over", (done)->
      d1 = 
        "drug bank"         : "drug day 1 funky"
        "drug name"         : "drug name day 1"
        "pingedAt"          : "2015-03-22 00:00:00"
        "createdAt"         : "2015-03-22 00:00:00"
        "updatedAt"         : "2015-03-22 00:00:00"

      @dbRepo.query(@km.getInsertStatement(d1)).then ()=>

        @dbRepo.query(@ksm.getSelectStatement {}).success (records)=>
          expect(records.length).toEqual 0
          @dsc.copyBatches @repo1_name, 2, ()=>
            @dbRepo.query(@ksm.getSelectStatement { $order : [{ $desc : "pingedAt" }] }).success (records)=>
              expect(records.length).toEqual 1
              expect(records[0].pingedAt).toEqual "2015-03-22 00:00:00"
              expect(records[0]["drug bank"]).toEqual "drug day 1 funky"
              done()

    it "should not crash when there are not records", (done)->
      @dbRepo.query(@ksm.getSelectStatement {}).success (records)=>
        expect(records.length).toEqual 0
        @dsc.copyBatches @repo1_name, 2, ()=>
          @dbRepo.query(@ksm.getSelectStatement { $order : [{ $desc : "pingedAt" }] }).success (records)=>
            expect(records.length).toEqual 0
            done()
