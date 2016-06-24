process.env['NODE_ENV'] = 'test'
request = require 'request'
KrakeModel = require '../models/krake_model'
KrakeSetModel = require '../models/krake_set_model'
fs = require 'fs'
Sequelize = require 'sequelize'
krake_definition = fs.readFileSync(__dirname + '/fixtures/krake_definition.json').toString()

test_objects    = require "../krake_data_server"
app             = test_objects.app
dbRepo          = test_objects.dbRepo
dbSystem        = test_objects.dbSystem
krakeSchema     = test_objects.krakeSchema
recordBody      = test_objects.recordBody
recordSetBody   = test_objects.recordSetBody
CacheController = test_objects.CacheController

dataSetSchema           = require('krake-toolkit').schema.data_set
dataSetKrakeSchema      = require('krake-toolkit').schema.data_set_krake
dataSetKrakeRuleSchema  = require('krake-toolkit').schema.data_set_krake_rule

describe "krake data server", ->
  beforeEach (done)->
    @dbRepo = dbRepo
    @dbSystem = dbSystem    
    @repo_name = "1_66240a39bc8c73a3ec2a08222936fc49eses"
    @set_name = "1_data_set_111111111111es"    

    @port = 9803
    @test_server = "http://localhost:" + @port + "/"

    @Krake = dbSystem.define 'krakes', krakeSchema    
    @DataSet          = @dbSystem.define 'data_sets', dataSetSchema
    @DataSetKrake     = @dbSystem.define 'data_set_krakes', dataSetKrakeSchema
    @DataSetKrakeRule = @dbSystem.define 'data_set_krake_rules', dataSetKrakeRuleSchema

    @DataSet.hasMany @Krake, { through: @DataSetKrake}
    @Krake.hasMany @DataSet, { through: @DataSetKrake}

    @DataSetKrakeRule.belongsTo @DataSetKrake
    @DataSetKrake.hasMany @DataSetKrakeRule, { as: "data_set_krake_rule", foreignKey: 'data_set_krake_id'}

    @Records = dbRepo.define @repo_name, recordBody
    @RecordSets = @dbRepo.define @set_name, recordSetBody

    app.listen @port
    @dbSystem.sync({force: true}).done ()=>
      chainer = new Sequelize.Utils.QueryChainer()
        .add(@Krake.sync({force: true}))
        .add(@Records.sync({force: true}))
        .run()
        .success ()=>      
          @Krake.create({ content: krake_definition, handle: @repo_name})    
          done()

  afterEach ()=>
    app.close()

  it "should make connection with krake data server", (done)->
    request @test_server, (error, response, body)->
      expect(error).toBe null
      expect(response.statusCode).toEqual 200
      expect(body).toEqual "Krake Data Server"
      done()

  it "should run krake data server in test mode", (done)->
    request @test_server + 'env', (error, response, body)->
      expect(body).toEqual "test"
      done()

  describe "/connect", ->
    beforeEach (done)->
      @repo1_name = "1_data_source_1111111es"
      @Records    = @dbRepo.define @repo1_name, recordBody  

      chainer = new Sequelize.Utils.QueryChainer()
      chainer
        .add(@Records.sync({force: true}))
        .add(@RecordSets.sync({force: true}))
        .run()
        .success ()=>
          @Krake.create({ content: krake_definition, handle: @repo1_name}).then ()=>
            @km = new KrakeModel dbSystem, @repo1_name, [], ()=>
              @ksm = new KrakeSetModel dbSystem, @set_name, @km.columns, ()=>

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

                promise1 = @dbRepo.query queries.join(";")
                promise1.then ()=>
                  done()


    it "should port all records from data source table into data set table", (done)->
      d1 = new Date()
      api_location = @test_server + 'connect/' + @repo1_name + '/' + @set_name
      @dbRepo.query(@ksm.getSelectStatement {}).then (records)=>
        records = records[0]
        expect(records.length).toEqual 0
        request api_location, (error, response, body)=>
          expect(()=>
            JSON.parse body
          ).not.toThrow()          
          results = JSON.parse body
          expect(results["status"]).toEqual "success"
          expect(results["message"]).toEqual "connected"

          @dbRepo.query(@ksm.getSelectStatement { $order : [{ $desc : "pingedAt" }] }).then (records)=>
            records = records[0]
            expect(records.length).toEqual 3
            expect(records[0].pingedAt).toEqual "2015-03-24 00:00:00"
            expect(records[0]["drug bank"]).toEqual "drug day 3 funky"
            expect(records[1].pingedAt).toEqual "2015-03-23 00:00:00"
            expect(records[1]["drug bank"]).toEqual "drug day 2 funky"
            expect(records[2].pingedAt).toEqual "2015-03-22 00:00:00"
            expect(records[2]["drug bank"]).toEqual "drug day 1 funky"
            done()

  describe "/synchronize", ->
    beforeEach (done)->
      @repo1_name = "1_data_source_1111111es" 
      @Records = @dbRepo.define @repo1_name, recordBody

      chainer = new Sequelize.Utils.QueryChainer()
      chainer
        .add(@Records.sync({force: true}))
        .add(@RecordSets.sync({force: true}))
        .run()
        .success ()=>
          @Krake.create({ content: krake_definition, handle: @repo1_name}).then ()=>
            @km = new KrakeModel dbSystem, @repo1_name, [], ()=>
              @ksm = new KrakeSetModel dbSystem, @set_name, @km.columns, ()=>

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

                promise1 = @dbRepo.query queries.join(";")
                promise1.then ()=>
                  done()


    it "should port latest records from data source table into data set table", (done)->
      d1 = new Date()
      api_location = @test_server + 'synchronize/' + @repo1_name + '/' + @set_name
      @dbRepo.query(@ksm.getSelectStatement {}).then (records)=>
        records = records[0]
        expect(records.length).toEqual 0
        request api_location, (error, response, body)=>
          expect(()=>
            JSON.parse body
          ).not.toThrow()          
          results = JSON.parse body
          expect(results["status"]).toEqual "success"
          expect(results["message"]).toEqual "synchronized"

          @dbRepo.query(@ksm.getSelectStatement { $order : [{ $desc : "pingedAt" }] }).then (records)=>
            records = records[0]
            expect(records.length).toEqual 2
            expect(records[0].pingedAt).toEqual "2015-03-24 00:00:00"
            expect(records[0]["drug bank"]).toEqual "drug day 3 funky"
            expect(records[1].pingedAt).toEqual "2015-03-23 00:00:00"
            expect(records[1]["drug bank"]).toEqual "drug day 2 funky"
            done()

  describe "/disconnect", ->
    beforeEach (done)->
      @repo1_name = "1_data_source_1111111es" 
      @repo2_name = "2_data_source_2222222es"

      chainer = new Sequelize.Utils.QueryChainer()
      chainer
        .add(@Records.sync({force: true}))
        .add(@RecordSets.sync({force: true}))
        .run()
        .success ()=>

          @Krake.create({ content: krake_definition, handle: @repo1_name}).then ()=>
            @Krake.create({ content: krake_definition, handle: @repo2_name})
          .then ()=>
            @km = new KrakeModel dbSystem, @repo1_name, [], ()=>
              @ksm = new KrakeSetModel dbSystem, @set_name, @km.columns, ()=>

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
                queries_st = queries.join(";")

                promise1 = @dbRepo.query queries_st
                promise1.then ()=>
                  done()  


    it "should delete all records belonging to data source table from data set table", (done)->
      d1 = new Date()
      api_location = @test_server + 'disconnect/' + @repo1_name + '/' + @set_name
      @dbRepo.query(@ksm.getSelectStatement {}).then (records)=>
        records = records[0]
        expect(records.length).toEqual 2
        request api_location, (error, response, body)=>
          expect(()=>
            JSON.parse body
          ).not.toThrow()          
          results = JSON.parse body
          expect(results["status"]).toEqual "success"
          expect(results["message"]).toEqual "disconnected"

          @dbRepo.query(@ksm.getSelectStatement { $order : [{ $desc : "pingedAt" }] }).then (records)=>
            records = records[0]
            expect(records.length).toEqual 1
            expect(records[0].datasource_handle).toEqual @repo2_name
            done()
