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

    @Krake            = @dbSystem.define 'krakes', krakeSchema    
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
    chainer = new Sequelize.Utils.QueryChainer()
      .add(@dbSystem.sync({force: true}))      
      .run()
      .success ()=>
        done()

  afterEach ()=>
    app.close()

  describe "/:data_repository/schema", ->
    beforeEach (done)->
      chainer = new Sequelize.Utils.QueryChainer()
      chainer
        .add(@Records.sync({force: true}))
        .add(@RecordSets.sync({force: true}))
        .run()
        .success ()=>
          @Krake.create({ content: krake_definition, handle: @repo_name}).success ()=>
            done()


    it "should return all the columns ", (done)->
      d1 = new Date()
      api_location = @test_server + @repo_name + '/schema'
      request api_location, (error, response, body)=>
        expect(()=>
          JSON.parse body
        ).not.toThrow()
        results = JSON.parse body
        expect(results["columns"].length).toEqual 9
        done()    

  describe "/data_set/:dataset_repository/schema", ->
    beforeEach (done)->
      promise1 = @dbSystem.sync()
      promise2 = promise1.then ()=>
        @Krake.sync({force: true})

      promise3 = promise2.then ()=>
        @DataSet.sync({force: true})

      promise4 = promise3.then ()=>
        @DataSetKrake.sync({force: true})

      promise5 = promise4.then ()=>
        @RecordSets.sync({force: true})

      promise6 = promise5.then ()=>
        @Krake.create({ content: krake_definition, handle: @repo_name})

      promise7 = promise6.then (@krake_obj)=>
        @DataSet.create({ handle: @set_name, name: @set_name })

      promise8 = promise7.then (@dataset_obj)=>
        @dataset_obj.setKrakes [@krake_obj]

      promise9 = promise8.then ()=>
        @dataset_obj.getKrakes()

      promise10 = promise9.then (krakes)=>
        @ksm = new KrakeSetModel @dbSystem, @set_name, [], ()=>
          done()

    it "should port all records from data source table into data set table", (done)->
      api_location = @test_server + 'data_set/' + @set_name + '/schema'
      request api_location, (error, response, body)=>
        expect(()=>
          JSON.parse body
        ).not.toThrow()
        results = JSON.parse body
        expect(results["columns"].length).toEqual 10
        done()


