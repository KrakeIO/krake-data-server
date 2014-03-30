fs                      = require 'fs'
kson                    = require 'kson'
Sequelize               = require 'sequelize'
schemaConfig            = require('krake-toolkit').schema.config 
recordSetBody           = require('krake-toolkit').schema.record_set
krakeSchema             = require('krake-toolkit').schema.krake
dataSetSchema           = require('krake-toolkit').schema.data_set
dataSetKrakeSchema      = require('krake-toolkit').schema.data_set_krake
dataSetKrakeRuleSchema  = require('krake-toolkit').schema.data_set_krake_rule
CONFIG = null
ENV = "test"
try 
  CONFIG = kson.parse(fs.readFileSync(__dirname + '/../../config/config.js').toString())[ENV];
catch error
  console.log 'cannot parse config.js, %s', error
  process.exit(1)
  
options             = {}
options.host        = process.env['KRAKE_PG_HOST'] || CONFIG.postgres.host
options.port        = CONFIG.postgres.port
options.dialect     = 'postgres'
options.logging     = false
pool                = {}
pool.maxConnections = 5
pool.maxIdleTime    = 30
options.pool        = pool
userName            = process.env['KRAKE_PG_USERNAME'] || CONFIG.postgres.username
password            = process.env['KRAKE_PG_PASSWORD'] || CONFIG.postgres.password

dbRepo    = new Sequelize CONFIG.postgres.database, userName, password, options

options["define"]=
  underscored: true
  
dbSystem  = new Sequelize CONFIG.userDataDB, userName, password, options

KrakeSetModel     = require '../../models/krake_set_model'
krake_definition  = fs.readFileSync(__dirname + '/../fixtures/krake_definition.json').toString()

describe "KrakeSetModel", ->

  beforeEach (done)->
    @dbRepo     = dbRepo
    @dbSystem   = dbSystem
    @repo_name  = "krake_source_tests"
    @set_name   = "krake_sets_tests"

    @RecordSets    = dbRepo.define @set_name, recordSetBody

    @Krake            = @dbSystem.define 'krakes', krakeSchema, schemaConfig
    @DataSet          = @dbSystem.define 'data_sets', dataSetSchema, schemaConfig
    @DataSetKrake     = @dbSystem.define 'data_set_krakes', dataSetKrakeSchema, schemaConfig
    @DataSetKrakeRule = @dbSystem.define 'data_set_krake_rules', dataSetKrakeRuleSchema, schemaConfig

    @DataSet.hasMany @Krake, { through: @DataSetKrake}
    @Krake.hasMany @DataSet, { through: @DataSetKrake}

    @DataSetKrakeRule.belongsTo @DataSetKrake
    @DataSetKrake.hasMany @DataSetKrakeRule, { as: "data_set_krake_rule", foreignKey: 'data_set_krake_id'}

    promise1 = @dbSystem.sync()
    promise2 = promise1.then ()=>
      @Krake.sync({force: true})

    promise3 = promise2.then ()=>
      @DataSet.sync({force: true})

    promise4 = promise3.then ()=>
      @DataSetKrake.sync({force: true})

    promise5 = promise4.then ()=>
      # Force reset dataRepository table in test database
      @RecordSets.sync({force: true}).success ()=>

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

  it "should not crash when krake content is invalid", (done)->
    ksm = new KrakeSetModel @dbSystem, @set_name, null, (is_valid)->
      expect(is_valid).toBe true      
      done()

  it "should return a krake that is associated with the dataset", (done)->
    @ksm.loadKrakes (krakes)=>
      expect(krakes.length).toEqual 1
      expect(krakes[0].handle).toEqual @repo_name
      done()

  it "should return a krake that is associated with the dataset", ->
    expect(@ksm.columns.length).toEqual 10

  describe "multiple krakes", (done)->

    it "should return two krakes ", (done)->
      krake_definition  = fs.readFileSync(__dirname + '/../fixtures/krake_definition.json').toString()
      promise6 = @Krake.create({ content: krake_definition, handle: @repo_name})


      promise7 = promise6.then (krake_obj2)=>
        @dataset_obj.setKrakes [@krake_obj, krake_obj2]

      promise8 = promise7.then ()=>
        @dataset_obj.getKrakes()

      promise8.then (krakes)=>
        @ksm = new KrakeSetModel @dbSystem, @set_name, [], ()=>
          @ksm.loadKrakes (krakes)=>
            expect(krakes.length).toEqual 2
            expect(krakes[0].handle).toEqual @repo_name
            expect(krakes[1].handle).toEqual @repo_name
            done()

    it "should return not return duplicate cols if both krakes share the same krake definition ", (done)->
      krake_definition  = fs.readFileSync(__dirname + '/../fixtures/krake_definition.json').toString()
      promise6 = @Krake.create({ content: krake_definition, handle: @repo_name})

      promise7 = promise6.then (krake_obj2)=>
        @dataset_obj.setKrakes [@krake_obj, krake_obj2]

      promise8 = promise7.then ()=>
        @dataset_obj.getKrakes()

      promise8.then (krakes)=>
        @ksm = new KrakeSetModel @dbSystem, @set_name, [], ()=>
          expect(@ksm.columns.length).toEqual 10
          done()

    it "should add unique columns from all child krakes  ", (done)->
      krake_definition  = fs.readFileSync(__dirname + '/../fixtures/krake_definition2.json').toString()
      promise6 = @Krake.create({ content: krake_definition, handle: @repo_name})

      promise7 = promise6.then (krake_obj2)=>
        @dataset_obj.setKrakes [@krake_obj, krake_obj2]

      promise8 = promise7.then ()=>
        @dataset_obj.getKrakes()

      promise8.then (krakes)=>
        @ksm = new KrakeSetModel @dbSystem, @set_name, [], ()=>
          expect(@ksm.columns.length).toEqual 11
          done()