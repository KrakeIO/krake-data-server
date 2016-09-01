fs = require 'fs'
kson = require 'kson'
Sequelize = require 'sequelize'
ktk = require 'krake-toolkit'
recordBody = require('krake-toolkit').schema.record
recordSetBody = require('krake-toolkit').schema.record_set
krakeSchema = require('krake-toolkit').schema.krake
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
  
userName = process.env['KRAKE_PG_USERNAME'] || CONFIG.postgres.username
password = process.env['KRAKE_PG_PASSWORD'] || CONFIG.postgres.password

ModelFactoryController = require '../../controllers/model_factory_controller'
KrakeModel = require '../../models/krake_model'
KrakeSetModel = require '../../models/krake_set_model'
krake_definition = fs.readFileSync(__dirname + '/../fixtures/krake_definition.json').toString()

describe "ModelFactoryController", ->

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
    options.define =
      underscored: true

    @dbSystem = new Sequelize CONFIG.userDataDB, userName, password, options

    @set_name = "1_data_set_111111111111es"
    @repo_name = "1_data_source_1111111es"

    @Krake            = @dbSystem.define 'krakes', krakeSchema    
    @DataSet          = @dbSystem.define 'data_sets', dataSetSchema
    @DataSetKrake     = @dbSystem.define 'data_set_krakes', dataSetKrakeSchema
    @DataSetKrakeRule = @dbSystem.define 'data_set_krake_rules', dataSetKrakeRuleSchema

    @DataSet.hasMany @Krake, { through: @DataSetKrake}
    @Krake.hasMany @DataSet, { through: @DataSetKrake}

    @DataSetKrakeRule.belongsTo @DataSetKrake
    @DataSetKrake.hasMany @DataSetKrakeRule, { as: "data_set_krake_rule", foreignKey: 'data_set_krake_id'}

    @dbSystem.sync({force: true}).then ()=>
      @mfc = new ModelFactoryController @dbSystem, ()=>
        @Krake.create({ content: krake_definition, handle: @repo_name})
          .then ()=> @DataSet.create({ handle: @set_name, name: @set_name })
          .then ()=> done()
          .catch (e)=>
            console.log e
            done()

  it "should indicate true if a handle belong to a krake", (done)->
    @mfc.isKrake @repo_name, (result)=>
      expect(result).toBe true
      done()

  it "should indicate false if a handle does not belong to a krake", (done)->
    @mfc.isKrake @set_name, (result)=>
      expect(result).toBe false
      done()      

  it "should indicate true if a handle belongs to a data_set", (done)->
    @mfc.isDataSet @set_name, (result)=>
      expect(result).toBe true
      done()  

  it "should indicate false if a handle does not belongs to a data_set", (done)->
    @mfc.isDataSet @repo_name, (result)=>
      expect(result).toBe false
      done()  