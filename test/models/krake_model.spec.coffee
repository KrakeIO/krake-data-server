fs = require 'fs'
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
krake_definition = fs.readFileSync(__dirname + '/../fixtures/krake_definition.json').toString()

describe "KrakeModel", ->

  beforeEach (done)->
    @repo_name = "test_tables"
    @Krake = dbSystem.define 'krakes', krakeSchema 
    @Krake.sync({force: true}).success ()=>
      @Krake.create({ content: krake_definition, handle: @repo_name}).success ()->  
        done()

  it "should have columns", (done)->
    km = new KrakeModel dbSystem, @repo_name, (success, error_msg)->
      expect(km.columns.length).toEqual 6
      done()

  describe "selectClause", ->
    beforeEach (done)-> 
      @km = new KrakeModel dbSystem, @repo_name, ()->
        done()

    it "should return the properly formatted common columns ", (done)->
      select_clause = @km.selectClause { $select : [] }
      expect(select_clause).toEqual '"createdAt","updatedAt","pingedAt"'
      done()

    it "should return the properly formatted custom columns", (done)->
      select_clause = @km.selectClause { $select : ["col1", "col2"] }

      expected_query =  "properties::hstore->'col1' as \"col1\"" +
                        ",properties::hstore->'col2' as \"col2\"" +
                        ',"createdAt","updatedAt","pingedAt"'
      expect(select_clause).toEqual expected_query
      done()

    it "should return the properly formatted definition columns", (done)->
      select_clause = @km.selectClause {}

      expected_query =  'properties::hstore->\'drug bank\' as "drug bank",' +
                        'properties::hstore->\'drug name\' as "drug name",' +
                        'properties::hstore->\'categories\' as "categories",' +
                        'properties::hstore->\'therapeutic indication\' as "therapeutic indication",' +
                        'properties::hstore->\'origin_url\' as "origin_url",' +
                        'properties::hstore->\'origin_pattern\' as "origin_pattern",' +
                        '"createdAt","updatedAt","pingedAt"'
      expect(select_clause).toEqual expected_query
      done()

    it "should return $count"

    it "should return $distinct"

    it "should return $max"

    it "should return $min"

  describe "whereClause", ->
    beforeEach (done)-> 
      @km = new KrakeModel dbSystem, @repo_name, ()->
        done()

    it "should not return any where condition if there are not where conditions in query_obj", (done)->
      where_clause = @km.whereClause {}
      expect(where_clause).toEqual ""
      done()      

    it "should return well formated '=' where condition for common column", (done)->
      query_obj = 
        $where : [{ 
          pingedAt : "2013-07-06 02:57:09"
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "'pingedAt' = '2013-07-06 02:57:09'"
      done()

    it "should return well formated '=' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{ 
          col1 : "the test"
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "properties->'col1' = 'the test'"
      done()

    it "should return well formated 'contains' where condition"

    it "should return well formated '>' where condition"

    it "should return well formated '>=' where condition"

    it "should return well formated '<' where condition"

    it "should return well formated '<=' where condition"

    it "should return well formated '!=' where condition"

    it "should return well formated 'and' where condition"

    it "should return well formated 'or' where condition"

    it "should return well formated '!=null' where condition"
