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
    @dbRepo = dbRepo    
    @repo_name = "1_66240a39bc8c73a3ec2a08222936fc49eses"
    @Krake = dbSystem.define 'krakes', krakeSchema 

    # Force create dataSchema table in test database
    @Krake.sync({force: true}).success ()=>
      @Krake.create({ content: krake_definition, handle: @repo_name}).success ()=>

        # Force create dataRepository table in test database
        @Records = dbRepo.define @repo_name, recordBody  
        @Records.sync({force: true}).success ()=>

          # instantiates a krake model
          @km = new KrakeModel dbSystem, @repo_name, ()->
            done()

  it "should have columns", (done)->
    km = new KrakeModel dbSystem, @repo_name, (success, error_msg)->
      expect(km.columns.length).toEqual 6
      done()

  describe "getQuery", =>
    it "should call all the sub clauses", (done)->
      spyOn(@km, 'selectClause').andCallThrough()
      spyOn(@km, 'whereClause').andCallThrough()
      query_string = @km.getQuery({})
      expect(()=>  
        @dbRepo.query query_string
      ).not.toThrow()      
      expect(@km.selectClause).toHaveBeenCalled()
      expect(@km.whereClause).toHaveBeenCalled()
      done()

    it "should call all the sub clauses", (done)->
      spyOn(@km, 'selectClause').andCallThrough()
      spyOn(@km, 'whereClause').andCallThrough()
      query_string = @km.getQuery({})
      expect(()=>
        @dbRepo.query query_string
      ).not.toThrow()
      done()

  describe "selectClause", ->
    it "should return the properly formatted common columns ", (done)->
      select_clause = @km.selectClause { $select : [] }
      expect(select_clause).toEqual '1'
      done()

    it "should not return duplicated common columns ", (done)->
      select_clause = @km.selectClause { $select : ["createdAt"] }
      expect(select_clause).toEqual '"createdAt"'
      done()

    it "should return the properly formatted repository columns", (done)->
      select_clause = @km.selectClause { $select : ["col1", "col2"] }

      expected_query =  "properties::hstore->'col1' as \"col1\"" +
                        ",properties::hstore->'col2' as \"col2\""
      expect(select_clause).toEqual expected_query
      done()

    it "should return the properly formatted repository columns", (done)->
      select_clause = @km.selectClause {}

      expected_query =  'properties::hstore->\'drug bank\' as "drug bank",' +
                        'properties::hstore->\'drug name\' as "drug name",' +
                        'properties::hstore->\'categories\' as "categories",' +
                        'properties::hstore->\'therapeutic indication\' as "therapeutic indication",' +
                        'properties::hstore->\'origin_url\' as "origin_url",' +
                        'properties::hstore->\'origin_pattern\' as "origin_pattern"'
      expect(select_clause).toEqual expected_query
      done()

    it "should not create an invalid normal select statement", (done)->
      query_string = @km.getQuery({})
      expect(()=>  
        @dbRepo.query query_string
      ).not.toThrow()
      done()


    describe "$count", ->
      it "should return properly formatted common columns for $count", (done)->
        select_clause = @km.selectClause { $select : [{ $count : "pingedAt" }] }
        expect(select_clause).toEqual 'count("pingedAt") as "pingedAt"'
        done()

      it "should return properly formatted repository columns for $count", (done)->
        select_clause = @km.selectClause { $select : [{ $count : "col1" }] }
        expect(select_clause).toEqual 'count(properties::hstore->\'col1\') as "col1"'
        done()

      it "should not create an invalid $count select statement for common columns", (done)->
        query_string = @km.getQuery({ $select : [{ $count : "pingedAt" }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

      it "should not create an invalid $count select statement for repository columns", (done)->
        query_string = @km.getQuery({ $select : [{ $count : @km.columns[0] }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

    describe "$distinct", ->
      it "should return properly formatted common columns for $distinct", (done)->
        select_clause = @km.selectClause { $select : [{ $distinct : "pingedAt" }] }
        expect(select_clause).toEqual 'distinct cast("pingedAt" as text) as "pingedAt"'
        done()

      it "should return properly formatted repository columns for $distinct", (done)->
        select_clause = @km.selectClause { $select : [{ $distinct : "col1" }] }
        expect(select_clause).toEqual 'distinct cast(properties::hstore->\'col1\' as text) as "col1"'
        done()

      it "should not create an invalid $distinct select statement for common columns", (done)->
        query_string = @km.getQuery({ $select : [{ $distinct : "pingedAt" }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

      it "should not create an invalid $distinct select statement for repository columns", (done)->
        query_string = @km.getQuery({ $select : [{ $distinct : @km.columns[0] }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

    describe "$max", ->
      it "should return properly formatted common columns for $max", (done)->
        select_clause = @km.selectClause { $select : [{ $max : "pingedAt" }] }
        expect(select_clause).toEqual 'max("pingedAt") as "pingedAt"'
        done()

      it "should return properly formatted repository columns for $max", (done)->
        select_clause = @km.selectClause { $select : [{ $max : "col1" }] }
        expect(select_clause).toEqual 'max(properties::hstore->\'col1\') as "col1"'
        done()

      it "should not create an invalid $distinct select statement for common columns", (done)->
        query_string = @km.getQuery({ $select : [{ $max : "pingedAt" }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

      it "should not create an invalid $distinct select statement for repository columns", (done)->
        query_string = @km.getQuery({ $select : [{ $max : @km.columns[0] }] })
        console.log query_string
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

    describe "$min", ->
      it "should return properly formatted common columns for $min", (done)->
        select_clause = @km.selectClause { $select : [{ $min : "pingedAt" }] }
        expect(select_clause).toEqual 'min("pingedAt") as "pingedAt"'
        done()

      it "should return properly formatted repository columns for $min", (done)->
        select_clause = @km.selectClause { $select : [{ $min : "col1" }] }
        expect(select_clause).toEqual 'min(properties::hstore->\'col1\') as "col1"'
        done()

      it "should not create an invalid $min select statement for common columns", (done)->
        query_string = @km.getQuery({ $select : [{ $min : "pingedAt" }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

      it "should not create an invalid $min select statement for repository columns", (done)->
        query_string = @km.getQuery({ $select : [{ $min : @km.columns[0] }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

  describe "whereClause", ->
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

    it "should return well formated 'contains' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $contains : "2013-07-06 02:57:09" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "'pingedAt' like '%2013-07-06 02:57:09%'"
      done()

    it "should return well formated 'contains' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $contains : "the test" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "properties->'col1' like '%the test%'"
      done()

    it "should return well formated '>' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $gt : "2013-07-06 02:57:09" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "'pingedAt' > '2013-07-06 02:57:09'"
      done()      

    it "should return well formated '>' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $gt : "the test" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "properties->'col1' > 'the test'"
      done()

    it "should return well formated '>=' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $gte : "2013-07-06 02:57:09" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "'pingedAt' >= '2013-07-06 02:57:09'"
      done()      

    it "should return well formated '>=' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $gte : "the test" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "properties->'col1' >= 'the test'"
      done()

    it "should return well formated '<' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $lt : "2013-07-06 02:57:09" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "'pingedAt' < '2013-07-06 02:57:09'"
      done()      

    it "should return well formated '<' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $lt : "the test" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "properties->'col1' < 'the test'"
      done()

    it "should return well formated '<=' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $lte : "2013-07-06 02:57:09" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "'pingedAt' <= '2013-07-06 02:57:09'"
      done()      

    it "should return well formated '<=' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $lte : "the test" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "properties->'col1' <= 'the test'"
      done()

    it "should return well formated '!=' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $ne : "2013-07-06 02:57:09" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "'pingedAt' != '2013-07-06 02:57:09'"
      done()      

    it "should return well formated '!=' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $ne : "the test" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "properties->'col1' != 'the test'"
      done()

    it "should return well formated 'not null' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $exist : true }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "'pingedAt' not NULL"
      done()

    it "should return well formated 'not null' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $exist : true }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "properties->'col1' not NULL"
      done()

    it "should return well formated 'and' where condition for common column without use of precedence", (done)->
      query_obj = 
        $where : [{
            pingedAt : "2013-07-06 02:57:09"
          },{
            updatedAt : "2013-07-06 02:57:09"
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "'pingedAt' = '2013-07-06 02:57:09' and 'updatedAt' = '2013-07-06 02:57:09'"
      done()    

    it "should return well formated 'and' where condition for common column non nested", (done)->
      query_obj = 
        $where : [{
          $and : [{
              pingedAt : "2013-07-06 02:57:09"
            },{
              updatedAt : "2013-07-06 02:57:09"
          }]
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "('pingedAt' = '2013-07-06 02:57:09' and 'updatedAt' = '2013-07-06 02:57:09')"
      done()

    it "should return well formated 'and' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          $and : [{
              col1 : "the test1"
            },{
              col2 : "the test2"
          }]
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "(properties->'col1' = 'the test1' and properties->'col2' = 'the test2')"
      done()

    it "should return well formated 'or' where condition for common column non nested", (done)->
      query_obj = 
        $where : [{
          $or : [{
              pingedAt : "2013-07-06 02:57:09"
            },{
              updatedAt : "2013-07-06 02:57:09"
          }]
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "('pingedAt' = '2013-07-06 02:57:09' or 'updatedAt' = '2013-07-06 02:57:09')"
      done()

    it "should return well formated 'or' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          $or : [{
              col1 : "the test1"
            },{
              col2 : "the test2"
          }]
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "(properties->'col1' = 'the test1' or properties->'col2' = 'the test2')"
      done()

    it "should return well formated multi nested 'and' where condition for common column non nested", (done)->
      query_obj = 
        $where : [{
          $and : [{
              pingedAt : "2013-07-06 02:57:09"
            },{
              $and : [{
                  updatedAt : "2013-07-06 02:57:09",
                },{
                  createdAt : "2013-07-06 02:57:09",
              }]
          }]
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "('pingedAt' = '2013-07-06 02:57:09' and ('updatedAt' = '2013-07-06 02:57:09' and 'createdAt' = '2013-07-06 02:57:09'))"
      done()

    it "should return well formated multi nested 'or' where condition for common column non nested", (done)->
      query_obj = 
        $where : [{
          $or : [{
              pingedAt : "2013-07-06 02:57:09"
            },{
              $or : [{
                  updatedAt : "2013-07-06 02:57:09",
                },{
                  createdAt : "2013-07-06 02:57:09",
              }]
          }]
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "('pingedAt' = '2013-07-06 02:57:09' or ('updatedAt' = '2013-07-06 02:57:09' or 'createdAt' = '2013-07-06 02:57:09'))"
      done()

    it "should return well formated multi nested 'or' || 'and' where condition for common column non nested", (done)->
      query_obj = 
        $where : [{
          $or : [{
              pingedAt : "2013-07-06 02:57:09"
            },{
              $and : [{
                  updatedAt : "2013-07-06 02:57:09",
                },{
                  createdAt : "2013-07-06 02:57:09",
              }]
          }]
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "('pingedAt' = '2013-07-06 02:57:09' or ('updatedAt' = '2013-07-06 02:57:09' and 'createdAt' = '2013-07-06 02:57:09'))"
      done()    