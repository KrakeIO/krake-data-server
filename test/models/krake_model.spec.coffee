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
KrakeModel = require '../../models/krake_model'
krake_definition = fs.readFileSync(__dirname + '/../fixtures/krake_definition.json').toString()

describe "KrakeModel", ->

  beforeEach (done)->
    @dbRepo = dbRepo
    @repo_name = "1_66240a39bc8c73a3ec2a08222936fc49eses"
    @Krake = dbSystem.define 'krakes', krakeSchema

    # Force reset dataSchema table in test database
    promise1 = @Krake.sync({force: true})
    promise2 = promise1.then ()=>
      @Krake.create({ content: krake_definition, handle: @repo_name})

    promise3 = promise2.then ()=>
      # Force reset dataRepository table in test database
      @Records = dbRepo.define @repo_name, recordBody  
      @Records.sync({force: true}).success ()=>

        # instantiates a krake model
        @km = new KrakeModel dbSystem, @repo_name, ()->
          done()

  it "should return status as true when repository is valid", (done)->
    km = new KrakeModel dbSystem, @repo_name, (status, error_msg)->
      expect(status).toEqual true
      expect(error_msg).toEqual null
      done()

  it "should return status as false when repository is invalid", (done)->
    km = new KrakeModel dbSystem, "invalid table name", (status, error_msg)->
      expect(status).toEqual false
      expect(error_msg).toEqual 'Sorry. The data repository you were looking for does not exist'
      done()

  it "should have columns", (done)->
    km = new KrakeModel dbSystem, @repo_name, (success, error_msg)->
      expect(km.columns.length).toEqual 6
      done()

  describe "colName", ->
    it "should return correct common column name", (done)->
      expect(@km.colName "pingedAt").toBe '"pingedAt"'
      done()

    it "should return correct common column name with auto replacement for double quote", (done)->
      expect(@km.colName "pin\"gedAt").toBe "properties::hstore->'pin&#34;gedAt'"
      done()

    it "should return correct common column name with auto replacement for single quote", (done)->
      expect(@km.colName "pin'gedAt").toBe "properties::hstore->'pin&#39;gedAt'"
      done()

    it "should return correct repository column name", (done)->
      expect(@km.colName "col1").toBe "properties::hstore->'col1'"
      done()

    it "should return correct repository column name with auto replacement for double quote", (done)->
      expect(@km.colName "col\"1").toBe "properties::hstore->'col&#34;1'"
      done()

    it "should return correct repository column name with auto replacement for single quote", (done)->
      expect(@km.colName "col'1").toBe "properties::hstore->'col&#39;1'"
      done()

  describe "getInsertStatement", ->
    it "should call its sub functions", (done)->
      spyOn(@km, 'getHstoreValues').andCallThrough()
      query_string = @km.getInsertStatement({})
      expect(@km.getHstoreValues).toHaveBeenCalled()
      done()

    it "should insert current date for common columns that do not have date indicated", (done)->
      data_obj = 
        "drug bank" : "what to do"
      expect(@km.getInsertStatement(data_obj).match("pingedAt").length).toEqual 1
      expect(@km.getInsertStatement(data_obj).match("updatedAt").length).toEqual 1
      expect(@km.getInsertStatement(data_obj).match("createdAt").length).toEqual 1
      done()      

    it "should insert a record without error", (done)->
      d1 = new Date()
      d1.setDate(10)
      data_obj = 
        "drug bank" : "what to do"
        "pingedAt" : new Date()
        "pingedAt" : new Date()
      insert_query = @km.getInsertStatement(data_obj)
      expect(()=>
        @dbRepo.query(insert_query)
      ).not.toThrow()
      done()

    it "should insert a record without error when value has single quote by replacing it with &#39;", (done)->
      d1 = new Date()
      d1.setDate(10)
      data_obj = 
        "drug \'bank" : "what 'to do"
        "pingedAt" : new Date()
        "pingedAt" : new Date()
      insert_query = @km.getInsertStatement(data_obj)
      expect(()=>
        @dbRepo.query(insert_query)
      ).not.toThrow()
      done()

    it "should insert a record without error when value has double quote", (done)->
      d1 = new Date()
      d1.setDate(10)
      data_obj = 
        "drug \"bank" : "what \"to do"
        "pingedAt" : new Date()
        "pingedAt" : new Date()
      insert_query = @km.getInsertStatement(data_obj)
      expect(()=>
        @dbRepo.query(insert_query)
      ).not.toThrow()
      done()
      
    it "should insert a record that is retrievable", (done)->
      data_obj = 
        "drug bank" : "what to do"
        "pingedAt" : new Date()
        "pingedAt" : new Date()
      insert_query = @km.getInsertStatement(data_obj)
      @dbRepo.query(insert_query).success ()=>
        query_string = @km.getSelectStatement { $select : ["drug bank", "pingedAt"] }
        @dbRepo.query(query_string).success (records)->
          expect(records.length).toEqual 1
          expect(records[0]["drug bank"]).toEqual "what to do"
          done()

  describe "getFormattedDate", ->
    it "should ensure formatted date returns date of good and proper format", (done)->
      d = new Date
      control = d.getFullYear() + "-"  +  (d.getMonth() + 1)  + "-" + d.getDate()  + " " + d.getHours() + ":"  +  d.getMinutes()  + ":" + d.getSeconds()
      expect(@km.getFormattedDate d).toEqual control
      done()

  describe "getHstoreValues", ->

    it "should ignore values from the common columns", (done)->
      data_obj = 
        "drug bank" : "what to do"
        "pingedAt" : new Date()
        "createdAt" : new Date()
        "updatedAt" : new Date()
      expect(@km.getHstoreValues(data_obj)).toEqual '"drug bank" => "what to do"'
      done()

    it "should return a valid insert statement for a single repository column", (done)->
      data_obj = 
        "drug bank" : "what to do"
      expect(@km.getHstoreValues(data_obj)).toEqual '"drug bank" => "what to do"'
      done()

    it "should return a valid insert statement for two repository columns", (done)->
      data_obj = 
        "drug bank" : "This is the bank"
        "drug name" : "This is my name"
      expect(@km.getHstoreValues(data_obj)).toEqual '"drug bank" => "This is the bank","drug name" => "This is my name"'
      done()

    it "should return false if not values are declared", (done)->
      expect(@km.getHstoreValues({})).toEqual false
      done()


  describe "getSelectStatement", =>
    it "should call all the sub clauses", (done)->
      spyOn(@km, 'selectClause').andCallThrough()
      spyOn(@km, 'whereClause').andCallThrough()
      spyOn(@km, 'orderClause').andCallThrough()
      query_string = @km.getSelectStatement({})
      expect(()=>  
        @dbRepo.query query_string
      ).not.toThrow()      
      expect(@km.selectClause).toHaveBeenCalled()
      expect(@km.whereClause).toHaveBeenCalled()
      expect(@km.orderClause).toHaveBeenCalled()
      done()

    it "should call all the sub clauses", (done)->
      spyOn(@km, 'selectClause').andCallThrough()
      spyOn(@km, 'whereClause').andCallThrough()
      query_string = @km.getSelectStatement({})
      expect(()=>
        @dbRepo.query query_string
      ).not.toThrow()
      done()

    it "should return the latest batch", (done)->
      d1 = new Date()
      d1.setDate(10)
      d2 = new Date()
      d2.setDate(20)

      promise1 = @Records.create({ properties: "", pingedAt: d1 })
      promise2 = promise1.then @Records.create({ properties: "", pingedAt: d2 })
      promise2.then ()=>
        query_string = @km.getSelectStatement { $select : [{ $max : "pingedAt" }] }
        @dbRepo.query(query_string).success (records)->
          expect(records[0].pingedAt).toEqual d2
          done()

    it "should return the earliest batch", (done)->
      d1 = new Date()
      d1.setDate(10)
      d2 = new Date()
      d2.setDate(20)

      promise1 = @Records.create({ properties: "", pingedAt: d1 })
      promise2 = promise1.then @Records.create({ properties: "", pingedAt: d2 })
      promise2.then ()=>
        query_string = @km.getSelectStatement { $select : [{ $min : "pingedAt" }] }
        @dbRepo.query(query_string).success (records)->
          expect(records[0].pingedAt).toEqual d1
          done()

    it "should get the all the batches", (done)->
      d1 = new Date()
      d1.setDate(10)
      d2 = new Date()
      d2.setDate(20)

      promise1 = @Records.create({ properties: "", pingedAt: d1 })
      promise2 = promise1.then @Records.create({ properties: "", pingedAt: d1 })
      promise3 = promise2.then @Records.create({ properties: "", pingedAt: d2 })
      promise4 = promise3.then @Records.create({ properties: "", pingedAt: d2 })
      promise4.then ()=>
        query_string = @km.getSelectStatement { $select : [{ $distinct : "pingedAt" }] }
        @dbRepo.query(query_string).success (records)->
          expect(records.length).toEqual 2
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
      query_string = @km.getSelectStatement({})
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
        query_string = @km.getSelectStatement({ $select : [{ $count : "pingedAt" }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

      it "should not create an invalid $count select statement for repository columns", (done)->
        query_string = @km.getSelectStatement({ $select : [{ $count : @km.columns[0] }] })
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
        query_string = @km.getSelectStatement({ $select : [{ $distinct : "pingedAt" }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

      it "should not create an invalid $distinct select statement for repository columns", (done)->
        query_string = @km.getSelectStatement({ $select : [{ $distinct : @km.columns[0] }] })
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
        query_string = @km.getSelectStatement({ $select : [{ $max : "pingedAt" }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

      it "should not create an invalid $distinct select statement for repository columns", (done)->
        query_string = @km.getSelectStatement({ $select : [{ $max : @km.columns[0] }] })
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
        query_string = @km.getSelectStatement({ $select : [{ $min : "pingedAt" }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

      it "should not create an invalid $min select statement for repository columns", (done)->
        query_string = @km.getSelectStatement({ $select : [{ $min : @km.columns[0] }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

  describe "whereClause", ->

    it "should not return any where condition if there are not where conditions in query_obj", (done)->
      where_clause = @km.whereClause {}
      expect(where_clause).toEqual false
      done()

    it "should return false when $where is an empty array", (done)->
      where_clause = @km.whereClause {}
      expect(where_clause).toEqual false
      done()

    it "should return well formated '=' where condition for common column", (done)->
      query_obj = 
        $where : [{ 
          pingedAt : "2013-07-06 02:57:09"
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "\"pingedAt\" = '2013-07-06 02:57:09'"
      done()

    it "should return well formated '=' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{ 
          col1 : "the test"
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "properties::hstore->'col1' = 'the test'"
      done()

    it "should return well formated 'contains' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $contains : "2013-07-06 02:57:09" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "\"pingedAt\" like '%2013-07-06 02:57:09%'"
      done()

    it "should return well formated 'contains' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $contains : "the test" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "properties::hstore->'col1' like '%the test%'"
      done()

    it "should return well formated '>' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $gt : "2013-07-06 02:57:09" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "\"pingedAt\" > '2013-07-06 02:57:09'"
      done()      

    it "should return well formated '>' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $gt : "the test" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "properties::hstore->'col1' > 'the test'"
      done()

    it "should return well formated '>=' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $gte : "2013-07-06 02:57:09" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "\"pingedAt\" >= '2013-07-06 02:57:09'"
      done()      

    it "should return well formated '>=' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $gte : "the test" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "properties::hstore->'col1' >= 'the test'"
      done()

    it "should return well formated '<' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $lt : "2013-07-06 02:57:09" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "\"pingedAt\" < '2013-07-06 02:57:09'"
      done()      

    it "should return well formated '<' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $lt : "the test" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "properties::hstore->'col1' < 'the test'"
      done()

    it "should return well formated '<=' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $lte : "2013-07-06 02:57:09" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "\"pingedAt\" <= '2013-07-06 02:57:09'"
      done()      

    it "should return well formated '<=' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $lte : "the test" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "properties::hstore->'col1' <= 'the test'"
      done()

    it "should return well formated '!=' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $ne : "2013-07-06 02:57:09" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "\"pingedAt\" != '2013-07-06 02:57:09'"
      done()      

    it "should return well formated '!=' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $ne : "the test" }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "properties::hstore->'col1' != 'the test'"
      done()

    it "should return well formated 'not null' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $exist : true }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "\"pingedAt\" not NULL"
      done()

    it "should return well formated 'not null' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $exist : true }
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "properties::hstore->'col1' not NULL"
      done()

    it "should return well formated 'and' where condition for common column without use of precedence", (done)->
      query_obj = 
        $where : [{
            pingedAt : "2013-07-06 02:57:09"
          },{
            updatedAt : "2013-07-06 02:57:09"
        }]
      where_clause = @km.whereClause query_obj
      expect(where_clause).toEqual "\"pingedAt\" = '2013-07-06 02:57:09' and \"updatedAt\" = '2013-07-06 02:57:09'"
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
      expect(where_clause).toEqual "(\"pingedAt\" = '2013-07-06 02:57:09' and \"updatedAt\" = '2013-07-06 02:57:09')"
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
      expect(where_clause).toEqual "(properties::hstore->'col1' = 'the test1' and properties::hstore->'col2' = 'the test2')"
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
      expect(where_clause).toEqual "(\"pingedAt\" = '2013-07-06 02:57:09' or \"updatedAt\" = '2013-07-06 02:57:09')"
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
      expect(where_clause).toEqual "(properties::hstore->'col1' = 'the test1' or properties::hstore->'col2' = 'the test2')"
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
      expect(where_clause).toEqual "(\"pingedAt\" = '2013-07-06 02:57:09' and (\"updatedAt\" = '2013-07-06 02:57:09' and \"createdAt\" = '2013-07-06 02:57:09'))"
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
      expect(where_clause).toEqual "(\"pingedAt\" = '2013-07-06 02:57:09' or (\"updatedAt\" = '2013-07-06 02:57:09' or \"createdAt\" = '2013-07-06 02:57:09'))"
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
      expect(where_clause).toEqual "(\"pingedAt\" = '2013-07-06 02:57:09' or (\"updatedAt\" = '2013-07-06 02:57:09' and \"createdAt\" = '2013-07-06 02:57:09'))"
      done()

  describe "orderClause", ->
    it "should not return ordering conditions when $order is missing", (done)->
      query_obj = {}
      order_clause = @km.orderClause query_obj
      expect(order_clause).toEqual false
      done()

    it "should not return ordering conditions when $order is an empty array", (done)->
      query_obj = { $order : [] }
      order_clause = @km.orderClause query_obj
      expect(order_clause).toEqual false
      done()

    it "should return well formatted ordering for common columns", (done)->
      query_obj = 
        $order : [{
          $asc : "pingedAt"
        }]
      order_clause = @km.orderClause query_obj
      expect(order_clause).toEqual '"pingedAt" asc'
      done()

    it "should return well formatted ordering for repository specific columns", (done)->
      query_obj = 
        $order : [{
          $asc : "drug bank"
        }]
      order_clause = @km.orderClause query_obj
      expect(order_clause).toEqual "properties::hstore->'drug bank' asc"
      done()

    it "should return well formatted compound ordering", (done)->
      query_obj = 
        $order : [{
            $asc : "pingedAt"
          },{
            $desc : "drug bank"
        }]
      order_clause = @km.orderClause query_obj
      expect(order_clause).toEqual '"pingedAt" asc,properties::hstore->\'drug bank\' desc'
      done()

    it "should return well formatted ordering that does not cause an error", (done)->
      query_obj = 
        $select : ["pingedAt"]
        $order : [{
            $asc : "pingedAt"
          },{
            $desc : "drug bank"          
        }]
      query_string = @km.getSelectStatement query_obj
      expect(()=>
        @dbRepo.query query_string
      ).not.toThrow()
      done()

  describe "limitClause", ->
    it "should return a limit for 10 and not cause an error", (done)->
      query_obj = 
        $select : ["drug bank"]
        $limit : 10
      query_string = @km.getSelectStatement query_obj
      expect(query_string.match("LIMIT 10").length).toEqual 1
      expect(()=>
        @dbRepo.query query_string
      ).not.toThrow()
      done()

      
    it "should insert a record that is retrievable", (done)->
      data_obj = 
        "drug bank" : "what to do"
        "pingedAt" : new Date()
        "pingedAt" : new Date()
      insert_query = @km.getInsertStatement(data_obj)
      promise1 = @dbRepo.query(insert_query)
      promise2 = promise1.then @dbRepo.query(insert_query)
      promise3 = promise2.then @dbRepo.query(insert_query)
      promise3.then ()=>
        query_string = @km.getSelectStatement { $select : ["drug bank", "pingedAt"], $limit : 1 }
        @dbRepo.query(query_string).success (records)->
          expect(records.length).toEqual 1
          done()

  describe "offsetClause", ->
    it "should return an offset of 10 and not cause an error", (done)->
      query_obj = 
        $select : ["drug bank"]
        $offset : 10
      query_string = @km.getSelectStatement query_obj
      expect(query_string.match("OFFSET 10").length).toEqual 1
      expect(()=>
        @dbRepo.query query_string
      ).not.toThrow()
      done()


    it "should insert a record that is retrievable", (done)->
      data_obj1 = 
        "drug bank" : "what to do"

      data_obj2 = 
        "drug bank" : "what to do again"

      data_obj3 = 
        "drug bank" : "what to do again and again"

      insert_query1 = @km.getInsertStatement(data_obj1)
      insert_query2 = @km.getInsertStatement(data_obj2)
      insert_query3 = @km.getInsertStatement(data_obj3)

      promise1 = @dbRepo.query(insert_query1)
      promise2 = promise1.then @dbRepo.query(insert_query2)
      promise3 = promise2.then @dbRepo.query(insert_query3)
      promise4 = promise3.then @dbRepo.query(insert_query1)
      promise4.then ()=>
        query_string = @km.getSelectStatement { $select : ["drug bank", "pingedAt"], $limit : 1, $offset : 0 }
        @dbRepo.query(query_string).success (records)->
          expect(records.length).toEqual 1
          done()

    it "should insert a record that is retrievable", (done)->
      data_obj1 = 
        "drug bank" : "what to do"

      data_obj2 = 
        "drug bank" : "what to do again"

      data_obj3 = 
        "drug bank" : "what to do again and again"

      insert_query1 = @km.getInsertStatement(data_obj1)
      insert_query2 = @km.getInsertStatement(data_obj2)
      insert_query3 = @km.getInsertStatement(data_obj3)

      promise1 = @dbRepo.query(insert_query1)
      promise2 = promise1.then @dbRepo.query(insert_query2)
      promise3 = promise2.then @dbRepo.query(insert_query3)
      promise4 = promise3.then @dbRepo.query(insert_query1)
      promise4.then ()=>
        query_string = @km.getSelectStatement { $select : ["drug bank", "pingedAt"], $limit : 1, $offset : 4 }
        @dbRepo.query(query_string).success (records)->
          expect(records.length).toBe 0
          done()