dateFormat              = require 'dateformat'
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

    @DataSet.hasMany @Krake, { as: "krakes", through: @DataSetKrake}
    @Krake.hasMany @DataSet, { as: "data_sets", through: @DataSetKrake}

    @DataSetKrakeRule.belongsTo @DataSetKrake
    @DataSetKrake.hasMany @DataSetKrakeRule, { as: "data_set_krake_rule", foreignKey: 'data_set_krake_id'}

    promise0 = @dbSystem.sync()

    # Force reset dataSchema table in test database
    promise1 = promise0.then ()=>
      @Krake.sync({force: true})

    promise2 = promise1.then ()=>
      @DataSet.sync({force: true})

    promise3 = promise2.then ()=>
      @DataSetKrake.sync({force: true})

    promise4 = promise3.then ()=>
      # Force reset dataRepository table in test database
      @RecordSets.sync({force: true}).success ()=>

    promise5 = promise4.then ()=>
      @Krake.create({ content: krake_definition, handle: @repo_name})

    promise6 = promise5.then (@krake_obj)=>
      @DataSet.create({ handle: @set_name, name: @set_name })

    promise7 = promise6.then (@dataset_obj)=>
      @dataset_obj.setKrakes [@krake_obj]

    promise8 = promise7.then ()=>
      @ksm = new KrakeSetModel dbSystem, @set_name, [], ()=>
        done()

  describe "simpleColName", ->
    it "should return correct common column name in properly formated timestamp", (done)->
      expect(@ksm.simpleColName "pingedAt").toBe "to_char(\"pingedAt\", 'YYYY-MM-DD HH24:MI:SS')"
      done()

    it "should return correct common column name with auto replacement for double quote", (done)->
      expect(@ksm.simpleColName "pin\"gedAt").toBe "properties::hstore->'pin&#34;gedAt'"
      done()

    it "should return correct common column name with auto replacement for single quote", (done)->
      expect(@ksm.simpleColName "pin'gedAt").toBe "properties::hstore->'pin&#39;gedAt'"
      done()

    it "should return correct repository column name", (done)->
      expect(@ksm.simpleColName "col1").toBe "properties::hstore->'col1'"
      done()

    it "should return correct repository column name with auto replacement for double quote", (done)->
      expect(@ksm.simpleColName "col\"1").toBe "properties::hstore->'col&#34;1'"
      done()

    it "should return correct repository column name with auto replacement for single quote", (done)->
      expect(@ksm.simpleColName "col'1").toBe "properties::hstore->'col&#39;1'"
      done()

  describe "compoundColNameSelect", ->
    it "should return correct common column name", (done)->
      expect(@ksm.compoundColNameSelect "pingedAt").toBe 'to_char("pingedAt", \'YYYY-MM-DD HH24:MI:SS\')'
      done()

    it "should return correct common column name", (done)->
      expect(@ksm.compoundColNameSelect "createdAt").toBe 'to_char("createdAt", \'YYYY-MM-DD HH24:MI:SS\')'
      done()

    it "should return correct common column name", (done)->
      expect(@ksm.compoundColNameSelect "updatedAt").toBe 'to_char("updatedAt", \'YYYY-MM-DD HH24:MI:SS\')'
      done()

    it "should return correct common column name with auto replacement for double quote", (done)->
      expect(@ksm.compoundColNameSelect "pin\"gedAt").toBe "properties::hstore->'pin&#34;gedAt'"
      done()

    it "should return correct common column name with auto replacement for single quote", (done)->
      expect(@ksm.compoundColNameSelect "pin'gedAt").toBe "properties::hstore->'pin&#39;gedAt'"
      done()

    it "should return correct repository column name", (done)->
      expect(@ksm.compoundColNameSelect "col1").toBe "properties::hstore->'col1'"
      done()

    it "should return correct repository column name with auto replacement for double quote", (done)->
      expect(@ksm.compoundColNameSelect "col\"1").toBe "properties::hstore->'col&#34;1'"
      done()

    it "should return correct repository column name with auto replacement for single quote", (done)->
      expect(@ksm.compoundColNameSelect "col'1").toBe "properties::hstore->'col&#39;1'"
      done()

  describe "compoundColNameWhere", ->
    it "should return correct common column name", (done)->
      expect(@ksm.compoundColNameWhere "pingedAt").toBe '"pingedAt"'
      done()

    it "should return correct common column name with auto replacement for double quote", (done)->
      expect(@ksm.compoundColNameWhere "pin\"gedAt").toBe "properties::hstore->'pin&#34;gedAt'"
      done()

    it "should return correct common column name with auto replacement for single quote", (done)->
      expect(@ksm.compoundColNameWhere "pin'gedAt").toBe "properties::hstore->'pin&#39;gedAt'"
      done()

    it "should return correct repository column name", (done)->
      expect(@ksm.compoundColNameWhere "col1").toBe "properties::hstore->'col1'"
      done()

    it "should return correct repository column name with auto replacement for double quote", (done)->
      expect(@ksm.compoundColNameWhere "col\"1").toBe "properties::hstore->'col&#34;1'"
      done()

    it "should return correct repository column name with auto replacement for single quote", (done)->
      expect(@ksm.compoundColNameWhere "col'1").toBe "properties::hstore->'col&#39;1'"
      done()

  describe "getInsertStatement", ->
    it "should call its sub functions", (done)->
      spyOn(@ksm, 'getHstoreValues').andCallThrough()
      query_string = @ksm.getInsertStatement({})
      expect(@ksm.getHstoreValues).toHaveBeenCalled()
      done()

    it "should insert current date for common columns that do not have date indicated", (done)->
      data_obj = 
        "drug bank" : "what to do"
      expect(@ksm.getInsertStatement(data_obj).match("pingedAt").length).toEqual 1
      expect(@ksm.getInsertStatement(data_obj).match("updatedAt").length).toEqual 1
      expect(@ksm.getInsertStatement(data_obj).match("createdAt").length).toEqual 1
      done()      

    it "should insert a record without error", (done)->
      d1 = new Date()
      d1.setDate(10)
      data_obj = 
        "drug bank" : "what to do"
        "pingedAt" : new Date()
        "pingedAt" : new Date()
      insert_query = @ksm.getInsertStatement(data_obj)
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
      insert_query = @ksm.getInsertStatement(data_obj)
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
      insert_query = @ksm.getInsertStatement(data_obj)
      expect(()=>
        @dbRepo.query(insert_query)
      ).not.toThrow()
      done()
      
    it "should insert a record that is retrievable", (done)->
      data_obj = 
        "drug bank" : "what to do"
        "pingedAt" : new Date()
        "pingedAt" : new Date()
      insert_query = @ksm.getInsertStatement(data_obj)
      @dbRepo.query(insert_query).success ()=>
        query_string = @ksm.getSelectStatement { $select : ["drug bank", "pingedAt"] }
        @dbRepo.query(query_string).success (records)->
          expect(records.length).toEqual 1
          expect(records[0]["drug bank"]).toEqual "what to do"
          done()

  describe "getFormattedDate", ->
    it "should ensure formatted date returns date of good and proper format", (done)->
      d = new Date
      control = d.getFullYear() + "-"  +  (d.getMonth() + 1)  + "-" + d.getDate()  + " " + d.getHours() + ":"  +  d.getMinutes()  + ":" + d.getSeconds()
      expect(@ksm.getFormattedDate d).toEqual control
      done()

  describe "getHstoreValues", ->

    it "should ignore values from the common columns", (done)->
      data_obj = 
        "drug bank" : "what to do"
        "pingedAt" : new Date()
        "createdAt" : new Date()
        "updatedAt" : new Date()
      expect(@ksm.getHstoreValues(data_obj)).toEqual '"drug bank" => "what to do"'
      done()

    it "should return a valid insert statement for a single repository column", (done)->
      data_obj = 
        "drug bank" : "what to do"
      expect(@ksm.getHstoreValues(data_obj)).toEqual '"drug bank" => "what to do"'
      done()

    it "should return a valid insert statement for two repository columns", (done)->
      data_obj = 
        "drug bank" : "This is the bank"
        "drug name" : "This is my name"
      expect(@ksm.getHstoreValues(data_obj)).toEqual '"drug bank" => "This is the bank","drug name" => "This is my name"'
      done()

    it "should return false if not values are declared", (done)->
      expect(@ksm.getHstoreValues({})).toEqual false
      done()


  describe "getSelectStatement", =>
    it "should call all the sub clauses", (done)->
      spyOn(@ksm, 'selectClause').andCallThrough()
      spyOn(@ksm, 'whereClause').andCallThrough()
      spyOn(@ksm, 'orderClause').andCallThrough()
      query_string = @ksm.getSelectStatement({})
      expect(()=>  
        @dbRepo.query query_string
      ).not.toThrow()      
      expect(@ksm.selectClause).toHaveBeenCalled()
      expect(@ksm.whereClause).toHaveBeenCalled()
      expect(@ksm.orderClause).toHaveBeenCalled()
      done()

    it "should call all the sub clauses", (done)->
      spyOn(@ksm, 'selectClause').andCallThrough()
      spyOn(@ksm, 'whereClause').andCallThrough()
      query_string = @ksm.getSelectStatement({})
      expect(()=>
        @dbRepo.query query_string
      ).not.toThrow()
      done()

    it "should return the latest batch", (done)->
      d1 = new Date()
      d1.setDate(10)
      d2 = new Date()
      d2.setDate(20)

      promise1 = @RecordSets.create({ properties: "", pingedAt: d1 })
      promise2 = promise1.then @RecordSets.create({ properties: "", pingedAt: d2 })
      promise2.then ()=>
        query_string = @ksm.getSelectStatement { $select : [{ $max : "pingedAt" }] }
        @dbRepo.query(query_string).success (records)->
          expect(records[0].pingedAt).toEqual dateFormat(d2, "UTC:yyyy-mm-dd HH:MM:ss")
          done()

    it "should return the earliest batch", (done)->
      d1 = new Date()
      d1.setDate(10)
      d2 = new Date()
      d2.setDate(20)

      promise1 = @RecordSets.create({ properties: "", pingedAt: d1 })
      promise2 = promise1.then @RecordSets.create({ properties: "", pingedAt: d2 })
      promise2.then ()=>
        query_string = @ksm.getSelectStatement { $select : [{ $min : "pingedAt" }] }
        @dbRepo.query(query_string).success (records)->
          expect(records[0].pingedAt).toEqual dateFormat(d1, "UTC:yyyy-mm-dd HH:MM:ss")
          done()

    it "should get the all the batches", (done)->
      d1 = new Date()
      d1.setDate(10)
      d2 = new Date()
      d2.setDate(20)

      promise1 = @RecordSets.create({ properties: "", pingedAt: d1 })
      promise2 = promise1.then @RecordSets.create({ properties: "", pingedAt: d1 })
      promise3 = promise2.then @RecordSets.create({ properties: "", pingedAt: d2 })
      promise4 = promise3.then @RecordSets.create({ properties: "", pingedAt: d2 })
      promise4.then ()=>
        query_string = @ksm.getSelectStatement { $select : [{ $distinct : "pingedAt" }] }
        @dbRepo.query(query_string).success (records)->
          expect(records.length).toEqual 2
          done()

  describe "selectClause", ->

    it "should return pingedAt, createdAt, updatedAt, datasource_handle by default when there is no select clause ", (done)->
      select_clause = @ksm.selectClause {  }
      
      checksum = 'properties::hstore->\'drug bank\' as "drug bank",'+
        'properties::hstore->\'drug name\' as "drug name",'+
        'properties::hstore->\'categories\' as "categories",'+
        'properties::hstore->\'therapeutic indication\' as "therapeutic indication",'+
        'properties::hstore->\'origin_url\' as "origin_url",properties::hstore->\'origin_pattern\' as "origin_pattern",'+
        'to_char("createdAt", \'YYYY-MM-DD HH24:MI:SS\') as "createdAt",'+
        'to_char("updatedAt", \'YYYY-MM-DD HH24:MI:SS\') as "updatedAt",'+
        'to_char("pingedAt", \'YYYY-MM-DD HH24:MI:SS\') as "pingedAt",'+
        'datasource_handle as "datasource_handle"'

      expect(select_clause).toEqual checksum
      done()

    it "should return pingedAt, createdAt, UpdatedAt by default", (done)->
      select_clause = @ksm.selectClause { $select : [] }
      expect(select_clause).toEqual 'to_char("createdAt", \'YYYY-MM-DD HH24:MI:SS\') as "createdAt",to_char("updatedAt", \'YYYY-MM-DD HH24:MI:SS\') as "updatedAt",to_char("pingedAt", \'YYYY-MM-DD HH24:MI:SS\') as "pingedAt"'
      done()

    it "should not return any duplicated status_col when it is already in the select clause", (done)->
      select_clause = @ksm.selectClause { $select : ["pingedAt"] }
      expect(select_clause).toEqual 'to_char("pingedAt", \'YYYY-MM-DD HH24:MI:SS\') as "pingedAt",to_char("createdAt", \'YYYY-MM-DD HH24:MI:SS\') as "createdAt",to_char("updatedAt", \'YYYY-MM-DD HH24:MI:SS\') as "updatedAt"'

      select_clause = @ksm.selectClause { $select : ["updatedAt"] }
      expect(select_clause).toEqual 'to_char("updatedAt", \'YYYY-MM-DD HH24:MI:SS\') as "updatedAt",to_char("createdAt", \'YYYY-MM-DD HH24:MI:SS\') as "createdAt",to_char("pingedAt", \'YYYY-MM-DD HH24:MI:SS\') as "pingedAt"' 

      select_clause = @ksm.selectClause { $select : ["createdAt"] }
      expect(select_clause).toEqual 'to_char("createdAt", \'YYYY-MM-DD HH24:MI:SS\') as "createdAt",to_char("updatedAt", \'YYYY-MM-DD HH24:MI:SS\') as "updatedAt",to_char("pingedAt", \'YYYY-MM-DD HH24:MI:SS\') as "pingedAt"'
      done()

    it "should return the properly formatted repository columns", (done)->
      select_clause = @ksm.selectClause { $select : ["col1", "col2"] }

      expected_query =  "properties::hstore->'col1' as \"col1\"," +
                        "properties::hstore->'col2' as \"col2\"," +
                        "to_char(\"createdAt\", 'YYYY-MM-DD HH24:MI:SS') as \"createdAt\"," +
                        "to_char(\"updatedAt\", 'YYYY-MM-DD HH24:MI:SS') as \"updatedAt\"," +
                        "to_char(\"pingedAt\", 'YYYY-MM-DD HH24:MI:SS') as \"pingedAt\""
      expect(select_clause).toEqual expected_query
      done()

    it "should not create an invalid normal select statement", (done)->
      query_string = @ksm.getSelectStatement({})
      expect(()=>  
        @dbRepo.query query_string
      ).not.toThrow()
      done()


    describe "$count", ->
      it "should return properly formatted common columns for $count", (done)->
        select_clause = @ksm.selectClause { $select : [{ $count : "pingedAt" }] }
        expect(select_clause).toEqual 'count(to_char("pingedAt", \'YYYY-MM-DD HH24:MI:SS\')) as "pingedAt"'
        done()

      it "should return properly formatted repository columns for $count", (done)->
        select_clause = @ksm.selectClause { $select : [{ $count : "col1" }] }
        expect(select_clause).toEqual 'count(properties::hstore->\'col1\') as "col1"'
        done()

      it "should not create an invalid $count select statement for common columns", (done)->
        query_string = @ksm.getSelectStatement({ $select : [{ $count : "pingedAt" }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

      it "should not create an invalid $count select statement for repository columns", (done)->
        query_string = @ksm.getSelectStatement({ $select : [{ $count : @ksm.columns[0] }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

    describe "$distinct", ->
      it "should return properly formatted common columns for $distinct", (done)->
        select_clause = @ksm.selectClause { $select : [{ $distinct : "pingedAt" }] }
        expect(select_clause).toEqual 'distinct cast(to_char("pingedAt", \'YYYY-MM-DD HH24:MI:SS\') as text) as "pingedAt"'
        done()

      it "should return properly formatted repository columns for $distinct", (done)->
        select_clause = @ksm.selectClause { $select : [{ $distinct : "col1" }] }
        expect(select_clause).toEqual 'distinct cast(properties::hstore->\'col1\' as text) as "col1"'
        done()

      it "should not create an invalid $distinct select statement for common columns", (done)->
        query_string = @ksm.getSelectStatement({ $select : [{ $distinct : "pingedAt" }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

      it "should not create an invalid $distinct select statement for repository columns", (done)->
        query_string = @ksm.getSelectStatement({ $select : [{ $distinct : @ksm.columns[0] }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

    describe "$max", ->
      it "should return properly formatted common columns for $max", (done)->
        select_clause = @ksm.selectClause { $select : [{ $max : "pingedAt" }] }
        expect(select_clause).toEqual 'max(to_char("pingedAt", \'YYYY-MM-DD HH24:MI:SS\')) as "pingedAt"'
        done()

      it "should return properly formatted repository columns for $max", (done)->
        select_clause = @ksm.selectClause { $select : [{ $max : "col1" }] }
        expect(select_clause).toEqual 'max(properties::hstore->\'col1\') as "col1"'
        done()

      it "should not create an invalid $distinct select statement for common columns", (done)->
        query_string = @ksm.getSelectStatement({ $select : [{ $max : "pingedAt" }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

      it "should not create an invalid $distinct select statement for repository columns", (done)->
        query_string = @ksm.getSelectStatement({ $select : [{ $max : @ksm.columns[0] }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

    describe "$min", ->
      it "should return properly formatted common columns for $min", (done)->
        select_clause = @ksm.selectClause { $select : [{ $min : "pingedAt" }] }
        expect(select_clause).toEqual 'min(to_char("pingedAt", \'YYYY-MM-DD HH24:MI:SS\')) as "pingedAt"'
        done()

      it "should return properly formatted repository columns for $min", (done)->
        select_clause = @ksm.selectClause { $select : [{ $min : "col1" }] }
        expect(select_clause).toEqual 'min(properties::hstore->\'col1\') as "col1"'
        done()

      it "should not create an invalid $min select statement for common columns", (done)->
        query_string = @ksm.getSelectStatement({ $select : [{ $min : "pingedAt" }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

      it "should not create an invalid $min select statement for repository columns", (done)->
        query_string = @ksm.getSelectStatement({ $select : [{ $min : @ksm.columns[0] }] })
        expect(()=>  
          @dbRepo.query query_string
        ).not.toThrow()
        done()

  describe "hasAggregate", ->
    it "should return false when has no $select clause", (done)->
      query_obj = { }
      expect(@ksm.hasAggregate query_obj).toEqual false
      done()

    it "should return false when has no aggregated selects", (done)->
      query_obj = { $select : [ @ksm.columns[0]] }
      expect(@ksm.hasAggregate query_obj).toEqual false
      done()

    it "should return true when has $count", (done)->
      query_obj = { $select : [{ $count : @ksm.columns[0] }] }
      expect(@ksm.hasAggregate query_obj).toEqual true
      done()

    it "should return true when has $distinct", (done)->
      query_obj = { $select : [{ $distinct : @ksm.columns[0] }] }
      expect(@ksm.hasAggregate query_obj).toEqual true
      done()

    it "should return true when has $max", (done)->
      query_obj = { $select : [{ $max : @ksm.columns[0] }] }
      expect(@ksm.hasAggregate query_obj).toEqual true
      done()

    it "should return true when has $min", (done)->
      query_obj = { $select : [{ $max : @ksm.columns[0] }] }
      expect(@ksm.hasAggregate query_obj).toEqual true
      done()

  describe "whereClause", ->

    it "should not return any where condition if there are not where conditions in query_obj", (done)->
      where_clause = @ksm.whereClause {}
      expect(where_clause).toEqual false
      done()

    it "should return false when $where is an empty array", (done)->
      where_clause = @ksm.whereClause {}
      expect(where_clause).toEqual false
      done()

    it "should return well formated '=' where condition for common column", (done)->
      query_obj = 
        $where : [{ 
          pingedAt : "2013-07-06 02:57:09"
        }]
      where_clause = @ksm.whereClause query_obj
      expect(where_clause).toEqual "\"pingedAt\" = '2013-07-06 02:57:09'"
      done()

    it "should return well formated '=' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{ 
          col1 : "the test"
        }]
      where_clause = @ksm.whereClause query_obj
      expect(where_clause).toEqual "properties::hstore->'col1' = 'the test'"
      done()

    it "should return well formated 'contains' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $contains : "2013-07-06 02:57:09" }
        }]
      where_clause = @ksm.whereClause query_obj
      expect(where_clause).toEqual "\"pingedAt\" like '%2013-07-06 02:57:09%'"
      done()

    it "should return well formated 'contains' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $contains : "the test" }
        }]
      where_clause = @ksm.whereClause query_obj
      expect(where_clause).toEqual "properties::hstore->'col1' like '%the test%'"
      done()

    it "should return well formated '>' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $gt : "2013-07-06 02:57:09" }
        }]
      where_clause = @ksm.whereClause query_obj
      expect(where_clause).toEqual "\"pingedAt\" > '2013-07-06 02:57:09'"
      done()      

    it "should return well formated '>' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $gt : "the test" }
        }]
      where_clause = @ksm.whereClause query_obj
      expect(where_clause).toEqual "properties::hstore->'col1' > 'the test'"
      done()

    it "should return well formated '>=' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $gte : "2013-07-06 02:57:09" }
        }]
      where_clause = @ksm.whereClause query_obj
      expect(where_clause).toEqual "\"pingedAt\" >= '2013-07-06 02:57:09'"
      done()      

    it "should return well formated '>=' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $gte : "the test" }
        }]
      where_clause = @ksm.whereClause query_obj
      expect(where_clause).toEqual "properties::hstore->'col1' >= 'the test'"
      done()

    it "should return well formated '<' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $lt : "2013-07-06 02:57:09" }
        }]
      where_clause = @ksm.whereClause query_obj
      expect(where_clause).toEqual "\"pingedAt\" < '2013-07-06 02:57:09'"
      done()      

    it "should return well formated '<' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $lt : "the test" }
        }]
      where_clause = @ksm.whereClause query_obj
      expect(where_clause).toEqual "properties::hstore->'col1' < 'the test'"
      done()

    it "should return well formated '<=' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $lte : "2013-07-06 02:57:09" }
        }]
      where_clause = @ksm.whereClause query_obj
      expect(where_clause).toEqual "\"pingedAt\" <= '2013-07-06 02:57:09'"
      done()      

    it "should return well formated '<=' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $lte : "the test" }
        }]
      where_clause = @ksm.whereClause query_obj
      expect(where_clause).toEqual "properties::hstore->'col1' <= 'the test'"
      done()

    it "should return well formated '!=' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $ne : "2013-07-06 02:57:09" }
        }]
      where_clause = @ksm.whereClause query_obj
      expect(where_clause).toEqual "\"pingedAt\" != '2013-07-06 02:57:09'"
      done()      

    it "should return well formated '!=' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $ne : "the test" }
        }]
      where_clause = @ksm.whereClause query_obj
      expect(where_clause).toEqual "properties::hstore->'col1' != 'the test'"
      done()

    it "should return well formated 'not null' where condition for common column", (done)->
      query_obj = 
        $where : [{
          pingedAt : { $exist : true }
        }]
      where_clause = @ksm.whereClause query_obj
      expect(where_clause).toEqual "\"pingedAt\" not NULL"
      done()

    it "should return well formated 'not null' where condition for repository specific column", (done)->
      query_obj = 
        $where : [{
          col1 : { $exist : true }
        }]
      where_clause = @ksm.whereClause query_obj
      expect(where_clause).toEqual "properties::hstore->'col1' not NULL"
      done()

    it "should return well formated 'and' where condition for common column without use of precedence", (done)->
      query_obj = 
        $where : [{
            pingedAt : "2013-07-06 02:57:09"
          },{
            updatedAt : "2013-07-06 02:57:09"
        }]
      where_clause = @ksm.whereClause query_obj
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
      where_clause = @ksm.whereClause query_obj
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
      where_clause = @ksm.whereClause query_obj
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
      where_clause = @ksm.whereClause query_obj
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
      where_clause = @ksm.whereClause query_obj
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
      where_clause = @ksm.whereClause query_obj
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
      where_clause = @ksm.whereClause query_obj
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
      where_clause = @ksm.whereClause query_obj
      expect(where_clause).toEqual "(\"pingedAt\" = '2013-07-06 02:57:09' or (\"updatedAt\" = '2013-07-06 02:57:09' and \"createdAt\" = '2013-07-06 02:57:09'))"
      done()

  describe "orderClause", ->
    it "should not return ordering conditions when $order is missing", (done)->
      query_obj = {}
      order_clause = @ksm.orderClause query_obj
      expect(order_clause).toEqual false
      done()

    it "should not return ordering conditions when $order is an empty array", (done)->
      query_obj = { $order : [] }
      order_clause = @ksm.orderClause query_obj
      expect(order_clause).toEqual false
      done()

    it "should return well formatted ordering for common columns", (done)->
      query_obj = 
        $order : [{
          $asc : "pingedAt"
        }]
      order_clause = @ksm.orderClause query_obj
      expect(order_clause).toEqual '"pingedAt" asc'
      done()

    it "should return well formatted ordering for repository specific columns", (done)->
      query_obj = 
        $order : [{
          $asc : "drug bank"
        }]
      order_clause = @ksm.orderClause query_obj
      expect(order_clause).toEqual "properties::hstore->'drug bank' asc"
      done()

    it "should return well formatted compound ordering", (done)->
      query_obj = 
        $order : [{
            $asc : "pingedAt"
          },{
            $desc : "drug bank"
        }]
      order_clause = @ksm.orderClause query_obj
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
      query_string = @ksm.getSelectStatement query_obj
      expect(()=>
        @dbRepo.query query_string
      ).not.toThrow()
      done()

  describe "limitClause", ->
    it "should return a limit for 10 and not cause an error", (done)->
      query_obj = 
        $select : ["drug bank"]
        $limit : 10
      query_string = @ksm.getSelectStatement query_obj
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
      queries = []
      queries.push @ksm.getInsertStatement(data_obj)
      queries.push @ksm.getInsertStatement(data_obj)
      queries.push @ksm.getInsertStatement(data_obj)
      @dbRepo.query(queries.join(";")).then ()=>
        query_string = @ksm.getSelectStatement { $select : ["drug bank", "pingedAt"], $limit : 1 }
        @dbRepo.query(query_string).success (records)->
          expect(records.length).toEqual 1
          done()

  describe "offsetClause", ->
    it "should return an offset of 10 and not cause an error", (done)->
      query_obj = 
        $select : ["drug bank"]
        $offset : 10
      query_string = @ksm.getSelectStatement query_obj
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

      queries = []
      queries.push @ksm.getInsertStatement(data_obj1)
      queries.push @ksm.getInsertStatement(data_obj2)
      queries.push @ksm.getInsertStatement(data_obj3)
      queries.push @ksm.getInsertStatement(data_obj1)
      @dbRepo.query(queries.join(";")).then ()=>
        query_string = @ksm.getSelectStatement { $select : ["drug bank", "pingedAt"], $limit : 1, $offset : 0 }
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

      queries = []
      queries.push @ksm.getInsertStatement(data_obj1)
      queries.push @ksm.getInsertStatement(data_obj2)
      queries.push @ksm.getInsertStatement(data_obj3)
      queries.push @ksm.getInsertStatement(data_obj1)
      @dbRepo.query(queries.join(";")).then ()=>
        query_string = @ksm.getSelectStatement { $select : ["drug bank", "pingedAt"], $limit : 1, $offset : 4 }
        @dbRepo.query(query_string).success (records)->
          expect(records.length).toBe 0
          done()