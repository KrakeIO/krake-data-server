crypto = require 'crypto'
fs = require 'fs'
rimraf = require 'rimraf'
kson = require 'kson'
Sequelize = require 'sequelize'
recordBody = require('krake-toolkit').schema.record
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

dbRepo = new Sequelize CONFIG.postgres.database, userName, password, options

options["define"]=
  underscored: true
dbSystem = new Sequelize CONFIG.userDataDB, userName, password, options

CacheController = require '../../controllers/cache_controller'
KrakeModel = require '../../models/krake_model'
fixture = fs.readFileSync(__dirname + '/../fixtures/krake_definition.json').toString()


describe "CacheController with KrakeModel", ->
  beforeEach (done) ->
    @dbRepo = dbRepo
    @repo_name = "1_66240a39bc8c73a3ec2a08222936fc49eses"
    @test_folder = "/tmp/test_folder/"
    rimraf.sync @test_folder
    @repo_name = "test_tables"
    
    @Krake = dbSystem.define 'krakes', krakeSchema 
    @Krake.sync({force: true}).success ()=>
      @Krake.create({ content: fixture, handle: @repo_name})   
      @cm = new CacheController @test_folder, dbRepo, recordBody
      @Records = dbRepo.define @repo_name, recordBody  
      @Records.sync({force: true}).success ()=>
        @km = new KrakeModel dbSystem, @repo_name, [], (success, error_msg)->
          done()

  afterEach (done)->
    rimraf.sync @test_folder
    done()

  it "should create a cache folder if it does not exist", (done)->
    fs.rmdirSync(@test_folder) if fs.existsSync(@test_folder)
    expect(fs.existsSync @test_folder).toBe false
    cm = new CacheController @test_folder, dbRepo, recordBody
    expect(fs.existsSync @test_folder).toBe true
    done()

  describe "getCacheKey", ->
    it "should return valid cacheKey", (done)->
      query_string = ""
      expect(@cm.getCacheKey @repo_name, query_string).toEqual(@repo_name + "_" + crypto.createHash('md5').update(query_string).digest("hex"))
      done()

    it "should return valid cacheKey", (done)->
      query_string = ""
      expect(@cm.getCacheKey @repo_name, query_string).toEqual(@repo_name + "_" + crypto.createHash('md5').update(query_string).digest("hex"))
      done()      

  describe "generateCache", ->
    it "should generate cache without error", (done)->
      query = @km.getSelectStatement { $select : [{ $max : "pingedAt" }] }
      @cm.generateCache(@repo_name, null, [], [], query, "json", (error)=>
        expect(error).toEqual(null)
        done()
      )

    it "should generate cache in folder location", (done)->
      query_string = @km.getSelectStatement { $select : [{ $max : "pingedAt" }] }
      format = 'json'
      cache_name = @cm.getCacheKey @repo_name, query_string        
      path_to_file = @test_folder + cache_name + '.' + format
      @cm.generateCache @repo_name, null, [], [], query_string, format, (error)=>
        expect(fs.existsSync(path_to_file)).toBe true
        done()

    it "should generate cache that is valid JSON format", (done)->
      format = 'json'      
      data_obj1 = 
        "drug bank" : "cache that stuff"
      query_string = @km.getSelectStatement { $select : ["drug bank", "pingedAt"] }
      cache_name = @cm.getCacheKey @repo_name, query_string
      path_to_file = @test_folder + cache_name + '.' + format

      insert_query1 = @km.getInsertStatement(data_obj1)
      promise1 = @dbRepo.query(insert_query1)
      promise1.then ()=>

        @cm.generateCache @repo_name, null, [], [], query_string, format, (error)=>
          expect(fs.existsSync path_to_file).toBe true
          expect(()=>
            JSON.parse fs.readFileSync(path_to_file)
          ).not.toThrow()

          data_obj = JSON.parse fs.readFileSync(path_to_file)
          expect(data_obj[0]['drug bank']).toEqual "cache that stuff"
          done()


    it "should generate cache that is valid JSON format", (done)->
      format = 'json'      
      data_obj1 = 
        "drug \"bank" : "cache \"that double quote"
        "drug \'name" : "cache \'that single quote"
      query_string = @km.getSelectStatement { $select : ["drug \"bank", "drug \'name", "pingedAt"] }
      cache_name = @cm.getCacheKey @repo_name, query_string
      path_to_file = @test_folder + cache_name + '.' + format

      insert_query1 = @km.getInsertStatement(data_obj1)
      promise1 = @dbRepo.query(insert_query1)
      promise1.then ()=>

        @cm.generateCache @repo_name, null, [], [], query_string, format, (error)=>
          expect(fs.existsSync path_to_file).toBe true
          expect(()=>
            JSON.parse fs.readFileSync(path_to_file)
          ).not.toThrow()

          data_obj = JSON.parse fs.readFileSync(path_to_file)
          expect(data_obj[0]['drug &#34;bank']).toEqual "cache &#34;that double quote"
          expect(data_obj[0]['drug &#39;name']).toEqual "cache &#39;that single quote"
          done()

  describe "clearCache", ->
    it "should clear cache folder of one file belonging to repository", (done)->
      query_string = @km.getSelectStatement { $select : [{ $max : "pingedAt" }] }
      format = 'json'
      cache_name = @cm.getCacheKey @repo_name, query_string
      @cm.generateCache @repo_name, null, [], [], query_string, format, (error)=>
        expect(fs.existsSync(@test_folder + cache_name + '.' + format)).toBe true
        @cm.clearCache @repo_name, ()->
          expect(fs.existsSync(@test_folder + cache_name + '.' + format)).toBe false
          done()

    it "should clear cache folder of all files belonging to repository", (done)->
      query_string1 = @km.getSelectStatement { $select : [{ $min : "pingedAt" }] }
      query_string2 = @km.getSelectStatement { $select : [{ $distinct : "pingedAt" }] }
      format = 'json'
      cache_name1 = @cm.getCacheKey @repo_name, query_string1
      cache_name2 = @cm.getCacheKey @repo_name, query_string2
      @cm.generateCache @repo_name, null, [], [], query_string1, format, (error)=>
        @cm.generateCache @repo_name, null, [], [], query_string2, format, (error)=>
          expect(fs.readdirSync(@test_folder).length).toEqual 2
          @cm.clearCache @repo_name, ()->
            expect(fs.existsSync(@test_folder + cache_name1 + '.' + format)).toBe false
            expect(fs.existsSync(@test_folder + cache_name2 + '.' + format)).toBe false
            done()

  describe "writeHtmlToCache", ->
    it "should render valid HTML pages", (done)->
      format = 'html'      
      data_obj1 = 
        "drug bank" : "cache that stuff"
      query_string = @km.getSelectStatement { $select : ["drug bank", "pingedAt"] }
      cache_name = @cm.getCacheKey @repo_name, query_string
      path_to_file = @test_folder + cache_name + '.' + format

      insert_query1 = @km.getInsertStatement(data_obj1)
      promise1 = @dbRepo.query(insert_query1)
      promise1.then ()=>

        @cm.generateCache @repo_name, null, ["drug bank", "pingedAt"], [], query_string, format, (error)=>
          expect(fs.existsSync path_to_file).toBe true
          html_output = fs.readFileSync(path_to_file).toString()
          expect(html_output.match("drug bank").length).toBe 1
          expect(html_output.match("cache that stuff").length).toBe 1
          done()

