process.env['NODE_ENV'] = 'test'
request = require 'request'
KrakeModel = require '../models/krake_model'
fs = require 'fs'
krake_definition = fs.readFileSync(__dirname + '/fixtures/krake_definition.json').toString()

test_objects = require "../krake_data_server"
app = test_objects.app
dbRepo = test_objects.dbRepo
dbSystem = test_objects.dbSystem
krakeSchema = test_objects.krakeSchema
recordBody = test_objects.recordBody
CacheController = test_objects.CacheController

describe "krake data server", ->
  beforeEach (done)->
    @repo_name = "1_66240a39bc8c73a3ec2a08222936fc49eses"
    @port = 9803
    @test_server = "http://localhost:" + @port + "/"
    app.listen @port
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

  describe "krake data server routes", ->
    beforeEach (done)->
      @dbRepo = dbRepo
      @repo_name = "1_66240a39bc8c73a3ec2a08222936fc49eses"
      @Krake = dbSystem.define 'krakes', krakeSchema
      @test_folder = "/tmp/test/"
      @cm = new CacheController @test_folder, dbRepo, recordBody

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
            request @test_server + @repo_name + '/clear_cache', (error, response, body)->
              done()

    afterEach (done)->
      request @test_server + @repo_name + '/clear_cache', (error, response, body)->
        done()

    it "should clear the cache", (done)->
      query_string = @km.getSelectStatement { $select : [{ $max : "pingedAt" }] }
      format = 'json'
      cache_name = @cm.getCacheKey @repo_name, query_string
      @cm.generateCache @repo_name, [], [], query_string, format, (error)=>
        expect(fs.existsSync(@test_folder + cache_name + '.' + format)).toBe true
        request @test_server + @repo_name + '/clear_cache', (error, response, body)->
          expect(fs.existsSync(@test_folder + cache_name + '.' + format)).toBe false
          done()

    it "should respond with a valid JSON http response", (done)->
      d1 = new Date()
      api_location = @test_server + @repo_name + '/json?q={"$limit":2}'
      data_obj = 
        "drug bank" : "This is some bank" + d1.toString()
        "drug name" : "This is some drug" + d1.toString()
        "categories" : "This is some category" + d1.toString()
        "therapeutic indication" : "This is some theraphy" + d1.toString()
        "pingedAt" : new Date()
        "pingedAt" : new Date()
      insert_query = @km.getInsertStatement(data_obj)
      promise1 = @dbRepo.query(insert_query)
      promise2 = promise1.then @dbRepo.query(insert_query)
      promise3 = promise2.then @dbRepo.query(insert_query)
      promise3.then ()=>
        request api_location, (error, response, body)->
          expect(()=>
            JSON.parse body
          ).not.toThrow()          
          results = JSON.parse body
          expect(results.length).toEqual 2
          expect(results[0]["drug bank"]).toEqual "This is some bank" + d1.toString()
          done()
