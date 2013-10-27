fs = require 'fs'
kson = require 'kson'
Sequelize = require 'sequelize'

CONFIG = null
ENV = (process.env['NODE_ENV'] || 'development').toLowerCase()
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

dbHandler = new Sequelize CONFIG.postgres.database, userName, password, options
db_dev = new Sequelize CONFIG.userDataDB, userName, password, options

recordBody = require '../../schema/record'
CacheController = require '../../controllers/cache_controller'
KrakeModel = require '../../models/krake_model'

cm = new CacheController '/tmp/', dbHandler, recordBody
km = new KrakeModel db_dev


describe "Testing Cache Manager", ()->

  it "should return a correct previous batch date", (done)->
    cm.getBatches "1_worldwide_directory_of_public_companieseses", (batches)->
      batches = ["2013-09-20 10:28:26","2013-09-01 10:44:04","2013-09-01 10:38:29"]    
      expect(batches.length).toBe(3)
      expect(batches[0]).toBe(batches[0])
      expect(batches[1]).toBe(batches[1])
      expect(batches[2]).toBe(batches[2])
      done()

  it "should return a correct previous batch date", (done)->
    cm.getPreviousBatch "1_worldwide_directory_of_public_companieseses", "2013-09-20 10:28:26", (batch)->
      expect(batch).toBe("2013-09-01 10:44:04")
      done()
      
  it "should return false when previous batch does not exist", (done)->
    cm.getPreviousBatch "1_worldwide_directory_of_public_companieseses", "2013-09-01 10:38:29", (batch)->
      expect(batch).toBe(false)
      done()
      
  it "should return false when current batch does not exist", (done)->
    cm.getPreviousBatch "1_worldwide_directory_of_public_companieseses", "2013-09-01 10:11:29", (batch)->
      expect(batch).toBe(false)
      done()

  
  it "should generate a cached diff file in csv format", (done)->
    tableName = "1_worldwide_directory_of_public_companieseses"  
    km = new KrakeModel db_dev, tableName, ()=> 
      queryForUpdate = 'SELECT ' + 
        km.getColumnsQuery() +
        ' ,\"createdAt\", \"updatedAt\", \"pingedAt\" ' + 
        ' FROM "' + tableName + '" ' +
        ' WHERE "updatedAt" = \'2013-09-20 10:28:26\' '
      
      forDeleted = 'SELECT ' + 
        km.getColumnsQuery() +
        ' ,\"createdAt\", \"updatedAt\", \"pingedAt\" ' + 
        ' FROM "' + tableName + '" ' +
        ' WHERE '+ 
        ' "pingedAt" = \'2013-09-01 10:44:04\'  '      
      
      testPath = '/tmp/mytest.csv'
      fs.existsSync(testPath) && fs.unlinkSync(testPath)
      cm.generateDiffCache km, queryForUpdate, forDeleted, 'csv', testPath, (result)=>
        expect(result).toBe(null)
        expect(fs.existsSync(testPath)).toBe(true)
        done()
        
  it "should generate a cached diff file in json format", (done)->
    tableName = "1_worldwide_directory_of_public_companieseses"  
    km = new KrakeModel db_dev, tableName, ()=>          
      queryForUpdate = 'SELECT ' + 
        km.getColumnsQuery() +
        ' ,\"createdAt\", \"updatedAt\", \"pingedAt\" ' + 
        ' FROM "' + tableName + '" ' +
        ' WHERE "updatedAt" = \'2013-09-20 10:28:26\' '

      forDeleted = 'SELECT ' + 
        km.getColumnsQuery() +
        ' ,\"createdAt\", \"updatedAt\", \"pingedAt\" ' + 
        ' FROM "' + tableName + '" ' +
        ' WHERE '+ 
        ' "pingedAt" = \'2013-09-01 10:44:04\'  '      

      testPath = '/tmp/mytest.json'
      fs.existsSync(testPath) && fs.unlinkSync(testPath)
      cm.generateDiffCache km, queryForUpdate, forDeleted, 'json', testPath, (result)=>
        expect(result).toBe(null)
        expect(fs.existsSync(testPath)).toBe(true)
        try
          kson.parse(fs.readFileSync(testPath).toString())
        catch e
          expect(e).toBe(undefined)
        done()          
    
  it "should detect for generate new diff cache in json format", (done)->
    spyOn(cm, 'generateDiffCache').andCallThrough()
      
    tableName = "1_worldwide_directory_of_public_companieseses"
    testPath = "/tmp/1_worldwide_directory_of_public_companieseses_b764618c206490510bef00cbc67de05a_diff.json"
    fs.existsSync(testPath) && fs.unlinkSync(testPath)
    
    km = new KrakeModel db_dev, tableName, ()=>
      cm.getDiffCache km, '2013-09-20 10:28:26', 'json', (err, pathToFile)=>
        expect(err).toBe(null)
        expect(typeof pathToFile).toBe("string")
        expect(cm.generateDiffCache).toHaveBeenCalled()
        done()
