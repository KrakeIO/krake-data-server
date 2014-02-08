async = require 'async'
crypto = require 'crypto'
exec = require('child_process').exec
fs = require 'fs'
kson = require 'kson'

class CacheManager
  constructor: (@cachePath, @dbRepo, @modelBody)->
    @csvDelimiter = "  DELIMITER ';' "
    @createCacheFolder()

  createCacheFolder: ()->
    fs.mkdirSync(@cachePath) unless fs.existsSync(@cachePath)
  
  # @Description : returns the path to the cached record 
  # @param : tableName:String
  # @param : columns:array
  # @param : urlColumns:array
  # @param : query:string
  # @param : format:string
  # @param : callback:function(error:string, pathToFile:string)
  getCache: (tableName, columns, urlColumns, query, format, callback)->
    cacheIdentifier = tableName + "_" + crypto.createHash('md5').update(kson.stringify(query)).digest("hex")
    pathToFile = @cachePath + cacheIdentifier + "." + format
    
    fs.exists pathToFile, ( exists )=>
      if !exists then @generateCache tableName, columns, urlColumns, query, format, pathToFile, (err)->
        console.log '%s : Created new cache : %s', tableName, format
        callback && callback err, pathToFile
        
      else 
        console.log '%s : Cache exist : %s', tableName, format
        callback && callback null, pathToFile



  # @Description : clears the cached records for a table
  # @param : tableName:string
  # @param : callback:function(error:String, status:Boolean)->
  clearCache: (tableName, callback)->
    callback && callback null, true



  # @Description : generates a cached record
  # @param : tableName:String
  # @param : columns:array
  # @param : urlColumns:array
  # @param : query:string
  # @param : format:string
  # @param : pathToFile:string  
  # @param : callback:function(error:string)  
  generateCache: (tableName, columns, urlColumns, query, format, pathToFile, callback)->
    model = @dbRepo.define tableName, @modelBody
    cbSuccess = ()=>
      switch format
        when 'json'
          query = 'Copy ( select array_to_json(array_agg( row_to_json(row))) from (' +
            query + ") row ) To '" + pathToFile + "';"

        when 'csv'
          query = "Copy (" + query + ") To '" + pathToFile + "' With " + @csvDelimiter + " CSV HEADER;"
          console.log query
      
      @dbRepo.query(query).success(
        (rows)=>
          switch format
            when 'json', 'csv'
              console.log '%s : cache successfully created : %s', tableName, format
              callback && callback null
                
            when 'html'
              @writeHtmlToCache rows, columns, urlColumns, pathToFile, (status)->
              console.log '%s : cache successfully created : %s', tableName, format
              callback && callback null              
              
          
      ).error(
        (e)=>
          console.log "Error occured while fetching records\nError: %s ", e
          callback && callback e
      )
      
    cbFailure = (error)=>
      console.log "rational db connection failure.\n Error message := " + error  
      callback && callback error

    model.sync().success(cbSuccess).error(cbFailure)



  # @Description : creates the HTML cache from the retrieved results
  # @param : rows:array
  # @param : columns:array
  # @param : urlColumns:array
  # @param : pathToFile:string
  # @param : callback:function()
  writeHtmlToCache : (rows, columns, urlColumns, pathToFile, callback)=>
    results = rows
    fs.appendFileSync pathToFile, "<table class='table' id='data_table'>\r\n"
    
    fs.appendFileSync pathToFile, "\t<tr id='data-table-header'>\r\n"
    for y in [0...columns.length]
      fs.appendFileSync pathToFile, "\t\t<th>" + columns[y] + "</th>\r\n"
      
    fs.appendFileSync pathToFile, "\t</tr>\r\n"

    if results
      # iterates through all the columns
      for i in [0...results.length]
        columns = columns || Object.keys array[i]
        fs.appendFileSync pathToFile, "\t<tr>\r\n"

        # adds a new row of record
        for y in [0...columns.length]
          index = columns[y]
          if urlColumns.indexOf(index) < 0          
            fs.appendFileSync pathToFile, "\t\t<td>" + results[i][index] + "</td>\r\n"
            
          else          
            fs.appendFileSync pathToFile, "\t\t<td><a target='_blank' rel='nofollow' href='" +
              results[i][index] + "'>" + results[i][index] + "</a></td>\r\n" 
            
        fs.appendFileSync pathToFile,  "\t</tr>\r\n"

    fs.appendFileSync pathToFile, "</table>"
    callback && callback()
  

module.exports = CacheManager