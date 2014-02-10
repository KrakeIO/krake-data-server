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

  getCacheKey: (repo_name, query)->
    repo_name + "_" + crypto.createHash('md5').update(query).digest("hex")
  
  # @Description : returns the path to the cached record 
  # @param : repo_name:String
  # @param : columns:array
  # @param : urlColumns:array
  # @param : query:string
  # @param : format:string
  # @param : callback:function(error:string, pathToFile:string)
  getCache: (repo_name, columns, urlColumns, query, format, callback)->
    cacheKey = @getCacheKey repo_name, query
    pathToFile = @cachePath + cacheKey + "." + format
    
    fs.exists pathToFile, ( exists )=>
      if !exists then @generateCache repo_name, columns, urlColumns, query, format, (err)->
        console.log '%s : Created new cache : %s', repo_name, format
        callback && callback err, pathToFile
        
      else 
        console.log '%s : Cache exist : %s', repo_name, format
        callback && callback null, pathToFile



  # @Description : clears the cached records for a table
  # @param : repo_name:string
  # @param : callback:function(error:String, status:Boolean)->
  clearCache: (repo_name, callback)->
    callback && callback null, true
    fs.readdirSync(@cachePath).forEach (file_name)=>
      if file_name.indexOf(repo_name) != -1
        file_path = @cachePath + file_name
        fs.unlinkSync file_path

    callback && callback()

  # @Description : generates a cached record
  # @param : repo_name:String
  # @param : columns:array
  # @param : urlColumns:array
  # @param : query:string
  # @param : format:string
  # @param : callback:function(error:string)  
  generateCache: (repo_name, columns, urlColumns, query, format, callback)->
    pathToFile = @cachePath + @getCacheKey(repo_name, query) + "." + format
    model = @dbRepo.define repo_name, @modelBody
    cbSuccess = ()=>
      switch format
        when 'json'
          query = 'Copy ( select array_to_json(array_agg( row_to_json(row))) from (' +
            query + ") row ) To '" + pathToFile + "';"

        when 'csv'
          query = "Copy (" + query + ") To '" + pathToFile + "' With " + @csvDelimiter + " CSV HEADER;"

      @dbRepo.query(query).success(
        (rows)=>
          switch format
            when 'json', 'csv' then callback && callback null
            when 'html'
              @writeHtmlToCache rows, columns, urlColumns, pathToFile, (status)->
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