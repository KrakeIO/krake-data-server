async = require 'async'
crypto = require 'crypto'
exec = require('child_process').exec
fs = require 'fs'
kson = require 'kson'
Q = require 'q'
UnescapeStream  = require 'unescape-stream'
unescape = new UnescapeStream()

class CacheController
  constructor: (@cachePath, @dbRepo, @modelBody, @s3Backer)->
    @csvDelimiter = "  DELIMITER ',' "
    @createCacheFolder()

  createCacheFolder: ()->
    fs.mkdirSync(@cachePath) unless fs.existsSync(@cachePath)

  getCacheKey: (repo_name, query)->
    repo_name + "_" + crypto.createHash('md5').update(query).digest("hex")

  # Description: given the necessary details attempts to fetch the Stream Object to data harvested 
  #
  # Params:
  #   repo_name:String
  #   krake:Object
  #   query_obj:Object
  #   format:String
  #
  # Returns
  #   Promise:Object
  #     resolve:function( download_stream )
  #
  getCacheStream: (repo_name, krake, query_obj, format)->
    console.log "[CacheController] #{new Date()} \t\tget Cache Stream"
    deferred = Q.defer()

    query_promise = @getSqlQuery( repo_name, krake, query_obj )

    query_promise
      .then ( query_string )=>
        console.log "[CacheController] #{new Date()} \t\tQuery String generated"      
        if @isHardRefresh( query_obj )
          console.log "[CacheController] #{new Date()} \t\tforced to regenerate cache"      
          @tryRefreshAndFetchLocalCacheStreamObject( repo_name, krake, query_obj, query_string, format )

        else if !@localCacheExists( repo_name, query_string, format)
          console.log "[CacheController] #{new Date()} \t\tlocal cache does not exists. Generating it now"
          @tryRefreshAndFetchLocalCacheStreamObject( repo_name, krake, query_obj, query_string, format )        

        else if @localCacheExists( repo_name, query_string, format )
          console.log "[CacheController] #{new Date()} \t\tlocal cache exists. Return it"
          @tryFetchLocalCacheStreamObject( repo_name, query_string, format )

      .then ( cache_stream )=>
        console.log "[CacheController] #{new Date()} \t\tgot Cache Stream"
        deferred.resolve cache_stream

      .catch ( err )=>
        console.log "[CacheController] #{new Date()} \t\tfailed to get Cache Stream"
        deferred.reject err

    deferred.promise

  # @Description : returns a promise that resolves to the stream object that reads from the local cache
  #   Forcefully generates the local cache even if it exists
  #
  # Params:
  #   repo_name:String
  #   query_string:Object
  #   format:string
  #
  # Returns:
  #   Promise:Object
  #     resolve( promise: Promise )
  #
  tryRefreshAndFetchLocalCacheStreamObject: ( repo_name, krake, query_obj, query_string, format )->
    console.log "[CacheController] #{new Date()} \t\ttrying to generate only from local cache"
    deferred = Q.defer()
    
    @generateCache( repo_name, krake.record_model_body, krake.columns, krake.url_columns, query_string, format )
      .then ()=>
        unescape = new UnescapeStream()
        pathToFile = @getPathToLocalCache( repo_name, query_string, format )
        local_cache_stream = fs.createReadStream( pathToFile ).pipe(unescape)
        deferred.resolve local_cache_stream

      .catch ( err )=>
        console.log "[CacheController] #{new Date()} \t\terror occurred generating s3 cache "
        deferred.reject err

    deferred.promise

  # @Description : returns a promise that resolves to the stream object that reads from the local cache
  #   Assumes the local cache already exists
  #
  # Params:
  #   repo_name:String
  #   query_string:Object
  #   format:string
  #
  # Returns:
  #   Promise:Object
  #     resolve( promise: Promise )
  #
  tryFetchLocalCacheStreamObject: ( repo_name, query_string, format )->
    deferred = Q.defer()
    deferred.resolve @getLocalCacheStreamObject( repo_name, query_string, format )
    deferred.promise    

  # @Description : returns the stream object that reads from the local cache.
  #   Assumes the local cache already exists
  #
  # Params:
  #   repo_name:String
  #   query_string:Object
  #   format:string
  #
  # Returns
  #   local_cache_stream:readStream
  #
  getLocalCacheStreamObject: ( repo_name, query_string, format )->
    pathToFile = @getPathToLocalCache( repo_name, query_string, format )
    unescape = new UnescapeStream()
    local_cache_stream = fs.createReadStream( pathToFile ).pipe(unescape)

  # @Description : returns the path to the local cache
  #   Assumes the local cache already exists
  #
  # Params:
  #   repo_name:String
  #   query_string:Object
  #   format:string
  #
  # Returns
  #   pathToFile:String
  #
  getPathToLocalCache: ( repo_name, query_string, format )->
    cacheKey = @getCacheKey repo_name, query_string
    s3CacheKey = cacheKey + "." + format
    pathToFile = @cachePath + cacheKey + "." + format    

  isValidFormat: (format)->
    format in ["json", "html", "csv"]

  getContentType: (format)->
    console.log "[CacheController] #{new Date()} \t\tgetting content type for #{format}"
    switch format
      when 'json'
        "application/json; charset=utf-8"
      when 'html'
        "text/html; charset=utf-8"
      when 'csv'
        "text/csv; charset=utf-8"

  # @Description : checks if the query object is forcing a hard refresh on the cache
  #
  # Params:
  #   repo_name:String
  #   query_string:Object
  #   format:string
  #
  # Returns
  #   exists:Boolean
  #
  isHardRefresh: ( query_obj )->
    return query_obj.$fresh

  # @Description : checks if the local cache already exists. Returns true if it does, false otherwise
  #
  # Params:
  #   repo_name:String
  #   query_string:Object
  #   format:string
  #
  # Returns
  #   exists:Boolean
  #
  localCacheExists: (repo_name, query_string, format)->
    cacheKey = @getCacheKey repo_name, query_string
    s3CacheKey = cacheKey + "." + format
    pathToFile = @cachePath + cacheKey + "." + format
    return fs.existsSync(pathToFile)

  # translates the query object to a valid SQL query string
  #
  # Params:
  #   repo_name: String
  #   krake: Object
  #   query_obj: Object
  #
  # Returns:
  #   Promise:Object
  #     resolve( query: String )
  #     reject( error: Object )
  #
  getSqlQuery: (repo_name, krake, query_obj)->
    deferred = Q.defer()

    if query_obj["$where"] || query_obj["$select"]
      query = krake.getSelectStatement query_obj
      deferred.resolve query
    else
      @getLatestBatch(krake)
        .then (latest_batch)=>
          query_obj = 
            '$fresh': true          
          if latest_batch
            console.log "latest batch: #{latest_batch}"
            query_obj['$where'] = [ { pingedAt: latest_batch } ]

          query = krake.getSelectStatement query_obj
          deferred.resolve query

        .catch (error)=>
          deferred.reject error
          console.log "getSqlQuery: " + error        

    deferred.promise

  # gets the total records count for the latest batch
  getLatestCount: (krake)->
    deferred = Q.defer()
    @getLatestBatch(krake).then (latest_batch)=>
      if latest_batch
        @getCountForBatch krake, latest_batch, deferred
      else
        deferred.resolve({
          batch: "None", 
          count: 0
        })

    deferred.promise

  # gets the total records count for a specific batch
  getCountForBatch: (krake, latest_batch, deferred)=>
    query_obj =
        $select: [{
          $count: "pingedAt"
        }],
        $where: [{
          "pingedAt": latest_batch
        }],
        $fresh: true

    query = krake.getSelectStatement query_obj

    @dbRepo.query(query).then(
      (rows)=>
        rows = rows[0]
        if rows.length == 1
          deferred.resolve({
            batch: latest_batch, 
            count: rows[0]["pingedAt"]
          })
        else
          deferred.resolve({
            batch: latest_batch, 
            count: 0            
          })
        
    ).catch(
      (e)=>
        deferred.reject(new Error(e))
    )    


  # gets the latest batch date
  #
  # returns Q.promise
  getLatestBatch: (krake)->
    deferred = Q.defer()
    query_obj =
      "$select" : [{
        "$distinct" : "pingedAt"
      }],
      "$order" : [{
        "$desc" : "pingedAt"
      }],
      "$fresh" : true,
      "$limit" : 1

    query = krake.getSelectStatement query_obj
    model = @dbRepo.define krake.repo_name, @modelBody
    model.sync()
      .then ()=>
        @dbRepo.query(query)

      .then (results)=>
        rows = results[0]
        if rows.length == 1
          deferred.resolve rows[0]["pingedAt"]
        else
          deferred.resolve ""
            
      .catch (e)=>
        deferred.reject(new Error(e))

    deferred.promise    



  # @Description : clears the cached records for a table
  # @param : repo_name:string
  # @param : callback:function(error:String, status:Boolean)->
  clearCache: (repo_name, callback)->
    fs.readdirSync(@cachePath).forEach (file_name)=>
      if file_name.indexOf(repo_name) != -1 && file_name.indexOf("html") != -1
        file_path = @cachePath + file_name
        fs.unlinkSync file_path
    callback?()


  # @Description : generates a cached record
  # @param : repo_name:String
  # @param : columns:array
  # @param : urlColumns:array
  # @param : query:string
  # @param : format:string
  # @param : callback:function(error:string)  
  generateCache: (repo_name, record_model_body, columns, urlColumns, query, format, callback)->
    deferred = Q.defer()

    pathToFile = @cachePath + @getCacheKey(repo_name, query) + "." + format
    
    record_body = record_model_body || @modelBody
    model = @dbRepo.define repo_name, record_body
    model.sync()
      .then ()=>
        switch format
          when 'json'
            query = 'Copy ( select array_to_json(array_agg( row_to_json(row))) from (' +
              query + ") row ) To '" + pathToFile + "' With NULL '[]';"

          when 'csv'
            query = "Copy (" + query + ") To '" + pathToFile + "' With " + @csvDelimiter + " CSV HEADER;"

        @dbRepo.query(query)

      .then (results)=>
        if format == 'html'
          @writeHtmlToCache results, columns, urlColumns, pathToFile, (status)->

        deferred.resolve( pathToFile )
        callback && callback null

      .catch (error)=>
        console.log "rational db connection failure.\n Error message := " + error  
        callback && callback error
        deferred.reject error    

    deferred.promise



  # @Description : creates the HTML cache from the retrieved results
  # @param : rows:array
  # @param : columns:array
  # @param : urlColumns:array
  # @param : pathToFile:string
  # @param : callback:function()
  writeHtmlToCache : (results, columns, urlColumns, pathToFile, callback)=>
    results = results[0]
    fs.appendFileSync pathToFile, "<table class='table' id='data_table'>\r\n"
    
    fs.appendFileSync pathToFile, "\t<tr id='data-table-header'>\r\n"
    class_names = []
    for y in [0...columns.length]
      class_names.push columns[y].replace(/\s+/g, '_').toLowerCase()
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
            fs.appendFileSync pathToFile, "\t\t<td class='"+class_names[y]+"'>" + results[i][index] + "</td>\r\n"
            
          else          
            fs.appendFileSync pathToFile, "\t\t<td class='"+class_names[y]+"'><a target='_blank' rel='nofollow' href='" +
              results[i][index] + "'>" + results[i][index] + "</a></td>\r\n" 
            
        fs.appendFileSync pathToFile,  "\t</tr>\r\n"

    fs.appendFileSync pathToFile, "</table>"
    callback && callback()
  

module.exports = CacheController