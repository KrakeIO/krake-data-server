async = require 'async'
crypto = require 'crypto'
exec = require('child_process').exec
fs = require 'fs'
kson = require 'kson'
Q = require 'q'

class CacheController
  constructor: (@cachePath, @dbRepo, @modelBody, @s3Backer)->
    @csvDelimiter = "  DELIMITER ',' "
    @createCacheFolder()

  createCacheFolder: ()->
    fs.mkdirSync(@cachePath) unless fs.existsSync(@cachePath)

  getCacheKey: (repo_name, query)->
    repo_name + "_" + crypto.createHash('md5').update(query).digest("hex")

  getCacheStream: (repo_name, krake, query_obj, format)->
    console.log "[CacheController] #{new Date()} \t\tget Cache Stream"
    deferred = Q.defer()

    query_promise = @getSqlQuery(repo_name, krake, query_obj)

    query_promise
      .then ( query_string )=>
        console.log "[CacheController] #{new Date()} \t\tQuery String generated"      
        @tryFetchCacheFromS3( repo_name, krake, query_obj, query_string, format )

      .then ( cache_stream )=>
        console.log "[CacheController] #{new Date()} \t\tCache Stream generated"
        deferred.resolve cache_stream

      .catch ( err )=>
        deferred.reject err

    deferred.promise

  tryFetchCacheFromS3: ( repo_name, krake, query_obj, query_string, format )->
    console.log "[CacheController] #{new Date()} \t\tfetching Cache from S3"
    deferred = Q.defer()

    cacheKey = @getCacheKey repo_name, query_string
    pathToFile = @cachePath + cacheKey + "." + format

    @s3Backer.cacheExist( repo_name, cacheKey )
      .then ( cache_exist )=>
        if cache_exist
          console.log "[CacheController] #{new Date()} \t\tS3 cache exists"
          download_stream_obj = @s3Backer.getDownloadStreamObject task_key, file_name, content_type
          deferred.resolve download_stream_obj
          broken_promise = Q.defer().promise

        else
          console.log "[CacheController] #{new Date()} \t\tS3 cache does not exist"
          @tryGenerateS3Cache( repo_name, krake, query_obj, query_string, format )

      .then ( download_stream_obj )=>
        console.log "[CacheController] #{new Date()} \t\treturning S3 cache stream "
        deferred.resolve download_stream_obj

      .catch (err)=>
        console.log "[CacheController] #{new Date()} \t\terror fetching s3 cache stream "
        deferred.reject err

    deferred.promise

  tryGenerateS3Cache: ( repo_name, krake, query_obj, query_string, format )->
    console.log "[CacheController] #{new Date()} \t\ttrying to generate cache on S3"
    deferred = Q.defer()

    cacheKey = @getCacheKey repo_name, query_string
    pathToFile = @cachePath + cacheKey + "." + format

    if (!fs.existsSync(pathToFile) || query_obj.$fresh)
      console.log "[CacheController] #{new Date()} \t\tlocal cache does not exist"
      @generateCache( repo_name, krake.columns, krake.url_columns, query_string, format )
        .then ()=>
          console.log "[CacheController] #{new Date()} \t\tuploading locale cache to S3"
          @s3Backer.streamUpload repo_name, cacheKey, pathToFile, @getContentType(format)
          
        .then ( s3_down_stream )=>
          console.log "[CacheController] #{new Date()} \t\treturning generated S3 cache stream "
          deferred.resolve s3_down_stream

        .catch ( err )=>
          console.log "[CacheController] #{new Date()} \t\terror occurred generating s3 cache "
          deferred.reject err

    else
      console.log "[CacheController] #{new Date()} \t\tlocal cache exist"
      @s3Backer.streamUpload( repo_name, cacheKey, pathToFile, @getContentType(format) )
        .then ( s3_down_stream )=> # When S3 cache exists
          console.log "[CacheController] #{new Date()} \t\treturning S3 cache stream "
          deferred.resolve s3_down_stream

    deferred.promise

  getContentType: (format)->
    switch format
      when 'json'
        "application/json; charset=utf-8"
      when 'html'
        "text/html; charset=utf-8"
      when 'csv'
        "text/csv; charset=utf-8"
  
  # @Description : returns the path to the cached record 
  #
  # Params:
  #   repo_name:String
  #   krake:KrakeModel
  #   query_obj:Object
  #   format:string
  #   callback:function(error:string, pathToFile:string)
  #
  # Returns
  #   promise:Object
  #
  getCache: (repo_name, krake, query_obj, format, callback)->
    deferred = Q.defer()

    columns = krake.columns
    urlColumns = krake.url_columns
    @getSqlQuery(repo_name, krake, query_obj)
      .then (query)=>
        cacheKey = @getCacheKey repo_name, query
        pathToFile = @cachePath + cacheKey + "." + format
            
        if (!fs.existsSync(pathToFile) || query_obj.$fresh) 
          @generateCache repo_name, columns, urlColumns, query, format, (err)=>
            callback && callback err, pathToFile
            if err 
              deferred.reject err
            else
              deferred.resolve pathToFile 
          
        else
          deferred.resolve pathToFile 
          callback && callback null, pathToFile

      .catch (error)=>
        console.log "getCache: " + error
        deferred.reject error

    deferred.promise

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
  generateCache: (repo_name, columns, urlColumns, query, format, callback)->
    deferred = Q.defer()

    pathToFile = @cachePath + @getCacheKey(repo_name, query) + "." + format
    
    cbSuccess = ()=>
      switch format
        when 'json'
          query = 'Copy ( select array_to_json(array_agg( row_to_json(row))) from (' +
            query + ") row ) To '" + pathToFile + "' With NULL '[]';"

        when 'csv'
          query = "Copy (" + query + ") To '" + pathToFile + "' With " + @csvDelimiter + " CSV HEADER;"

      @dbRepo.query(query).then(
        (results)=>
          switch format
            when 'json', 'csv' then callback && callback null
            when 'html'
              @writeHtmlToCache results, columns, urlColumns, pathToFile, (status)->
              callback && callback null     
              deferred.resolve true
              
          
      ).catch(
        (e)=>
          console.log "Error occured while fetching records\nError: %s ", e
          callback && callback e
          deferred.reject e
      )
      
    cbFailure = (error)=>
      console.log "rational db connection failure.\n Error message := " + error  
      callback && callback error
      deferred.reject error

    model = @dbRepo.define repo_name, @modelBody
    model.sync().then(cbSuccess).catch(cbFailure)

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