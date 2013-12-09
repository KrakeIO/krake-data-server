async = require 'async'
crypto = require 'crypto'
exec = require('child_process').exec
fs = require 'fs'
kson = require 'kson'

class CacheManager
  constructor: (@cachePath, @dbHandler, @modelBody)->
    @csvDelimiter = "  DELIMITER ';' "
  
  # @param : km:KrakeModel
  # @param : date:string
  # @param : format:string  
  # @param : callback:function(err:String, pathToCache:String)
  getDiffCache: (km, date, format, callback)->
    cacheIdentifier = km.krakeHandle + "_" + crypto.createHash('md5').update(kson.stringify(date)).digest("hex") + "_diff"
    pathToFile = @cachePath + cacheIdentifier + "." + format
  
    fs.exists pathToFile, ( exists )=>
      if !exists         
        @getPreviousBatch km.krakeHandle, date, (previousBatchDate)=>
          queryForUpdate = 'SELECT ' + 
            km.getColumnsQuery() +
            ' ,\"createdAt\", \"updatedAt\", \"pingedAt\" ' + 
            ' FROM "' + km.krakeHandle + '" ' +
            ' WHERE "updatedAt" = \'' + date + '\' '

          forDeleted = 'SELECT ' + 
            km.getColumnsQuery() +
            ' ,\"createdAt\", \"updatedAt\", \"pingedAt\" ' + 
            ' FROM "' + km.krakeHandle + '" ' +
            ' WHERE '+ 
            ' "pingedAt" = \'' + previousBatchDate + '\'  '      
      
          @generateDiffCache km, queryForUpdate, forDeleted, format, pathToFile, (err)->
            console.log '%s : Created new diff cache : %s', km.krakeHandle, format
            callback && callback err, pathToFile
      
      else 
        console.log '%s : Cache exist : %s', km.krakeHandle, format
        callback && callback null, pathToFile
  
  
  
  # @param : callback:function(error:String)
  generateDiffCache: (km, queryForUpdate, forDeleted, format, pathToCache, callback)->
    
    tempCache1 = pathToCache + "_u"
    tempCache2 = pathToCache + "_d"  
    cbSuccess = ()=>
      async.parallel [
        (callback)=>
          @generateCache km.krakeHandle, km.columns, km.url_columns, queryForUpdate, format, tempCache1, (err)->
            callback err, pathToCache

        , (callback)=>
          @generateCache km.krakeHandle, km.columns, km.url_columns, queryForUpdate, format, tempCache2, (err)->
            callback err, pathToCache            
            
      ], (err, results_array)=>
        command = "./shell/combine_diff." + format + ".sh " + 
          " " + pathToCache + "_uh" +
          " " + tempCache1 +
          " " + pathToCache + "_dh" +
          " " + tempCache2 +
          " " + pathToCache
  
        exec command, (error, stdout, stderr)->
          if !error
            console.log "File generated, %s", pathToCache
          else
            console.log "Error generating diff file, %s", error
          callback && callback(error)             

    
    cbFailure = (error)=>
      console.log "rational db connection failure.\n Error message := " + error  
      callback && callback error
    
    @dbHandler.define(km.krakeHandle, @modelBody).sync().success(cbSuccess).error(cbFailure)
  
  
  
  # @Description: Get previous batch given current batch
  # @param: table_name:string
  # @param: current_batch:string
  # @param: callback:function(string)
  getPreviousBatch : (tableName, currentBatch, callback)=>
    queryString = 'select distinct cast("pingedAt" as text) as "pingedAt" from "' + tableName + '" ' + 
      ' where "pingedAt" <  \'' + currentBatch + '\'' + 
      ' order by "pingedAt" desc limit 1 '

    @dbHandler.query(queryString).success(
      (rows)=>
        if rows.length > 0 
          callback && callback rows[0].pingedAt
        else
          callback && callback false
          
    ).error(
      (e)=>
        console.log "Error occured while fetching batches \nError: " + e
        callback []
    )
  

  # @Description: Get list of batches ever ran
  # @param: table_name:string
  # @param: callback:function(array[])
  getBatches : (table_name, callback)=>

    cbSuccess = ()=>

      query_string = 'select distinct cast("pingedAt" as text) as "pingedAt" from "' + table_name + '" order by "pingedAt" desc'
      @dbHandler.query(query_string).success(
        (rows)=>
          results = rows.map (batch)->
            batch.pingedAt
          callback results

      ).error(
        (e)=>
          console.log "Error occured while fetching batches \nError: " + e
          callback []
      )

    cbFailure = (error)=>
      console.log "getBatches : table does not exist \nError: " + error
      callback []
      
    @dbHandler.define(table_name, @modelBody).sync().success(cbSuccess).error(cbFailure)
  
  
  
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


  # @Description : generates a cached record
  # @param : tableName:String
  # @param : columns:array
  # @param : urlColumns:array
  # @param : query:string
  # @param : format:string
  # @param : pathToFile:string  
  # @param : callback:function(error:string)  
  generateCache: (tableName, columns, urlColumns, query, format, pathToFile, callback)->
    model = @dbHandler.define tableName, @modelBody
    cbSuccess = ()=>
      switch format
        when 'json'
          query = 'Copy ( select array_to_json(array_agg( row_to_json(row))) from (' +
            query + ") row ) To '" + pathToFile + "';"

        when 'csv'
          query = "Copy (" + query + ") To '" + pathToFile + "' With " + @csvDelimiter + " CSV HEADER;"
          console.log query
      
      @dbHandler.query(query).success(
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