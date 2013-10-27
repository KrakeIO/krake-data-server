crypto = require 'crypto'
fs = require 'fs'
kson = require 'kson'
exec = require('child_process').exec

class CacheManager
  constructor: (@cachePath, @dbHandler, @modelBody)->
  
  getCache: (tableName, columns, urlColumns, query, format, callback)->
    cacheIdentifier = tableName.concat("_").concat(crypto.createHash('md5').update(kson.stringify(query)).digest("hex"))
    pathToFile = @cachePath.concat(cacheIdentifier).concat(".").concat(format)
    
    fs.exists pathToFile, ( exists )=>
      if !exists then @generateCache tableName, columns, urlColumns, query, format, pathToFile, (status)->
        console.log '%s : Created new cache : %s', tableName, format
        if status 
          callback && callback false, pathToFile
          
        else
          callback && callback true, pathToFile
        
      else 
        console.log '%s : Cache exist : %s', tableName, format
        callback && callback false, pathToFile



  generateCache: (tableName, columns, urlColumns, query, format, pathToFile, callback)->
    model = @dbHandler.define tableName, @modelBody
    cbSuccess = ()=>
      switch format
        when 'json'
          query = [
            'Copy ( select array_to_json(array_agg( row_to_json(row))) from (' ,
            query , ") row ) To '", pathToFile, "';"
          ]
          query = query.join("")

        when 'csv'
          query = [ "Copy (", query, ") To '", pathToFile, "' With CSV HEADER;"]
          query = query.join("")
          console.log query
      
      @dbHandler.query(query).success(
        (rows)=>
          switch format
            when 'json', 'csv'
              console.log '%s : cache successfully created : %s', tableName, format
              callback && callback null
                
            when 'html'
              console.log rows
              @writeHtmlToCache rows, columns, urlColumns, pathToFile, (status)->
              console.log '%s : cache successfully created : %s', tableName, format
              callback && callback null              
              
          
      ).error(
        (e)=>
          console.log "Error occured while fetching records\nError: %s ", e
          callback e, null
      )
      
    cbFailure = (error)=>
      console.log "rational db connection failure.\n Error message := " + error  
      callback error, null

    model.sync().success(cbSuccess).error(cbFailure)



    
  writeHtmlToCache : (rows, columns, urlColumns, pathToFile, callback)=>
    results = rows
    fs.appendFileSync pathToFile, "<table class='table' id='data_table'>\r\n"
    
    fs.appendFileSync pathToFile, "\t<tr id='data-table-header'>\r\n"
    for y in [0...columns.length]
      fs.appendFileSync pathToFile, ["\t\t<th>", columns[y], "</th>\r\n"].join("")
      
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
            fs.appendFileSync pathToFile,[
              "\t\t<td>",
              results[i][index],
              "</td>\r\n"
            ].join("")
          else          
            fs.appendFileSync pathToFile, [
              "\t\t<td><a target='_blank' rel='nofollow' href='",
              results[i][index],
              "'>",
              results[i][index],
              "</a></td>\r\n" 
            ].join("")
            
        fs.appendFileSync pathToFile,  "\t</tr>\r\n"

    fs.appendFileSync pathToFile, "</table>"
    callback && callback()
  

module.exports = CacheManager