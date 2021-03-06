KrakeModel    = require '../models/krake_model'
KrakeSetModel = require '../models/krake_set_model'
recordSetBody = require('krake-toolkit').schema.record_set
Q             = require 'q'

class DataSetController
  constructor : (@dbSystem, @dbRepo, @set_name, callback)->  
    @model = @dbRepo.define @set_name, recordSetBody
    @model.sync().then ()=>
      callback && callback()
    .catch ()=>
      callback && callback()

  getRepoBatches : (repo_name, callback)->
    deferred = Q.defer()
    @km = new KrakeModel @dbSystem, repo_name, [], (status, error_message)=>
      query_string = @km.getSelectStatement 
        $select : [{ $distinct : "pingedAt" }]
        $order: [{ $desc: "pingedAt" }]

      @dbRepo.query(query_string)
        .then (records)->
          records = records[0]
          records = records || []
          records = records.map (record_obj)->
            record_obj.pingedAt

          callback && callback records
          deferred.resolve records

        .catch (error)->
          callback && callback []
          deferred.reject error

    deferred.promise

  # @Description: synchronizes the records from data source over to dataset
  #
  # @param: repo_name:String
  #   the name of the repository to port the data from
  #
  # @param: num_of_batches:Integers
  #   the number of batches to port over starting from most recent to earliest in reverse chronological order
  #   if null, then ports all batches over
  #
  # @param: callback:function
  #   the function that gets called when the synchronization operations completes
  consolidateBatches : (repo_name, num_of_batches, callback)->
    deferred = Q.defer()
    @getRepoBatches repo_name, (batches)=>  
      batches = batches || []
      cons_func_call  = "a#{@set_name}_#{repo_name}_consolidate()"
      clear_statement = @clearBatchesQuery repo_name, batches, num_of_batches
      copy_statement =  @copyBatchesQuery repo_name, batches, num_of_batches

      master_statement = "
        BEGIN ISOLATION LEVEL READ UNCOMMITTED;\r\n
          #{clear_statement}
          #{copy_statement}
        END;\r\n
        "

      commitIt = (retries, error)=>
        retries = retries || 0
        if retries == 3
          deferred.reject error
          return callback?()

        @dbRepo.query(master_statement)   
          .then ()=> 
            console.log "[DATA_SET_CONTROLLER] #{new Date()} consolidated records successful" +
              "\r\n\tdata_set: #{@set_name}" +
              "\r\n\trepo_name: #{repo_name} "
            callback?()
            deferred.resolve()

          .catch (e)=>
            console.log "[DATA_SET_CONTROLLER] #{new Date()} consolidated records failed #{e}" +
              "\r\n\tERROR: #{e}" +
              "\r\n\tdata_set: #{@set_name}" +
              "\r\n\trepo_name: #{repo_name} " +
              "\r\n\tretries: #{retries} "
            commitIt (retries + 1), e


      commitIt 0

    deferred.promise

  clearAll : (repo_name, callback)->
    del_query = @clearBatchesQuery repo_name, []
    @dbRepo.query(del_query)   
      .then ()=> 
        console.log "[DATA_SET_CONTROLLER] #{new Date()} cleared all records successful" +
          "\r\n\tdata_set: #{@set_name}" +
          "\r\n\trepo_name: #{repo_name} "
        callback?()
      .catch (e)=>
        console.log "[DATA_SET_CONTROLLER] #{new Date()} cleared all records failed #{e}" +
          "\r\n\tERROR: #{e}" +
          "\r\n\tdata_set: #{@set_name}" +
          "\r\n\trepo_name: #{repo_name} "            


  clearBatchesQuery : (repo_name, batches, num_of_batches)->
    del_query = " DELETE FROM  \"#{@set_name}\" WHERE \r\n" +
      " \"datasource_handle\"='" + repo_name + "' \r\n"

    if batches.length > 0 && num_of_batches && num_of_batches > 0 
      batch_and_clause = batches.slice(0,num_of_batches).map((batch)->
        " \"pingedAt\"='#{batch}' \r\n"
      ).join(" OR ")
      del_query += " AND (\r\n#{batch_and_clause} )"

    del_query += ";\r\n"


  copyBatchesQuery : (repo_name, batches, num_of_batches)->
    if num_of_batches && num_of_batches > 0
      batches = batches.slice(0,num_of_batches)

    query = batches.map((batch)=>
      'INSERT INTO "' + @set_name + '" ("properties", "datasource_handle", "pingedAt", "createdAt", "updatedAt") ' + "\r\n" +
      '  SELECT ' + "\r\n" +
      '    "properties",' + "\r\n" +
      '    \'' + repo_name + '\' as "datasource_handle",' + "\r\n" +
      '    \'' + batch + '\' as "pingedAt",' + "\r\n" +
      '    \'' + batch + '\' as "createdAt",' + "\r\n" +
      '    \'' + batch + '\' as "updatedAt"' + "\r\n" +
      '  FROM "' + repo_name + '" ' + "\r\n" +
      '  WHERE ' + "\r\n" +
      '   "pingedAt" = \'' + batch + '\' ;'+ "\r\n"

    ).join("\n\n")



module.exports = DataSetController