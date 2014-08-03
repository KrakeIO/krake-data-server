KrakeModel    = require '../models/krake_model'
KrakeSetModel = require '../models/krake_set_model'
recordSetBody = require('krake-toolkit').schema.record_set

class DataSetController
  constructor : (@dbSystem, @dbRepo, @set_name, callback)->  
    model = @dbRepo.define @set_name, recordSetBody
    model.sync().success ()=>
      callback && callback()
    .error ()=>
      callback && callback()

  getRepoBatches : (repo_name, callback)->
    @km = new KrakeModel @dbSystem, repo_name, (status, error_message)=>
      query_string = @km.getSelectStatement 
        $select : [{ $distinct : "pingedAt" }]
        $order: [{ $desc: "pingedAt" }]

      @dbRepo.query(query_string).success (records)->
        records = records || []
        records = records.map (record_obj)->
          record_obj.pingedAt

        callback && callback records

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
    @clearBatches repo_name,  num_of_batches, ()=>
      @copyBatches repo_name,  num_of_batches, ()=>
        callback && callback()

  clearBatches : (repo_name, num_of_batches, callback)->
    @getRepoBatches repo_name, (batches)=>
      if !batches 
        console.log "[DATA_SET_CONTROLLER] #{new Date()} #{@set_name} : Batches is false for #{repo_name}"
        callback?()

      else if batches.length > 0 

        del_query = " DELETE FROM  \"#{@set_name}\" WHERE " +
          " \"datasource_handle\"='" + repo_name + "' "

        if num_of_batches && num_of_batches > 0
          batch_and_clause = batches.slice(0,num_of_batches).map((batch)->
            " \"pingedAt\"='#{batch}' "
          ).join(" OR ")
          del_query += " AND ( #{batch_and_clause} )"

        @dbRepo.query(del_query)
          .success ()=> 
            console.log "[DATA_SET_CONTROLLER] #{new Date()} #{@set_name} : clear batch of #{repo_name} successful "
            callback?()
          .error (e)=>
            console.log "[DATA_SET_CONTROLLER] #{new Date()} #{@set_name} : clear batch of #{repo_name} failed : #{error}"
            callback?()
      
      else if batches.length == 0
        console.log "[DATA_SET_CONTROLLER] #{new Date()} #{@set_name} : Batches Empty for #{repo_name}"
        callback?()

  copyBatches : (repo_name, num_of_batches, callback)->
    @getRepoBatches repo_name, (batches)=>
      if !batches || batches.length == 0
        callback && callback()

      if batches.length > 0
        if num_of_batches && num_of_batches > 0
          batches_to_copy = batches.slice(0,num_of_batches)
        else
          batches_to_copy = batches

        query = batches_to_copy.map((batch)=>
          'INSERT INTO "' + @set_name + '" ("properties", "datasource_handle", "pingedAt", "createdAt", "updatedAt") ' +
          '  SELECT ' +
          '    "properties",' +
          '    \'' + repo_name + '\' as "datasource_handle",' +
          '    \'' + batch + '\' as "pingedAt",' +
          '    \'' + batch + '\' as "createdAt",' +
          '    \'' + batch + '\' as "updatedAt"' +
          '  FROM "' + repo_name + '" ' +
          '  WHERE ' +
          '   "pingedAt" = \'' + batch + '\' ;'

        ).join("\n\n")

        @dbRepo.query(query)
          .success ()=> 
            console.log "[DATA_SET_CONTROLLER] #{new Date()} #{@set_name} : copy of records from #{repo_name} successful"
            callback?()
          .error (e)=>
            console.log "[DATA_SET_CONTROLLER] #{new Date()} #{@set_name} : copy of records from #{repo_name} failed #{e}"




module.exports = DataSetController