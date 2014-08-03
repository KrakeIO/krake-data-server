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
    @getRepoBatches repo_name, (batches)=>  
      batches = batches || []
      cons_func_call  = "a#{@set_name}_#{repo_name}_consolidate()"
      clear_statement = @clearBatchesQuery repo_name, batches, num_of_batches
      copy_statement =  @copyBatchesQuery repo_name, batches, num_of_batches

      master_statement = "
        BEGIN ISOLATION LEVEL SERIALIZABLE;\r\n
          #{clear_statement}
          #{copy_statement}
        END;\r\n
        "
      # console.log master_statement

      @dbRepo.query(master_statement)   
        .success ()=> 
          console.log "[DATA_SET_CONTROLLER] #{new Date()} consolidated records successful" +
            "\r\n\tdata_set: #{@set_name}" +
            "\r\n\trepo_name: #{repo_name} "
          callback?()
        .error (e)=>
          console.log "[DATA_SET_CONTROLLER] #{new Date()} consolidated records failed #{e}" +
            "\r\n\tERROR: #{e}" +
            "\r\n\tdata_set: #{@set_name}" +
            "\r\n\trepo_name: #{repo_name} "            


        # @clearBatches repo_name, batches, num_of_batches, ()=>
        #   @copyBatches repo_name, batches, num_of_batches, ()=>
        #     callback && callback()

  clearAll : (repo_name, callback)->
    del_query = @clearBatchesQuery repo_name, []
    @dbRepo.query(del_query)   
      .success ()=> 
        console.log "[DATA_SET_CONTROLLER] #{new Date()} cleared all records successful" +
          "\r\n\tdata_set: #{@set_name}" +
          "\r\n\trepo_name: #{repo_name} "
        callback?()
      .error (e)=>
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