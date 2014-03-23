ktk           = require 'krake-toolkit'
DataSetSchema = ktk.schema.record_set
KrakeModel    = require '../models/krake_model'
KrakeSetModel    = require '../models/krake_set_model'

class DataSetController
  constructor : (@dbSystem, @dbRepo, @set_name, callback)->  

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

  clearMostRecent2Batches : (repo_name, callback)->

    @getRepoBatches repo_name, (batches)=>
      if !batches 
        callback && callback()

      else if batches.length > 0
        batches_to_clear = batches.slice(0,2)
        batch_and_clause = batches_to_clear.map((batch)->
          "\"pingedAt\"='" + batch + "'"          
        ).join(" or ")

        del_query = 'delete from  "'+ @set_name + '"' +
          " where " +
          "\"datasource_handle\"='" + repo_name + "' and " +
          "(" + batch_and_clause + ")"

        @dbRepo.query(del_query).success ()->
          callback && callback()
      
      else if batches.length == 0
        callback && callback()



module.exports = DataSetController