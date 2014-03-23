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

  copyMostRecent2Batches : (repo_name, callback)->
    @getRepoBatches repo_name, (batches)=>
      if !batches || batches.length == 0
        callback && callback()

      query = ""
      if batches.length >= 1
        query += 'INSERT INTO "' + @set_name + '" ("properties", "datasource_handle", "pingedAt", "createdAt", "updatedAt") ' +
          '  SELECT ' +
          '    "properties",' +
          '    \'' + repo_name + '\' as "datasource_handle",' +
          '    \'' + batches[0] + '\' as "pingedAt",' +
          '    \'' + batches[0] + '\' as "createdAt",' +
          '    \'' + batches[0] + '\' as "updatedAt"' +
          '  FROM "' + repo_name + '" ' +
          '  WHERE ' +
          '   "pingedAt" = \'' + batches[0] + '\' ;'
          "\n\n"

      if batches.length >= 2
        query += 'INSERT INTO "' + @set_name + '" ("properties", "datasource_handle", "pingedAt", "createdAt", "updatedAt") ' +
          '  SELECT ' +
          '    "properties",' +
          '    \'' + repo_name + '\' as "datasource_handle",' +
          '    \'' + batches[1] + '\' as "pingedAt",' +
          '    \'' + batches[1] + '\' as "createdAt",' +
          '    \'' + batches[1] + '\' as "updatedAt"' +
          '  FROM "' + repo_name + '" ' +
          '  WHERE ' +
          '   "pingedAt" = \'' + batches[1] + '\';'

      @dbRepo.query(query).success ()->
        callback && callback()




module.exports = DataSetController