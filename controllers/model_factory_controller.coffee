schemaConfig            = require('krake-toolkit').schema.config 
krakeSchema             = require('krake-toolkit').schema.krake
dataSetSchema           = require('krake-toolkit').schema.data_set

class ModelFactoryController

  constructor : (@dbSystem, callback)->
    @Krake    = @dbSystem.define 'krakes', krakeSchema
    @DataSet  = @dbSystem.define 'data_sets', dataSetSchema
    @dbSystem.sync().done ()=>
      callback && callback()

  isKrake : (handle, callback)->
    query =
      where: 
        handle: handle

    @Krake.count(query)
      .success (count)=>
        is_true = count > 0
        callback && callback is_true

      .error (error)=>
        console.log "Error: %s ", error
        callback && callback(false)

  isDataSet : (handle, callback)->
    query =
      where: 
        handle: handle

    @DataSet.count(query)
      .success (count)=>
        is_true = count > 0
        callback && callback is_true

      .error (error)=>
        console.log "Error: %s ", error
        callback && callback(false)

module.exports = ModelFactoryController