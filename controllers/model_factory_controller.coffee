schemaConfig            = require('krake-toolkit').schema.config 
krakeSchema             = require('krake-toolkit').schema.krake
dataSetSchema           = require('krake-toolkit').schema.data_set
KrakeModel              = require './../models/krake_model'
KrakeSetModel           = require './../models/krake_set_model'

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
      .then (count)=>
        console.log count
        is_true = count > 0
        callback && callback is_true

      .catch (error)=>
        console.log "Error: %s ", error
        callback && callback(false)

  isDataSet : (handle, callback)->
    query =
      where: 
        handle: handle

    @DataSet.count(query)
      .then (count)=>
        is_true = count > 0
        callback && callback is_true

      .catch (error)=>
        console.log "Error: %s ", error
        callback && callback(false)

  # Given a handle determines if the handle references a Krake or a DataSet
  #   thereafter returning the corresponding Model
  #
  # Params:
  #   handle: String
  #   callback: Function
  #     Model -> Krake || KrakeSet
  #
  getModel: (handle, callback)->
    @isKrake handle, (found)=>
      if !found then return
      console.log "\n[DATA_SERVER] #{new Date()} data source type detected — #{handle}"
      callback KrakeModel

    @isDataSet handle, (found)=>
      if !found then return
      console.log "\n[DATA_SERVER] #{new Date()} data set type detected — #{handle}"
      callback KrakeSetModel

module.exports = ModelFactoryController