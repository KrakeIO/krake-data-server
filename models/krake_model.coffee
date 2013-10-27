ktk = require 'krake-toolkit'
krakeSchema = ktk.schema.krake
QueryHelper = ktk.query.helper

class KrakeModel

  constructor : (@dbHandler, @krakeHandle, callback)->
    @Krake = @dbHandler.define 'krakes', krakeSchema
    gotKrakes = (krakes)=>
      for x in [0... krakes.length]
        current_krake = krakes[x]
        curr_qh = new QueryHelper(current_krake.content)
        @columns = curr_qh.getFilteredColumns() || curr_qh.getColumns() 
        @url_columns = curr_qh.getUrlColumns() 
        callback && callback curr_qh.is_valid
  
    couldNotGetKrakes = (error_msg)->
      callback && callback false, error_msg

    # Ensures only 1 Krake definition is retrieved given a krake handle
    @Krake.findAll({ where : { handle : @krakeHandle }, limit: 1 }).success(gotKrakes).error(couldNotGetKrakes)
    
    
  # @Descriptions: get the columns part of a postgresql query given a set of columns
  # @param: columns:array
  # @param: prefix:string
  # @return: columns_in_query:string
  getColumnsQuery : (prefix)->
    prefix = prefix || ""
    columns_in_query = @columns.map((column)->
      "properties::hstore->'" + column.replace(/'/, '\\\'') + "' as \"" + prefix + column.replace(/""/, '\\\""') + "\" \r\n"
    ).join(",")    

module.exports = KrakeModel