ktk = require 'krake-toolkit'
krakeSchema = ktk.schema.krake
QueryHelper = ktk.query.helper

class KrakeModel
  
  constructor : (@dbRepo, @repo_name, callback)->

    @common_cols = ["createdAt", "updatedAt", "pingedAt"]
    @Krake = @dbRepo.define 'krakes', krakeSchema

    gotKrakes = (krakes)=>
      for x in [0... krakes.length]
        current_krake = krakes[x]
        curr_qh = new QueryHelper(current_krake.content)
        if curr_qh.getFilteredColumns() && curr_qh.getFilteredColumns().length > 0
          @columns = curr_qh.getFilteredColumns()
        else if curr_qh.getColumns() && curr_qh.getColumns().length > 0
          @columns = curr_qh.getColumns()
        else
          @columns = []

        @url_columns = curr_qh.getUrlColumns()
        callback && callback curr_qh.is_valid

      if krakes.length == 0 
        @columns = []
        callback && callback(false, "No records were found")
  
    couldNotGetKrakes = (error_msg)->
      callback && callback false, error_msg

    # Ensures only 1 Krake definition is retrieved given a krake handle
    @Krake.findAll({ where : { handle : @repo_name }, limit: 1 }).success(gotKrakes).error(couldNotGetKrakes)
    
  getQuery : (query_obj)->
    query_string = 'SELECT ' + @selectClause(query_obj) + 
      ' FROM "' + @repo_name + '" ' + 
      ' WHERE ' + @whereClause(query_obj)

  selectClause : (query_obj)->
    query = ""
    sel_cols = query_obj.$select || @columns
    

    if sel_cols.length > 0
      query = sel_cols.map((column)->
        "properties::hstore->'" + column.replace(/'/, '\\\'') + "' as \"" + column.replace(/""/, '\\\""') + "\""
      ).join(",") 
      query += ","
    
    query += @common_cols.map((col)->
      JSON.stringify(col)
    ).join(",")

    query

  whereClause : (query_obj)->
    return "true" unless query_obj.$where 

    query = ['true']
    for condition_obj in query_obj.$where then do (condition_obj)=>
      col_name = Object.keys(condition_obj)[0]
      switch typeof(condition_obj[col_name])

        when "string" # Operators : = 
          if col_name in @common_cols
            query.push "'" + col_name + "'" + " = '" + condition_obj[col_name] + "'"
          else
            query.push "properties->'" + col_name + "'" + " = '" + condition_obj[col_name] + "'"

        when "array" # Operators : $and, $or 
          console.log "is an array"

        when "object" # Operators : $contains, $gt, $gte, $lt, $lte, $ne, $exist
          operator = Object.keys(condition_obj[col_name])[0]
      
    query.join(" and ")

  limitClause : (query_obj)->

  skipClause : (query_obj)->


module.exports = KrakeModel