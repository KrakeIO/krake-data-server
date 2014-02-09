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

        @columns = @columns || []

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
    query_string =  'SELECT ' + @selectClause(query_obj) + 
                    ' FROM "' + @repo_name + '" ' + 
                    ' WHERE ' + (@whereClause(query_obj) || 'true')

  selectClause : (query_obj)->

    sel_cols = query_obj.$select || @columns
    
    query = sel_cols.map((column)=>
      switch typeof(column) 
        when "string" # Operator : simple column select
          query_section = @colName(column)
          query_section += " as " + @colLabel(column) unless column in @common_cols
          query_section

        when "object" # Operator : $count, $distinct, $max, $min
          operator = Object.keys(column)[0]
          switch operator
            when "$count"
              "count(" + @colName(column[operator]) + ") as " + @colLabel(column[operator]) 

            when "$distinct"
              'distinct cast(' + @colName(column[operator]) + ' as text) as "' + column[operator].replace(/'/, '\\\'') + '"'

            when "$max"
              "max(" + @colName(column[operator]) + ") as " + @colLabel(column[operator]) 

            when "$min"
              "min(" + @colName(column[operator]) + ") as " + @colLabel(column[operator]) 


    ).join(",")

    # query += "," unless !sel_cols || sel_cols.length == 0
    # query += @common_cols.filter((col)=>
    #     col not in sel_cols.map ((sel_col)=>
    #       switch typeof(sel_col)
    #         when "string" then sel_col
    #         when "object" then sel_col[Object.keys(sel_col)[0]]
    #     )
    #   ).map((col)->
    #     JSON.stringify(col)
    #   ).join(",")
    query = "1" if sel_cols.length == 0
    query

  colName : (column)->
    if column in @common_cols 
      '"' + column.replace(/'/, '\\\'') + '"'
    else 
      "properties::hstore->'" + column.replace(/'/, '\\\'') + "'"

  colLabel : (column)->
    '"' + column.replace(/'/, '\\\'') + '"'

  whereClause : (query_obj)->
    return "" unless query_obj.$where 

    query = []
    for condition_obj in query_obj.$where then do (condition_obj)=>
      col_name = Object.keys(condition_obj)[0]
      sub_query = ""
      switch typeof(condition_obj[col_name])
        when "string" # Operators : =
          sub_query = "'" + col_name + "'" + " = '" + condition_obj[col_name] + "'"

        when "object"
          if condition_obj[col_name] instanceof Array  # Operators : $and, $or
            switch col_name
              when "$or"
                sub_query  = "(" + condition_obj[col_name].map((sub_condition)=>
                  @whereClause({ $where : [sub_condition] })
                ).join(" or ") + ")"

              when "$and" 
                sub_query = "(" + @whereClause({ $where : condition_obj[col_name] }) + ")"

          else  # Operators : $contains, $gt, $gte, $lt, $lte, $ne, $exist
            operator = Object.keys(condition_obj[col_name])[0]
            switch operator
              when "$contains"
                sub_query = "'" + col_name + "'" + " like '%" + condition_obj[col_name][operator] + "%'"

              when "$gt"
                sub_query = "'" + col_name + "'" + " > '" + condition_obj[col_name][operator] + "'"

              when "$gte"
                sub_query = "'" + col_name + "'" + " >= '" + condition_obj[col_name][operator] + "'"

              when "$lt"
                sub_query = "'" + col_name + "'" + " < '" + condition_obj[col_name][operator] + "'"

              when "$lte"
                sub_query = "'" + col_name + "'" + " <= '" + condition_obj[col_name][operator] + "'"

              when "$ne"
                sub_query = "'" + col_name + "'" + " != '" + condition_obj[col_name][operator] + "'"

              when "$exist"
                sub_query = "'" + col_name + "'" + " not NULL"

      if(col_name not in @common_cols) && (col_name not in ["$and", "$or"]) then sub_query = "properties->" + sub_query  
      query.push sub_query
      
    query.join(" and ")

  limitClause : (query_obj)->

  skipClause : (query_obj)->


module.exports = KrakeModel