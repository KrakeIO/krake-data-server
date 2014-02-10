ktk = require 'krake-toolkit'
krakeSchema = ktk.schema.krake
QueryHelper = ktk.query.helper

class KrakeModel
  
  constructor : (@dbRepo, @repo_name, callback)->
    @hstore_col = "properties"
    @common_cols = ["createdAt", "updatedAt", "pingedAt", @hstore_col]
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
  

  getInsertStatement : (data_obj)->
    insert_keys_string = @common_cols.map((column)=>
      '"' + column + '"'
    ).join(",")

    insert_value_string = @common_cols.map((column)=>
      switch column
        when "createdAt", "updatedAt", "pingedAt"
          if data_obj[column] && typeof(data_obj[column]) == 'string' then data_obj[column]
          else if data_obj[column] && typeof(data_obj[column]) == 'object' && data_obj[column].getMonth()
            @getFormattedDate data_obj[column]
          else 
            @getFormattedDate new Date()
        when @hstore_col
          @getHstoreValues data_obj

    ).map((column)=>
      "'" + column + "'"
    ).join(",")

    query_string =  'INSERT INTO "' + @repo_name + '"' +
                    ' (' + insert_keys_string + ')' +
                    ' VALUES ' +
                    ' (' + insert_value_string + ')'


  getFormattedDate : (date_obj)->
    d = date_obj 
    d.getFullYear() + "-"  +  (d.getMonth() + 1)  + "-" + d.getDate()  + " " + d.getHours() + ":"  +  d.getMinutes()  + ":" + d.getSeconds()

  getHstoreValues : (data_obj)->
    return false if Object.keys(data_obj).length == 0
    Object.keys(data_obj).filter((column)=>
        column not in @common_cols
      ).map((column)=>
        '"' + column.replace(/"/, '\\\"') + '" => "' + data_obj[column].replace(/"/, '\\\"') + '"'
      ).join(",")


  getSelectStatement : (query_obj)->
    query_string =  'SELECT ' + @selectClause(query_obj) + 
                    ' FROM "' + @repo_name + '" ' + 
                    ' WHERE ' + (@whereClause(query_obj) || 'true')

    
    if order_clause = @orderClause query_obj then query_string += ' ORDER BY ' + order_clause 
    if query_obj.$limit && query_obj.$limit > 0 then query_string += ' LIMIT ' + query_obj.$limit
    if query_obj.$offset && query_obj.$offset > 0 then query_string += ' OFFSET ' + query_obj.$offset
    query_string

  colName : (column)->
    if column in @common_cols 
      '"' + column.replace(/"/, '\\\"') + '"'
    else 
      @hstore_col + "::hstore->'" + column.replace(/'/, '\\\'') + "'"

  colLabel : (column)->
    '"' + column.replace(/'/, '\\\'') + '"'

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
              'distinct cast(' + @colName(column[operator]) + ' as text) as ' + @colLabel(column[operator])

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

  whereClause : (query_obj)->
    return false unless query_obj.$where && query_obj.$where.length > 0

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

      if(col_name not in @common_cols) && (col_name not in ["$and", "$or"]) then sub_query = @hstore_col + "->" + sub_query  
      query.push sub_query
      
    query.join(" and ")

  orderClause : (query_obj)->
    return false unless query_obj.$order && query_obj.$order.length > 0
    query_obj.$order.map((column)=>
      operator = Object.keys(column)[0]
      switch operator
        when "$asc" then @colName(column[operator]) + " asc" 
        when "$desc" then @colName(column[operator]) + " desc" 
    ).join(",")

module.exports = KrakeModel