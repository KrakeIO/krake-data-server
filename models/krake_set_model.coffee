schemaConfig            = require('krake-toolkit').schema.config 
QueryHelper             = require('krake-toolkit').query.helper
krakeSchema             = require('krake-toolkit').schema.krake
dataSetSchema           = require('krake-toolkit').schema.data_set
dataSetKrakeSchema      = require('krake-toolkit').schema.data_set_krake
dataSetKrakeRuleSchema  = require('krake-toolkit').schema.data_set_krake_rule

class KrakeSetModel
  
  constructor : (@dbSystem, @set_name, @columns, callback)->
    @krakes = []
    @hstore_col = "properties"
    @handle_col = "datasource_handle"
    @status_cols = ["createdAt", "updatedAt", "pingedAt"]
    @common_cols = @status_cols.concat([@hstore_col]).concat([@handle_col])

    @sync ()=>    
      @columns = @columns || []

      @status_cols.forEach (curr_col)=>
        if curr_col not in @columns then @columns.push curr_col

      if @handle_col not in @columns then @columns.push @handle_col
      
      callback && callback true

  sync : (callback)->
    @Krake            = @dbSystem.define 'krakes', krakeSchema, schemaConfig
    @DataSet          = @dbSystem.define 'data_sets', dataSetSchema, schemaConfig
    @DataSetKrake     = @dbSystem.define 'data_set_krakes', dataSetKrakeSchema, schemaConfig
    @DataSetKrakeRule = @dbSystem.define 'data_set_krake_rules', dataSetKrakeRuleSchema, schemaConfig

    @DataSet.hasMany @Krake, { through: @DataSetKrake}
    @Krake.hasMany @DataSet, { through: @DataSetKrake}

    @DataSetKrakeRule.belongsTo @DataSetKrake
    @DataSetKrake.hasMany @DataSetKrakeRule, { as: "data_set_krake_rule", foreignKey: 'data_set_krake_id'}

    @dbSystem.sync().done ()=>
      @loadKrakes(callback)

  loadKrakes : (callback)->
    query =
      where: 
        handle: @set_name
      include: [@Krake]
      limit: 1

    @DataSet
      .findAll(query)
      .success (@dataset_objs)=>
        if @dataset_objs.length == 0
          callback && callback()
        else
          @dataset_obj = @dataset_objs[0]
          @krakes = @dataset_objs[0].krakes
          @setFullColumns()
          callback && callback @krakes

      .error (error)=>
        callback && callback()

  setFullColumns : ()->
    @columns = @columns || []
    @url_columns = @url_columns || []      

    if @krakes.length > 0
      for current_krake in @krakes
        curr_qh = new QueryHelper(current_krake.content)

        if curr_qh.getFilteredColumns() && curr_qh.getFilteredColumns().length > 0
          for curr_col in curr_qh.getFilteredColumns()
            if curr_col not in @columns then @columns.push curr_col  

        else if curr_qh.getColumns() && curr_qh.getColumns().length > 0
          for curr_col in curr_qh.getColumns()
            if curr_col not in @columns then @columns.push curr_col            

        if curr_qh.getUrlColumns() && curr_qh.getUrlColumns() > 0
          for curr_col in curr_qh.getUrlColumns()
            if curr_col not in @url_columns then @url_columns.push curr_col

    @status_cols.forEach (curr_col)=>
      if curr_col not in @columns then @columns.push curr_col

        
 
  

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

        when @handle_col
          data_obj[column] || ""

        when @hstore_col
          @getHstoreValues data_obj

    ).map((column)=>
      "'" + column + "'"
    ).join(",")

    query_string =  'INSERT INTO "' + @set_name + '"' +
                    ' (' + insert_keys_string + ')' +
                    ' VALUES ' +
                    ' (' + insert_value_string + ')'


  getFormattedDate : (date_obj)->
    d = date_obj 
    d.getFullYear() + "-"  +  (d.getMonth() + 1)  + "-" + d.getDate()  + " " + d.getHours() + ":"  +  d.getMinutes()  + ":" + d.getSeconds()

  getHstoreValues : (data_obj)->
    return false if Object.keys(data_obj).length == 0
    Object.keys(data_obj).filter((column)=>
        column = column.replace(/"/, '&#34;').replace(/'/, '&#39;')
        column not in @common_cols
      ).map((column)=>
        '"' + column.replace(/"/, '&#34;').replace(/'/, '&#39;') + '" => "' + data_obj[column].replace(/"/, '&#34;').replace(/'/, '&#39;') + '"'
      ).join(",")


  getSelectStatement : (query_obj)->
    query_string =  'SELECT ' + @selectClause(query_obj) + 
                    ' FROM "' + @set_name + '" ' + 
                    ' WHERE ' + (@whereClause(query_obj) || 'true')

    
    if order_clause = @orderClause query_obj then query_string += ' ORDER BY ' + order_clause 
    if query_obj.$limit && query_obj.$limit > 0 then query_string += ' LIMIT ' + query_obj.$limit
    if query_obj.$offset && query_obj.$offset > 0 then query_string += ' OFFSET ' + query_obj.$offset
    query_string

  hstoreColName: (column)->
    @hstore_col + "::hstore->'" + column + "'"

  timeStampColName: (timestamp_column)->
    'to_char("' + timestamp_column + '", \'YYYY-MM-DD HH24:MI:SS\')'

  simpleColName: (column)->
    column = column.replace(/"/, '&#34;').replace(/'/, '&#39;')
    if column in @common_cols && column != @handle_col
      @timeStampColName column

    else if column == @handle_col
      column
      
    else 
      @hstoreColName column

  compoundColName : (column)->
    column = column.replace(/"/, '&#34;').replace(/'/, '&#39;')
    if column in @common_cols 
      '"' + column + '"'
    else 
      @hstoreColName column

  colLabel : (column)->
    column = column.replace(/"/, '&#34;').replace(/'/, '&#39;')    
    '"' + column + '"'

  selectClause : (query_obj)->

    sel_cols = query_obj.$select || @columns
    if !@hasAggregate query_obj
      @status_cols.forEach (curr_col)=>
        if curr_col not in sel_cols then sel_cols.push curr_col
    
    query = sel_cols.map((column)=>
      switch typeof(column) 
        when "string" # Operator : simple column select
          query_section = @simpleColName(column)
          query_section += " as " + @colLabel(column)
          query_section

        when "object" # Operator : $count, $distinct, $max, $min
          operator = Object.keys(column)[0]
          switch operator
            when "$count"
              "count(" + @compoundColName(column[operator]) + ") as " + @colLabel(column[operator]) 

            when "$distinct"
              'distinct cast(' + @compoundColName(column[operator]) + ' as text) as ' + @colLabel(column[operator])

            when "$max"
              "max(" + @compoundColName(column[operator]) + ") as " + @colLabel(column[operator]) 

            when "$min"
              "min(" + @compoundColName(column[operator]) + ") as " + @colLabel(column[operator]) 


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

  hasAggregate : (query_obj)->
    return false if !query_obj.$select
    query_obj.$select.filter((curr_col)=>
      typeof(curr_col) == "object"
    ).length > 0


  whereClause : (query_obj)->
    return false unless query_obj.$where && query_obj.$where.length > 0

    query = []
    for condition_obj in query_obj.$where then do (condition_obj)=>
      col_name = Object.keys(condition_obj)[0]
      sub_query = ""
      switch typeof(condition_obj[col_name])
        when "string" # Operators : =
          sub_query = @compoundColName(col_name) + " = '" + condition_obj[col_name] + "'"

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
                sub_query = @compoundColName(col_name) + " like '%" + condition_obj[col_name][operator] + "%'"

              when "$gt"
                sub_query = @compoundColName(col_name) + " > '" + condition_obj[col_name][operator] + "'"

              when "$gte"
                sub_query = @compoundColName(col_name) + " >= '" + condition_obj[col_name][operator] + "'"

              when "$lt"
                sub_query = @compoundColName(col_name) + " < '" + condition_obj[col_name][operator] + "'"

              when "$lte"
                sub_query = @compoundColName(col_name) + " <= '" + condition_obj[col_name][operator] + "'"

              when "$ne"
                sub_query = @compoundColName(col_name) + " != '" + condition_obj[col_name][operator] + "'"

              when "$exist"
                sub_query = @compoundColName(col_name) + " not NULL"

      query.push sub_query
    query.join(" and ")

  orderClause : (query_obj)->
    return false unless query_obj.$order && query_obj.$order.length > 0
    query_obj.$order.map((column)=>
      operator = Object.keys(column)[0]
      switch operator
        when "$asc" then @compoundColName(column[operator]) + " asc" 
        when "$desc" then @compoundColName(column[operator]) + " desc" 
    ).join(",")

module.exports = KrakeSetModel