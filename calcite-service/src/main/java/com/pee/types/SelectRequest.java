package com.pee.types;

import java.util.List;
import java.util.Map;

public class SelectRequest {
    public List<TableSchema> schema;   // all tables needed for the query
    public String table;               // primary table
    public List<ColumnSpec> columns;
    public List<JoinSpec> joins;
    public List<FilterSpec> filters;
    public List<String> groupBy;
    public List<AggSpec> aggregations;
    public List<FilterSpec> having;
    public List<OrderBySpec> orderBy;
    public Integer limit;
    public Integer offset;
    public String dialect;             // "POSTGRESQL" (default)
}
