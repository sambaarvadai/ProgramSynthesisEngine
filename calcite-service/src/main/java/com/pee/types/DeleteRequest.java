package com.pee.types;

import java.util.List;

public class DeleteRequest {
    public List<TableSchema> schema;
    public String table;
    public List<String> whereColumns;
    public List<FilterSpec> whereFilters;
    public String dialect;
}
