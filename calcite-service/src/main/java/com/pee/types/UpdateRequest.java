package com.pee.types;

import java.util.List;
import java.util.Map;

public class UpdateRequest {
    public List<TableSchema> schema;
    public String table;
    public List<String> setColumns;
    public Map<String, Object> staticSets;
    public List<String> whereColumns;
    public Map<String, Object> staticWhere;
    public List<FilterSpec> whereFilters;
    public String dialect;
}
