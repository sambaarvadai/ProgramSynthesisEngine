package com.pee.types;

import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAlias;

public class UpdateRequest {
    public List<TableSchema> schema;
    public String table;
    public List<String> setColumns;       // dynamic SET columns (from input rows)
    public Map<String, Object> staticSets; // static SET values (literal)
    public List<String> whereColumns;      // dynamic WHERE columns (from input rows)
    
    @JsonAlias({"whereFilters", "staticWhere"})
    public Map<String, Object> staticWhere; // static WHERE conditions e.g. { id: 107 }
    
    public String dialect;
}
