package com.pee.types;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import com.fasterxml.jackson.annotation.JsonAlias;

public class UpdateRequest {
    public List<TableSchema> schema;
    public String table;
    public List<String> setColumns;       // dynamic SET columns (from input rows)
    public Map<String, Object> staticSets; // static SET values (literal)
    private Map<String, String> sqlExprSets = new HashMap<>(); // SQL expressions for SET (e.g. { updated_at: "NOW()" })
    public List<String> whereColumns;      // dynamic WHERE columns (from input rows)
    
    @JsonAlias({"whereFilters", "staticWhere"})
    public Map<String, Object> staticWhere; // static WHERE conditions e.g. { id: 107 }

    public String dialect;

    public Map<String, String> getSqlExprSets() { return sqlExprSets; }
    public void setSqlExprSets(Map<String, String> sqlExprSets) {
      this.sqlExprSets = sqlExprSets;
    }
}
