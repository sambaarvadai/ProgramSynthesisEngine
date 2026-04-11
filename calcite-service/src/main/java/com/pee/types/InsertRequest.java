package com.pee.types;

import java.util.List;
import java.util.Map;

public class InsertRequest {
    public List<TableSchema> schema;
    public String table;
    public List<String> columns;           // dynamic columns from row
    public Map<String, Object> staticValues; // literal values
    public String mode;                    // "insert", "insert_ignore", "upsert"
    public List<String> conflictColumns;   // for upsert
    public List<String> updateColumns;     // for upsert
    public String dialect;
}
