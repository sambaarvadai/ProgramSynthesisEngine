package com.pee.types;

import java.util.List;

public class CompileResult {
    public String sql;
    public List<String> paramColumns;      // field names mapped to $1, $2...
    public List<Object> staticParams;      // literal values appended
    public List<String> optimizations;
    public String dialect;
}
