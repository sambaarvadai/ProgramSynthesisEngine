package com.pee.types;

public class FilterSpec {
    public String field;
    public String table;
    public String operator;  // "=", "!=", "<", ">", "IN", "NOT IN", "IS NULL", etc.
    public Object value;     // scalar, array, or subquery string
}
