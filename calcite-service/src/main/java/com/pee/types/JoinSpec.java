package com.pee.types;

public class JoinSpec {
    public String table;
    public String kind;    // "INNER", "LEFT", etc.
    public String onLeft;
    public String onRight;
}
