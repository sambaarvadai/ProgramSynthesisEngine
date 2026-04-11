package com.pee.types;

import java.util.List;

public class TableSchema {
    public String name;
    public List<ColumnDef> columns;
    public List<String> primaryKey;
    public List<ForeignKey> foreignKeys;
}
