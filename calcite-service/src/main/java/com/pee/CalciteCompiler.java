package com.pee;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.*;
import org.apache.calcite.plan.volcano.*;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.rel2sql.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.validate.*;
import org.apache.calcite.sql2rel.*;
import org.apache.calcite.tools.*;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.SchemaPlus;

import com.pee.types.*;
import java.util.*;

public class CalciteCompiler {

  public CompileResult compileSelect(SelectRequest req) throws Exception {
    // 1. Build Calcite schema from request tables
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);
    
    for (TableSchema table : req.schema) {
      rootSchema.add(table.name, buildCalciteTable(table));
    }

    // 2. Build SQL string from request
    // Calcite parses SQL -> validates against schema -> optimizes -> emits SQL
    String inputSQL = buildSelectSQL(req);
    
    // 3. Parse and validate
    FrameworkConfig config = Frameworks.newConfigBuilder()
      .defaultSchema(rootSchema)
      .sqlValidatorConfig(SqlValidator.Config.DEFAULT
        .withIdentifierExpansion(true))
      .parserConfig(SqlParser.Config.DEFAULT
        .withCaseSensitive(true))
      .build();
    
    Planner planner = Frameworks.getPlanner(config);
    SqlNode parsed = planner.parse(inputSQL);
    SqlNode validated = planner.validate(parsed);
    RelRoot relRoot = planner.rel(validated);
    RelNode rel = relRoot.project();

    // 4. Apply optimization rules
    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addRuleCollection(Arrays.asList(
      CoreRules.FILTER_INTO_JOIN,
      CoreRules.JOIN_CONDITION_PUSH,
      CoreRules.FILTER_REDUCE_EXPRESSIONS,
      CoreRules.PROJECT_REMOVE,
      CoreRules.AGGREGATE_PROJECT_MERGE
    ));
    HepPlanner hepPlanner = new HepPlanner(programBuilder.build());
    hepPlanner.setRoot(rel);
    RelNode optimized = hepPlanner.findBestExp();

    // 5. Emit SQL for target dialect
    SqlDialect dialect = getDialect(req.dialect);
    RelToSqlConverter converter = new RelToSqlConverter(dialect);
    SqlImplementor.Result result = converter.visitRoot(optimized);
    SqlNode sqlNode = result.asStatement();
    
    String sql = sqlNode.toSqlString(dialect).getSql();
    
    CompileResult out = new CompileResult();
    out.sql = sql;
    out.paramColumns = List.of();  // Calcite emits literals inline
    out.staticParams = List.of();
    out.optimizations = List.of("calcite_hep_optimizer");
    out.dialect = req.dialect != null ? req.dialect : "POSTGRESQL";
    
    return out;
  }

  public CompileResult compileInsert(InsertRequest req) throws Exception {
    // Build parameterized INSERT SQL directly
    // Calcite validates column names against schema
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    for (TableSchema table : req.schema) {
      rootSchema.add(table.name, buildCalciteTable(table));
    }
    
    // Validate that columns exist in the table
    TableSchema targetTable = req.schema.stream()
      .filter(t -> t.name.equals(req.table))
      .findFirst()
      .orElseThrow(() -> new Exception("Table not found: " + req.table));
    
    Set<String> validColumns = new HashSet<>();
    for (ColumnDef col : targetTable.columns) {
      validColumns.add(col.name);
    }
    
    List<String> allColumns = new ArrayList<>(req.columns);
    if (req.staticValues != null) {
      allColumns.addAll(req.staticValues.keySet());
    }
    
    for (String col : allColumns) {
      if (!validColumns.contains(col)) {
        throw new Exception("Column '" + col + "' does not exist in table '" + req.table + "'");
      }
    }
    
    // Build parameterized SQL
    List<String> paramColumns = new ArrayList<>(req.columns);
    List<Object> staticParams = new ArrayList<>();
    List<String> valuePlaceholders = new ArrayList<>();
    
    for (int i = 0; i < req.columns.size(); i++) {
      valuePlaceholders.add("$" + (i + 1));
    }
    
    int paramOffset = req.columns.size();
    if (req.staticValues != null) {
      for (Map.Entry<String, Object> entry : req.staticValues.entrySet()) {
        staticParams.add(entry.getValue());
        valuePlaceholders.add("$" + (++paramOffset));
      }
    }
    
    String colList = String.join(", ", allColumns.stream().map(c -> "\"" + c + "\"").toList());
    String valList = String.join(", ", valuePlaceholders);
    
    String sql = "INSERT INTO \"" + req.table + "\" (" + colList + ") VALUES (" + valList + ")";
    
    if ("insert_ignore".equals(req.mode)) {
      sql += " ON CONFLICT DO NOTHING";
    } else if ("upsert".equals(req.mode) && req.conflictColumns != null) {
      String conflictCols = String.join(", ", req.conflictColumns.stream().map(c -> "\"" + c + "\"").toList());
      List<String> updateCols = req.updateColumns != null ? req.updateColumns :
        allColumns.stream().filter(c -> !req.conflictColumns.contains(c)).toList();
      String updateClause = String.join(", ", updateCols.stream()
        .map(c -> "\"" + c + "\" = EXCLUDED.\"" + c + "\"").toList());
      sql += " ON CONFLICT (" + conflictCols + ") DO UPDATE SET " + updateClause;
    }
    
    CompileResult out = new CompileResult();
    out.sql = sql;
    out.paramColumns = paramColumns;
    out.staticParams = staticParams;
    out.optimizations = List.of("calcite_schema_validation");
    out.dialect = req.dialect != null ? req.dialect : "POSTGRESQL";
    
    return out;
  }

  public CompileResult compileUpdate(UpdateRequest req) throws Exception {
    // Validate and build parameterized UPDATE SQL
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    for (TableSchema table : req.schema) {
      rootSchema.add(table.name, buildCalciteTable(table));
    }
    
    TableSchema targetTable = req.schema.stream()
      .filter(t -> t.name.equals(req.table))
      .findFirst()
      .orElseThrow(() -> new Exception("Table not found: " + req.table));
    
    Set<String> validColumns = new HashSet<>();
    for (ColumnDef col : targetTable.columns) validColumns.add(col.name);
    
    // Validate all referenced columns
    for (String col : req.setColumns) {
      if (!validColumns.contains(col)) throw new Exception("SET column '" + col + "' not in table");
    }
    for (String col : req.whereColumns) {
      if (!validColumns.contains(col)) throw new Exception("WHERE column '" + col + "' not in table");
    }
    
    List<String> params = new ArrayList<>();
    List<String> paramColumns = new ArrayList<>();
    
    // SET clause
    List<String> setClauses = new ArrayList<>();
    for (String col : req.setColumns) {
      paramColumns.add(col);
      params.add("$" + params.size() + 1);
      setClauses.add("\"" + col + "\" = $" + params.size());
    }
    if (req.staticSets != null) {
      for (Map.Entry<String, Object> e : req.staticSets.entrySet()) {
        // Inline literals for static values
        String val = e.getValue() == null ? "NULL" :
          e.getValue() instanceof String ? "'" + e.getValue() + "'" :
          e.getValue().toString();
        setClauses.add("\"" + e.getKey() + "\" = " + val);
      }
    }
    if (req.getSqlExprSets() != null) {
      for (Map.Entry<String, String> e : req.getSqlExprSets().entrySet()) {
        // Embed SQL expressions as raw SQL (not quoted)
        setClauses.add("\"" + e.getKey() + "\" = " + e.getValue());
      }
    }
    
    // WHERE clause - build from whereColumns + staticWhere
    List<String> whereClauses = new ArrayList<>();
    List<Object> staticParams = new ArrayList<>();

    // Dynamic WHERE (from input rows)
    for (String col : req.whereColumns) {
      paramColumns.add(col);
      whereClauses.add("\"" + col + "\" = $" + paramColumns.size());
    }

    // Static WHERE (literal values)
    if (req.staticWhere != null) {
      for (Map.Entry<String, Object> e : req.staticWhere.entrySet()) {
        if (!validColumns.contains(e.getKey())) {
          throw new Exception("WHERE column '" + e.getKey() + "' not in table");
        }
        
        Object val = e.getValue();
        String column = "\"" + e.getKey() + "\"";
        
        if (val instanceof List) {
          List<?> list = (List<?>) val;
          if (list.isEmpty()) continue;
          
          if (list.size() == 1) {
            // Single element — use scalar equality
            whereClauses.add(column + " = " + formatLiteral(list.get(0)));
          } else {
            // Multiple elements — use IN (...) which Calcite handles cleanly
            List<String> literals = new ArrayList<>();
            for (Object item : list) {
              literals.add(formatLiteral(item));
            }
            whereClauses.add(column + " IN (" + String.join(", ", literals) + ")");
          }
        } else {
          // Scalar value — existing behaviour
          whereClauses.add(column + " = " + formatLiteral(e.getValue()));
        }
      }
    }

    if (whereClauses.isEmpty()) {
      throw new Exception("UPDATE requires at least one WHERE condition");
    }

    String sql = "UPDATE \"" + req.table + "\" SET " +
      String.join(", ", setClauses) +
      " WHERE " + String.join(" AND ", whereClauses);
    
    CompileResult out = new CompileResult();
    out.sql = sql;
    out.paramColumns = paramColumns;
    out.staticParams = staticParams;  // Include WHERE clause parameters from predicateToSQL
    out.optimizations = List.of("calcite_schema_validation");
    out.dialect = req.dialect != null ? req.dialect : "POSTGRESQL";
    return out;
  }

  public CompileResult compileDelete(DeleteRequest req) throws Exception {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    for (TableSchema table : req.schema) {
      rootSchema.add(table.name, buildCalciteTable(table));
    }
    
    TableSchema targetTable = req.schema.stream()
      .filter(t -> t.name.equals(req.table))
      .findFirst()
      .orElseThrow(() -> new Exception("Table not found: " + req.table));
    
    Set<String> validColumns = new HashSet<>();
    for (ColumnDef col : targetTable.columns) validColumns.add(col.name);
    
    List<String> paramColumns = new ArrayList<>();
    List<String> whereClauses = new ArrayList<>();
    List<Object> staticParams = new ArrayList<>();
    
    // Dynamic WHERE (from input rows)
    if (req.whereColumns != null) {
      for (int i = 0; i < req.whereColumns.size(); i++) {
        if (!validColumns.contains(req.whereColumns.get(i))) {
          throw new Exception("WHERE column '" + req.whereColumns.get(i) + "' not in table");
        }
        paramColumns.add(req.whereColumns.get(i));
        whereClauses.add("\"" + req.whereColumns.get(i) + "\" = $" + paramColumns.size());
      }
    }
    
    // Static WHERE (literal values from whereFilters)
    if (req.whereFilters != null) {
      for (FilterSpec f : req.whereFilters) {
        if (!validColumns.contains(f.field)) {
          throw new Exception("WHERE column '" + f.field + "' not in table");
        }
        String column = "\"" + f.field + "\"";
        
        if (f.value instanceof List) {
          List<?> list = (List<?>) f.value;
          if (list.isEmpty()) continue;
          
          if (list.size() == 1) {
            whereClauses.add(column + " = " + formatLiteral(list.get(0)));
          } else {
            List<String> literals = new ArrayList<>();
            for (Object item : list) {
              literals.add(formatLiteral(item));
            }
            whereClauses.add(column + " IN (" + String.join(", ", literals) + ")");
          }
        } else {
          whereClauses.add(column + " " + f.operator + " " + formatLiteral(f.value));
        }
      }
    }
    
    if (whereClauses.isEmpty()) {
      throw new Exception("DELETE requires at least one WHERE condition");
    }
    
    String sql = "DELETE FROM \"" + req.table + "\" WHERE " +
      String.join(" AND ", whereClauses);
    
    CompileResult out = new CompileResult();
    out.sql = sql;
    out.paramColumns = paramColumns;
    out.staticParams = staticParams;
    out.optimizations = List.of("calcite_schema_validation");
    out.dialect = req.dialect != null ? req.dialect : "POSTGRESQL";
    return out;
  }

  private AbstractTable buildCalciteTable(TableSchema table) {
    return new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (ColumnDef col : table.columns) {
          RelDataType type = mapType(typeFactory, col.type);
          builder.add(col.name, type).nullable(col.nullable);
        }
        return builder.build();
      }
    };
  }

  private RelDataType mapType(RelDataTypeFactory f, String type) {
    return switch (type.toUpperCase()) {
      case "INTEGER", "INT", "SERIAL" -> f.createSqlType(SqlTypeName.INTEGER);
      case "BIGINT" -> f.createSqlType(SqlTypeName.BIGINT);
      case "NUMERIC", "DECIMAL" -> f.createSqlType(SqlTypeName.DECIMAL);
      case "BOOLEAN" -> f.createSqlType(SqlTypeName.BOOLEAN);
      case "TIMESTAMP", "TIMESTAMPTZ" -> f.createSqlType(SqlTypeName.TIMESTAMP);
      case "DATE" -> f.createSqlType(SqlTypeName.DATE);
      default -> f.createSqlType(SqlTypeName.VARCHAR);
    };
  }

  private String quoteIdentifier(String dotted) {
    // "customers.id" -> "\"customers\".\"id\""
    String[] parts = dotted.split("\\.", 2);
    if (parts.length == 2) {
      return "\"" + parts[0] + "\".\"" + parts[1] + "\"";
    }
    return "\"" + dotted + "\"";
  }

  private String buildSelectSQL(SelectRequest req) {
    StringBuilder sb = new StringBuilder("SELECT ");
    
    List<String> cols = new ArrayList<>();
    for (ColumnSpec col : req.columns) {
      String field = col.table != null ?
        "\"" + col.table + "\".\"" + col.field + "\"" :
        "\"" + col.field + "\"";
      if (col.agg != null) field = col.agg + "(" + field + ")";
      if (col.alias != null) field += " AS \"" + col.alias + "\"";
      cols.add(field);
    }
    sb.append(String.join(", ", cols));
    sb.append(" FROM \"").append(req.table).append("\"");
    
    if (req.joins != null) {
      for (JoinSpec join : req.joins) {
        sb.append(" ").append(join.kind != null ? join.kind : "INNER")
          .append(" JOIN \"").append(join.table).append("\"")
          .append(" ON ").append(quoteIdentifier(join.onLeft))
          .append(" = ").append(quoteIdentifier(join.onRight));
      }
    }
    
    if (req.filters != null && !req.filters.isEmpty()) {
      sb.append(" WHERE ");
      List<String> conditions = new ArrayList<>();
      for (FilterSpec f : req.filters) {
        String field = f.table != null ?
          "\"" + f.table + "\".\"" + f.field + "\"" : "\"" + f.field + "\"";
        if ("NOT IN".equals(f.operator) && f.value instanceof String s
            && s.trim().toLowerCase().startsWith("select")) {
          conditions.add(field + " NOT IN (" + s.trim() + ")");
        } else if ("IS NULL".equals(f.operator)) {
          conditions.add(field + " IS NULL");
        } else if ("IS NOT NULL".equals(f.operator)) {
          conditions.add(field + " IS NOT NULL");
        } else {
          String val = f.value instanceof String ? "'" + f.value + "'" : String.valueOf(f.value);
          conditions.add(field + " " + f.operator + " " + val);
        }
      }
      sb.append(String.join(" AND ", conditions));
    }
    
    if (req.groupBy != null && !req.groupBy.isEmpty()) {
      sb.append(" GROUP BY ").append(String.join(", ",
        req.groupBy.stream().map(g -> "\"" + g + "\"").toList()));
    }
    
    if (req.orderBy != null && !req.orderBy.isEmpty()) {
      sb.append(" ORDER BY ");
      List<String> orders = new ArrayList<>();
      for (OrderBySpec ob : req.orderBy) {
        String field = ob.table != null ?
          "\"" + ob.table + "\".\"" + ob.field + "\"" : "\"" + ob.field + "\"";
        orders.add(field + " " + (ob.direction != null ? ob.direction : "ASC"));
      }
      sb.append(String.join(", ", orders));
    }
    
    if (req.limit != null) sb.append(" LIMIT ").append(req.limit);
    if (req.offset != null) sb.append(" OFFSET ").append(req.offset);
    
    return sb.toString();
  }

  private String formatLiteral(Object val) {
    if (val == null) return "NULL";
    if (val instanceof String) return "'" + ((String) val).replace("'", "''") + "'";
    if (val instanceof Boolean) return val.toString().toUpperCase();
    if (val instanceof List) {
      // Should not reach here directly — handled in caller
      // But as fallback, format as IN list
      List<?> list = (List<?>) val;
      List<String> parts = new ArrayList<>();
      for (Object item : list) parts.add(formatLiteral(item));
      return "(" + String.join(", ", parts) + ")";
    }
    return val.toString();  // numbers inline
  }

  private SqlDialect getDialect(String dialect) {
    if (dialect == null || dialect.equalsIgnoreCase("POSTGRESQL")) {
      return PostgresqlSqlDialect.DEFAULT;
    }
    return PostgresqlSqlDialect.DEFAULT;
  }
}
