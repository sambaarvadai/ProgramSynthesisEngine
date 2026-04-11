#!/usr/bin/env python3
"""
Substrait Plan Generator
Uses the real Python substrait package to generate protobuf plans
"""

import json
import sys
import struct
import substrait.plan_pb2 as plan_pb2
import substrait.type_pb2 as type_pb2
import substrait.algebra_pb2 as algebra_pb2
# import substrait.extension_registry as extension_registry  # Skip due to antlr4 dependency
from google.protobuf import json_format

class SubstraitGenerator:
    def __init__(self):
        self.function_registry = {
            'COUNT': 0,
            'SUM': 1,
            'AVG': 2,
            'MIN': 3,
            'MAX': 4,
            'equal': 100,
            'not_equal': 101,
            'less': 102,
            'less_or_equal': 103,
            'greater': 104,
            'greater_or_equal': 105,
            'add': 106,
            'subtract': 107,
            'multiply': 108,
            'divide': 109,
            'and': 110,
            'or': 111,
            'not': 112,
            'is_null': 113,
            'like': 114
        }
        
        self.extension_uris = [
            'https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml',
            'https://github.com/substrait-io/substrait/blob/main/extensions/functions_comparison.yaml',
            'https://github.com/substrait-io/substrait/blob/main/extensions/functions_logical.yaml'
        ]

    def translate_query_plan(self, query_plan_json):
        """Translate a QueryPlan JSON to Substrait protobuf"""
        try:
            query_plan = json.loads(query_plan_json)
        except json.JSONDecodeError:
            # If it's already a dict, use it directly
            if isinstance(query_plan_json, dict):
                query_plan = query_plan_json
            else:
                raise ValueError("Invalid JSON input")
        
        # Create Substrait plan
        plan = plan_pb2.Plan()
        # Note: version might be a different field in the protobuf definition
        
        # Skip extensions for now to avoid protobuf complexity
        # In a real implementation, we'd need to create proper extension messages
        
        # Translate the root node
        root_rel = self._translate_node(query_plan['root'], query_plan['nodes'])
        
        # Add relation to plan
        plan_rel = plan.relations.add()
        plan_rel.rel.CopyFrom(root_rel)
        # PlanRel doesn't have names field, that's for RelRoot
        
        return plan

    def _translate_node(self, node_id, nodes):
        """Translate a QueryDAGNode to Substrait Rel"""
        node = nodes[node_id]
        
        if node['kind'] == 'Scan':
            return self._translate_scan(node['payload'])
        elif node['kind'] == 'Filter':
            return self._translate_filter(node, nodes)
        elif node['kind'] == 'Join':
            return self._translate_join(node, nodes)
        elif node['kind'] == 'Agg':
            return self._translate_aggregate(node, nodes)
        elif node['kind'] == 'Project':
            return self._translate_project(node, nodes)
        elif node['kind'] == 'Sort':
            return self._translate_sort(node, nodes)
        elif node['kind'] == 'Limit':
            return self._translate_fetch(node, nodes)
        else:
            raise ValueError(f"Unsupported node kind: {node['kind']}")

    def _translate_scan(self, scan_node):
        """Translate Scan node to ReadRel"""
        rel = algebra_pb2.Rel()
        read_rel = algebra_pb2.ReadRel()
        read_rel.base_schema.CopyFrom(self._translate_schema(scan_node['schema']))
        
        if 'predicate' in scan_node and scan_node['predicate']:
            read_rel.filter.CopyFrom(self._translate_expression(scan_node['predicate']))
        
        rel.read.CopyFrom(read_rel)
        return rel

    def _translate_filter(self, node, nodes):
        """Translate Filter node to FilterRel"""
        rel = algebra_pb2.Rel()
        filter_rel = algebra_pb2.FilterRel()
        filter_rel.input.CopyFrom(self._translate_node(node['input'], nodes))
        filter_rel.condition.CopyFrom(self._translate_expression(node['predicate']))
        rel.filter.CopyFrom(filter_rel)
        return rel

    def _translate_join(self, node, nodes):
        """Translate Join node to JoinRel"""
        rel = algebra_pb2.Rel()
        join_rel = algebra_pb2.JoinRel()
        join_rel.left.CopyFrom(self._translate_node(node['left'], nodes))
        join_rel.right.CopyFrom(self._translate_node(node['right'], nodes))
        join_rel.type = self._translate_join_type(node['payload']['kind'])
        join_rel.expression.CopyFrom(self._translate_expression(node['payload']['on']))
        rel.join.CopyFrom(join_rel)
        return rel

    def _translate_aggregate(self, node, nodes):
        """Translate Aggregate node to AggregateRel"""
        rel = algebra_pb2.Rel()
        agg_rel = algebra_pb2.AggregateRel()
        agg_rel.input.CopyFrom(self._translate_node(node['input'], nodes))
        
        # Add groupings
        for key in node['keys']:
            grouping = agg_rel.groupings.add()
            grouping.grouping_expressions.append(self._translate_expression(key))
        
        # Add measures
        for agg in node['aggregations']:
            measure = agg_rel.measures.add()
            measure.measure.function_reference = self.function_registry.get(agg['fn'], 0)
            measure.measure.args.append(self._translate_expression(agg['expr']))
        
        rel.aggregate.CopyFrom(agg_rel)
        return rel

    def _translate_project(self, node, nodes):
        """Translate Project node to ProjectRel"""
        rel = algebra_pb2.Rel()
        project_rel = algebra_pb2.ProjectRel()
        project_rel.input.CopyFrom(self._translate_node(node['input'], nodes))
        
        for col in node['columns']:
            project_rel.expressions.append(self._translate_expression(col['expr']))
        
        rel.project.CopyFrom(project_rel)
        return rel

    def _translate_sort(self, node, nodes):
        """Translate Sort node to SortRel"""
        rel = algebra_pb2.Rel()
        sort_rel = algebra_pb2.SortRel()
        sort_rel.input.CopyFrom(self._translate_node(node['input'], nodes))
        
        for key in node['keys']:
            sort_field = sort_rel.sorts.add()
            sort_field.expr.CopyFrom(self._translate_expression(key['expr']))
            sort_field.direction = self._translate_sort_direction(key['direction'], key.get('nulls', 'LAST'))
        
        rel.sort.CopyFrom(sort_rel)
        return rel

    def _translate_fetch(self, node, nodes):
        """Translate Limit node to FetchRel"""
        rel = algebra_pb2.Rel()
        fetch_rel = algebra_pb2.FetchRel()
        fetch_rel.input.CopyFrom(self._translate_node(node['input'], nodes))
        fetch_rel.count = node['count']
        if 'offset' in node:
            fetch_rel.offset = node['offset']
        
        rel.fetch.CopyFrom(fetch_rel)
        return rel

    def _translate_expression(self, expr):
        """Translate ExprAST to Substrait Expression"""
        if expr['kind'] == 'Literal':
            return self._translate_literal(expr['value'])
        elif expr['kind'] == 'FieldRef':
            return self._translate_field_reference(expr['field'])
        elif expr['kind'] == 'BinaryOp':
            return self._translate_binary_op(expr['op'], expr['left'], expr['right'])
        elif expr['kind'] == 'FunctionCall':
            return self._translate_function_call(expr['name'], expr['args'])
        elif expr['kind'] == 'Conditional':
            return self._translate_conditional(expr['condition'], expr['then'], expr['else'])
        elif expr['kind'] == 'IsNull':
            return self._translate_is_null(expr['expr'])
        elif expr['kind'] == 'In':
            return self._translate_in(expr['expr'], expr['values'])
        else:
            raise ValueError(f"Unsupported expression kind: {expr['kind']}")

    def _translate_literal(self, value):
        """Translate literal value to Substrait Literal"""
        expr = algebra_pb2.Expression()
        literal = expr.literal
        
        if value is None:
            literal.null.null_type = type_pb2.NullType.NULL_NULL
        elif isinstance(value, bool):
            literal.boolean = value
        elif isinstance(value, int):
            literal.i32 = value
        elif isinstance(value, float):
            literal.fp64 = value
        elif isinstance(value, str):
            literal.string = value
        else:
            literal.string = str(value)
        
        return expr

    def _translate_field_reference(self, field):
        """Translate field reference to Substrait FieldReference"""
        expr = algebra_pb2.Expression()
        field_ref = expr.selection.direct_reference.struct_field
        field_ref.field = 0  # TODO: Look up field index in schema
        return expr

    def _translate_binary_op(self, op, left, right):
        """Translate binary operation to Substrait ScalarFunction"""
        expr = algebra_pb2.Expression()
        scalar_func = expr.scalar_function
        scalar_func.function_reference = self.function_registry.get(self._op_to_function_name(op), 0)
        scalar_func.args.append(self._translate_expression(left))
        scalar_func.args.append(self._translate_expression(right))
        return expr

    def _translate_function_call(self, name, args):
        """Translate function call to Substrait ScalarFunction"""
        expr = algebra_pb2.Expression()
        scalar_func = expr.scalar_function
        scalar_func.function_reference = self.function_registry.get(name, 0)
        for arg in args:
            scalar_func.args.append(self._translate_expression(arg))
        return expr

    def _translate_conditional(self, condition, then_expr, else_expr):
        """Translate conditional to Substrait IfThen"""
        expr = algebra_pb2.Expression()
        if_then = expr.if_then
        if_then.if_clause.CopyFrom(self._translate_expression(condition))
        if_then.then_clause.CopyFrom(self._translate_expression(then_expr))
        if_then.else_clause.CopyFrom(self._translate_expression(else_expr))
        return expr

    def _translate_is_null(self, expr):
        """Translate is null to Substrait ScalarFunction"""
        expr_result = algebra_pb2.Expression()
        scalar_func = expr_result.scalar_function
        scalar_func.function_reference = self.function_registry['is_null']
        scalar_func.args.append(self._translate_expression(expr))
        return expr_result

    def _translate_in(self, expr, values):
        """Translate IN operation to Substrait SingularOrList"""
        expr_result = algebra_pb2.Expression()
        singular_or_list = expr_result.singular_or_list
        singular_or_list.value.CopyFrom(self._translate_expression(expr))
        for value in values:
            singular_or_list.options.append(self._translate_expression(value))
        return expr_result

    def _translate_schema(self, schema):
        """Translate RowSchema to Substrait NamedStruct"""
        named_struct = type_pb2.NamedStruct()
        named_struct.names.extend([col['name'] for col in schema['columns']])
        
        struct = named_struct.struct
        for col in schema['columns']:
            data_type = struct.types.add()
            type_kind = self._engine_type_to_substrait_type(col['type'])
            if type_kind == 'string':
                # Create string type - just set the string field
                data_type.string.type_variation_reference = 0
                data_type.string.nullability = 0
            elif type_kind == 'fp64':
                # Create fp64 type - just set the fp64 field
                pass  # fp64 is primitive, no additional fields
            elif type_kind == 'bool':
                # Create bool type - just set the bool field
                pass  # bool is primitive, no additional fields
            else:
                # fallback to string
                data_type.string.type_variation_reference = 0
                data_type.string.nullability = 0
        
        return named_struct

    def _engine_type_to_substrait_type(self, engine_type):
        """Map engine type to Substrait type"""
        if engine_type.get('kind') == 'string':
            return 'string'
        elif engine_type.get('kind') == 'number':
            return 'fp64'
        elif engine_type.get('kind') == 'boolean':
            return 'bool'
        elif engine_type.get('kind') == 'null':
            return 'null'
        else:
            return 'string'

    def _op_to_function_name(self, op):
        """Map binary operator to function name"""
        op_map = {
            '+': 'add',
            '-': 'subtract',
            '*': 'multiply',
            '/': 'divide',
            '=': 'equal',
            '!=': 'not_equal',
            '<': 'less',
            '>': 'greater',
            '<=': 'less_or_equal',
            '>=': 'greater_or_equal',
            'AND': 'and',
            'OR': 'or',
            'LIKE': 'like'
        }
        return op_map.get(op, op)

    def _translate_join_type(self, kind):
        """Translate join type to Substrait JoinRel.JoinType"""
        join_type_map = {
            'INNER': algebra_pb2.JoinRel.JoinType.JOIN_TYPE_INNER,
            'LEFT': algebra_pb2.JoinRel.JoinType.JOIN_TYPE_LEFT,
            'RIGHT': algebra_pb2.JoinRel.JoinType.JOIN_TYPE_RIGHT,
            'FULL': algebra_pb2.JoinRel.JoinType.JOIN_TYPE_OUTER
        }
        return join_type_map.get(kind, algebra_pb2.JoinRel.JoinType.JOIN_TYPE_INNER)

    def _translate_sort_direction(self, direction, nulls):
        """Translate sort direction to Substrait SortField.SortDirection"""
        if direction == 'ASC':
            return algebra_pb2.SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST if nulls == 'LAST' else algebra_pb2.SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_FIRST
        else:
            return algebra_pb2.SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_LAST if nulls == 'LAST' else algebra_pb2.SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_FIRST

    def serialize_to_binary(self, plan):
        """Serialize Substrait plan to binary"""
        return plan.SerializeToString()

    def serialize_to_json(self, plan):
        """Serialize Substrait plan to JSON (for debugging)"""
        return json_format.MessageToJson(plan, indent=2)

def main():
    """CLI interface for testing"""
    if len(sys.argv) < 2:
        print("Usage: python substrait-generator.py <query_plan_json>")
        sys.exit(1)
    
    generator = SubstraitGenerator()
    
    try:
        with open(sys.argv[1], 'r') as f:
            query_plan_json = f.read()
        
        plan = generator.translate_query_plan(query_plan_json)
        
        # Output binary to stdout
        binary_data = generator.serialize_to_binary(plan)
        sys.stdout.buffer.write(binary_data)
        
        # Also output JSON for debugging to stderr
        json_output = generator.serialize_to_json(plan)
        sys.stderr.write(json_output)
        
    except Exception as e:
        sys.stderr.write(f"Error: {e}\n")
        sys.exit(1)

if __name__ == "__main__":
    main()
