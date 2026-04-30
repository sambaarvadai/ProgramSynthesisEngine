// Mock Substrait types - replace with @substrait-io/substrait when available
// This follows the Substrait protobuf specification structure

export namespace substrait {
  export enum Any {
    TYPE_URL_NOT_SET = 0,
    TYPE_URL_NOT_SET_VALUE = 0
  }

  export enum SortField_SortDirection {
    ASC_NULLS_FIRST = 0,
    ASC_NULLS_LAST = 1,
    DESC_NULLS_FIRST = 2,
    DESC_NULLS_LAST = 3
  }

  export enum JoinRel_JoinType {
    JOIN_TYPE_UNSPECIFIED = 0,
    INNER = 1,
    LEFT = 2,
    RIGHT = 3,
    OUTER = 4,
    SEMI = 5,
    ANTI = 6
  }

  export interface NamedStruct {
    names: string[]
    struct: Struct
  }

  export interface Struct {
    types: Type[]
  }

  export interface Type {
    kind?: { [key: string]: any }
  }

  export interface Expression {
    literal?: Literal
    selection?: FieldReference
    scalarFunction?: ScalarFunction
    ifThen?: IfThen
    singularOrList?: SingularOrList
  }

  export interface Literal {
    boolean?: boolean
    i32?: number
    i64?: number
    fp64?: number
    string?: string
    null?: any
  }

  export interface FieldReference {
    directReference?: DirectReference
  }

  export interface DirectReference {
    structField?: StructField
  }

  export interface StructField {
    field: number
  }

  export interface ScalarFunction {
    functionReference: number
    args: Expression[]
  }

  export interface IfThen {
    ifClause: Expression
    thenClause: Expression
    elseClause: Expression
  }

  export interface SingularOrList {
    value: Expression
    options: Expression[]
  }

  export interface Rel {
    read?: ReadRel
    filter?: FilterRel
    join?: JoinRel
    aggregate?: AggregateRel
    project?: ProjectRel
    sort?: SortRel
    fetch?: FetchRel
  }

  export interface ReadRel {
    common: RelCommon
    baseSchema: NamedStruct
    filter?: Expression
    projection?: Projection
  }

  export interface FilterRel {
    input: Rel
    common: RelCommon
    condition: Expression
  }

  export interface JoinRel {
    left: Rel
    right: Rel
    common: RelCommon
    type: JoinRel_JoinType
    expression: Expression
    postJoinFilter?: Expression
  }

  export interface AggregateRel {
    input: Rel
    common: RelCommon
    groupings: AggregateRel_Grouping[]
    measures: AggregateRel_Measure[]
  }

  export interface AggregateRel_Grouping {
    groupingExpressions: Expression[]
  }

  export interface AggregateRel_Measure {
    measure: AggregateFunction
  }

  export interface AggregateFunction {
    functionReference: number
    args: Expression[]
  }

  export interface ProjectRel {
    input: Rel
    common: RelCommon
    expressions: Expression[]
  }

  export interface SortRel {
    input: Rel
    common: RelCommon
    sorts: SortField[]
  }

  export interface SortField {
    expr: Expression
    direction: SortField_SortDirection
  }

  export interface FetchRel {
    input: Rel
    common: RelCommon
    count: number
    offset?: number
  }

  export interface RelCommon {
    emit?: RelCommon_Emit
  }

  export interface RelCommon_Emit {
    outputMapping: number[]
  }

  export interface Projection {
    select?: Expression[]
    input?: Expression[]
  }

  export interface Plan {
    version: string
    relations: RelRoot[]
    extensions: Extension[]
  }

  export interface RelRoot {
    rel: Rel
    names: string[]
  }

  export interface Extension {
    extensionUri: string
  }

  export const Plan = {
    decode: (binary: Uint8Array): Plan => {
      // Mock implementation - in real protobuf this would decode binary
      try {
        const jsonStr = new TextDecoder().decode(binary)
        return JSON.parse(jsonStr) as Plan
      } catch {
        return {
          version: '1.0',
          relations: [],
          extensions: []
        }
      }
    }
  }
}
