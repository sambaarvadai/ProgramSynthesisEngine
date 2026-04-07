export type {
  ColumnConfig,
  ForeignKeyConfig,
  TableConfig,
  SchemaConfig
} from './schema-config.js';

export {
  getTable,
  getRowSchema,
  findJoinPath,
  getRelatedTables,
  tableExists,
  UnknownTableError,
  NoJoinPathError
} from './schema-config.js';
