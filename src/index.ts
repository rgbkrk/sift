/**
 * pretext-table — public API
 *
 * React component:
 *   import { PretextTable } from 'pretext-table'
 *   <PretextTable url="/data.arrow" onChange={handleState} />
 *
 * Imperative engine:
 *   import { createTable } from 'pretext-table'
 *   const engine = createTable(container, tableData)
 *
 * State serialization:
 *   import { engineStateToExplorerState, predicateToSQL } from 'pretext-table'
 */

// React component
export { PretextTable, usePretextEngine } from './react'
export type { PretextTableProps, PretextTableHandle } from './react'

// Imperative engine
export { createTable } from './table'
export type {
  TableEngine,
  TableEngineState,
  TableEngineOptions,
  TableData,
  Column,
  ColumnType,
  ColumnSummary,
  NumericColumnSummary,
  CategoricalColumnSummary,
  BooleanColumnSummary,
  TimestampColumnSummary,
  ColumnFilter,
  RangeFilter,
  SetFilter,
  BooleanFilter,
} from './table'

// Accumulators (for custom data pipelines)
export {
  detectColumnType,
  refineColumnType,
  isNullSentinel,
  formatCell,
  NumericAccumulator,
  TimestampAccumulator,
  CategoricalAccumulator,
  BooleanAccumulator,
} from './accumulators'
export type { SummaryAccumulator } from './accumulators'

// Filter schema & state serialization
export {
  columnFiltersToPredicates,
  explorerStateToJSON,
  predicateToSQL,
  predicateToPandas,
  predicateToEnglish,
} from './filter-schema'
export type {
  ExplorerState,
  SortEntry,
  FilterPredicate,
  ColumnPredicate,
  CompoundPredicate,
  NotPredicate,
  BetweenPredicate,
  EqPredicate,
  InPredicate,
  ContainsPredicate,
  IsNullPredicate,
} from './filter-schema'
