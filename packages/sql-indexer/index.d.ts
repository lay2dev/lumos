import {
  Indexer as BaseIndexer,
  IndexerOptions,
  CellCollector as BaseCellCollector,
} from "@ckb-lumos/indexer";
import Knex from "knex";
import { QueryOptions } from "@ckb-lumos/base";

export type BlockListener = (block) => void;

export class Indexer extends BaseIndexer {
  constructor(uri: string, knex: Knex, options?: IndexerOptions, blockListener?:BlockListener);
}

export declare class CellCollector extends BaseCellCollector {
  constructor(knex: Knex, queries: QueryOptions);
}
