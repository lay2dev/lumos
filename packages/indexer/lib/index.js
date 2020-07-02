const { validators, normalizers, Reader, RPC } = require("ckb-js-toolkit");
const { OrderedSet } = require("immutable");
const XXHash = require("xxhash");
const { Indexer: NativeIndexer } = require("../native");

function defaultLogger(level, message) {
  console.log(`[${level}] ${message}`);
}

class Indexer {
  constructor(
    uri,
    path,
    {
      pollIntervalSeconds = 2,
      livenessCheckIntervalSeconds = 5,
      logger = defaultLogger,
    } = {}
  ) {
    this.uri = uri;
    this.livenessCheckIntervalSeconds = livenessCheckIntervalSeconds;
    this.logger = logger;
    this.nativeIndexer = new NativeIndexer(uri, path, pollIntervalSeconds);
  }

  running() {
    return this.nativeIndexer.running();
  }

  start() {
    return this.nativeIndexer.start();
  }

  stop() {
    return this.nativeIndexer.stop();
  }

  async tip() {
    return this.nativeIndexer.tip();
  }

  _getLiveCellsByScript(script, scriptType, argsLen, returnRawBuffer) {
    return this.nativeIndexer.getLiveCellsByScript(
      normalizers.NormalizeScript(script),
      scriptType,
      argsLen,
      returnRawBuffer
    );
  }

  _getTransactionsByScriptIterator(script, scriptType) {
    return this.nativeIndexer.getTransactionsByScriptIterator(
      normalizers.NormalizeScript(script),
      scriptType,
      ioType
    );
  }

  startForever() {
    this.nativeIndexer.start();
    setInterval(() => {
      if (!this.nativeIndexer.running()) {
        this.logger(
          "error",
          "Native indexer has stopped, maybe check the log?"
        );
        this.nativeIndexer.start();
      }
    }, this.livenessCheckIntervalSeconds * 1000);
  }

  collector({ lock = null, type = null, argsLen = -1, data = "0x" } = {}) {
    return new CellCollector(this, { lock, type, argsLen, data });
  }
}

class BufferValue {
  constructor(buffer) {
    this.buffer = buffer;
  }

  equals(other) {
    return (
      new Reader(this.buffer).serializeJson() ===
      new Reader(other.buffer).serializeJson()
    );
  }

  hashCode() {
    return XXHash.hash(Buffer.from(this.buffer), 0);
  }
}

class CellCollector {
  // if data left null, means every data content is ok
  constructor(
    indexer,
    { lock = null, type = null, argsLen = -1, data = "0x" } = {}
  ) {
    if (!lock && !type) {
      throw new Error("Either lock or type script must be provided!");
    }
    if (lock) {
      validators.ValidateScript(lock);
    }
    if (type && typeof type === "object") {
      validators.ValidateScript(type);
    }
    this.indexer = indexer;
    this.lock = lock;
    this.type = type;
    this.data = data;
    this.argsLen = argsLen;
  }

  // TODO: optimize this
  async count() {
    let result = 0;
    const c = this.collect();
    while (true) {
      const item = await c.next();
      if (item.done) {
        break;
      }
      result += 1;
    }
    return result;
  }

  async *collect() {
    if (this.lock && this.type && typeof this.type === "object") {
      let lockOutPoints = new OrderedSet();
      for (const o of this.indexer._getLiveCellsByScript(
        this.lock,
        0,
        this.argsLen,
        true
      )) {
        lockOutPoints = lockOutPoints.add(new BufferValue(o));
      }

      let typeOutPoints = new OrderedSet();
      for (const o of this.indexer._getLiveCellsByScript(
        this.type,
        1,
        this.argsLen,
        true
      )) {
        typeOutPoints = typeOutPoints.add(new BufferValue(o));
      }
      const outPoints = lockOutPoints.intersect(typeOutPoints);
      for (const o of outPoints) {
        const cell = this.indexer.nativeIndexer.getDetailedLiveCell(o.buffer);
        if (this.data && cell.data !== this.data) {
          continue;
        }
        yield cell;
      }
    } else {
      const script = this.lock || this.type;
      const scriptType = !!this.lock ? 0 : 1;
      for (const o of this.indexer._getLiveCellsByScript(
        script,
        scriptType,
        this.argsLen,
        true
      )) {
        const cell = this.indexer.nativeIndexer.getDetailedLiveCell(o);
        if (
          cell &&
          scriptType === 1 &&
          this.type === "empty" &&
          cell.cell_output.type
        ) {
          continue;
        }
        if (cell && this.data && cell.data !== this.data) {
          continue;
        }
        yield cell;
      }
    }
  }
}

// Notice this TransactionCollector implementation only uses indexer
// here. Since the indexer we use doesn't store full transaction data,
// we will have to run CKB RPC queries on each tx hash to fetch transaction
// data. In some cases this might slow your app down. An ideal solution would
// be combining this with some cacher to accelerate this process.
class TransactionCollector {
  constructor(
    indexer,
    { input_lock = null, output_lock = null, input_type = null, output_type } = {},
    { skipMissing = false, includeStatus = true } = {}
  ) {
    if (!input_lock && !output_lock && !input_type && output_type) {
      throw new Error("Either lock or type script must be provided!");
    }
    if (input_lock) {
      validators.ValidateScript(input_lock);
    }
    if (output_lock) {
      validators.ValidateScript(output_lock);
    }
    if (input_type) {
      validators.ValidateScript(input_type);
    }
    if (output_type) {
      validators.ValidateScript(ouput_type);
    }
    this.indexer = indexer;
    this.input_lock = input_lock;
    this.output_lock = output_lock;
    this.input_type = input_type;
    this.output_type = output_type;
    this.skipMissing = skipMissing;
    this.includeStatus = includeStatus;
    this.rpc = new RPC(indexer.uri);
  }

  async count() {
    let hashes = new OrderedSet();
    if (this.input_lock) {
      const inputLockHashes = new OrderedSet(this.indexer._getTransactionsByScriptIterator(this.input_lock, 0, 0).collect());
      hashes = hashes.intersect(inputLockHashes);
    }

    if (this.output_lock) {
      const outputLockHashes = new OrderedSet(this.indexer._getTransactionsByScriptIterator(this.output_lock, 0, 1).collect());
      hashes = hashes.intersect(outputLockHashes);
    }

    if (this.input_type) {
      const inputTypeHashes = new OrderedSet(this.indexer._getTransactionsByScriptIterator(this.input_type, 1, 0).collect());
      hashes = hashes.intersect(inputTypeHashes);
    }

    if (this.output_type) {
      const outputTypeHashes = new OrderedSet(this.indexer._getTransactionsByScriptIterator(this.output_type, 1, 1).collect());
      hashes = hashes.intersect(outputTypeHashes);
    }

    return hashes.size;
  }

  async *collect() {
    let hashes = new OrderedSet();
    if (this.input_lock) {
      const inputLockHashes = new OrderedSet(this.indexer._getTransactionsByScriptIterator(this.input_lock, 0, 0).collect());
      hashes = hashes.intersect(inputLockHashes);
    }

    if (this.output_lock) {
      const outputLockHashes = new OrderedSet(this.indexer._getTransactionsByScriptIterator(this.output_lock, 0, 1).collect());
      hashes = hashes.intersect(outputLockHashes);
    }

    if (this.input_type) {
      const inputTypeHashes = new OrderedSet(this.indexer._getTransactionsByScriptIterator(this.input_type, 1, 0).collect());
      hashes = hashes.intersect(inputTypeHashes);
    }

    if (this.output_type) {
      const outputTypeHashes = new OrderedSet(this.indexer._getTransactionsByScriptIterator(this.output_type, 1, 1).collect());
      hashes = hashes.intersect(outputTypeHashes);
    }

    for (const h of hashes) {
      const tx = await this.rpc.get_transaction(hash);
      if (!this.skipMissing && !tx) {
        throw new Error(`Transaction ${h} is missing!`);
      }
      if (this.includeStatus) {
        yield tx;
      } else {
        yield tx.transaction;
      }
    }
  }
}

module.exports = {
  Indexer,
  CellCollector,
  TransactionCollector,
};
