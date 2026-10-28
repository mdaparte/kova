// Times one pass against a Kova that is already listening.
//
// Drops and recreates only the database `benchmark`. Prints progress,
// then one line that starts with BENCHMARK_JSON and a JSON object.
// scripts/benchmark.py reads that line, adds the process memory and
// the data directory size, and writes the documentation page.
//
//   make benchmark
//   make benchmark KOVA_PORT=27019 BENCH_ROWS=1000 BENCH_LOOKUPS=100

const rows = Math.max(1, Number(process.env.BENCH_ROWS || 1000));
let lookups = Math.max(1, Number(process.env.BENCH_LOOKUPS || 100));
if (lookups >= rows) {
    lookups = Math.max(1, rows - 1);
}

const database = db.getSiblingDB("benchmark");
const jobs = ["clerk", "analyst", "manager", "salesman"];
const departments = [
    { dept: 0, name: "accounting" },
    { dept: 1, name: "research" },
    { dept: 2, name: "sales" },
    { dept: 3, name: "operations" },
];

const COUNTERS = [
    "queries",
    "inserts",
    "updates",
    "deletes",
    "walWrites",
    "bufferPoolHits",
    "bufferPoolMisses",
    "bufferPoolEvictions",
    "indexLookups",
    "collectionScans",
    "bytesIn",
    "bytesOut",
    "transactionsStarted",
    "transactionsCommitted",
    "transactionsAborted",
];

const steps = [];

function fail(message) {
    print("FAIL " + message);
    print("BENCHMARK_JSON " + JSON.stringify({ ok: false, error: message, steps: steps }));
    quit(1);
}

function asInt(value) {
    return NumberInt(value);
}

function command(spec) {
    try {
        const reply = database.runCommand(spec);
        return reply || { ok: 0, errmsg: "no reply" };
    } catch (err) {
        return { ok: 0, errmsg: String(err.message || err) };
    }
}

function asCount(value) {
    if (value == null) return 0;
    if (typeof value === "number") return value;
    if (typeof value.toNumber === "function") return value.toNumber();
    const n = Number(value);
    return Number.isFinite(n) ? n : 0;
}

function snap() {
    const reply = command({ serverStatus: 1 });
    const out = {};
    for (const key of COUNTERS) {
        out[key] = asCount(reply[key]);
    }
    const exec = reply.metrics && reply.metrics.queryExecutor;
    if (exec && exec.scanned != null) {
        out.keysExamined = asCount(exec.scanned);
        if (reply.collectionScans == null && exec.collectionScans && exec.collectionScans.total != null) {
            out.collectionScans = asCount(exec.collectionScans.total);
        }
    }
    return out;
}

function run(lang, name, fn) {
    const before = snap();
    const started = Date.now();
    let info;
    try {
        info = fn() || {};
    } catch (err) {
        info = { ok: false, error: String(err.message || err), rows: 0 };
    }
    const ms = Date.now() - started;
    const after = snap();
    const counters = {};
    for (const key of COUNTERS) {
        counters[key] = after[key] - before[key];
    }
    if (Object.prototype.hasOwnProperty.call(before, "keysExamined")) {
        counters.keysExamined = after.keysExamined - before.keysExamined;
    }
    const step = {
        lang: lang,
        name: name,
        op: info.op || "",
        ms: ms,
        rows: info.rows == null ? 0 : info.rows,
        ok: info.ok !== false,
        error: info.error || "",
        note: info.note || "",
        counters: counters,
    };
    steps.push(step);
    print((step.ok ? "ok   " : "FAIL ") + lang + "  " + name + "  " + ms + " ms  rows " + step.rows);
    return step;
}

function fromCommand(reply, rowCount) {
    if (reply && reply.ok) {
        let rowsOut = rowCount;
        if (rowsOut == null && reply.cursor && reply.cursor.firstBatch) {
            rowsOut = reply.cursor.firstBatch.length;
        }
        if (rowsOut == null && reply.n != null) {
            rowsOut = Number(reply.n);
        }
        return { ok: true, rows: rowsOut || 0 };
    }
    return { ok: false, rows: 0, error: (reply && reply.errmsg) || "no reply" };
}

function sql(statement) {
    const reply = command({ sql: statement });
    return fromCommand(reply);
}

const hello = command({ hello: 1 });
if (!hello.ok) {
    fail(hello.errmsg || "the server did not answer hello");
}
if (String(hello.errmsg || "").includes("authentication required")) {
    fail("security is on; this script expects it off");
}

run("mql", "Clear benchmark", () => {
    const reply = command({ dropDatabase: 1 });
    if (reply.ok) {
        return { ok: true, rows: 1, note: "Removed a previous benchmark database.", op: "{ dropDatabase: 1 }" };
    }
    const message = String(reply.errmsg || "");
    if (/not exist|ns not found|database not found/i.test(message)) {
        return { ok: true, rows: 0, note: "No benchmark database yet.", op: "{ dropDatabase: 1 }" };
    }
    return fromCommand(reply, 0);
});

run("mql", "Create database benchmark", () => {
    const names = ["dept", "orders", "ledger"];
    for (const name of names) {
        const reply = command({ create: name });
        if (!reply.ok) {
            return fromCommand(reply, 0);
        }
    }
    return {
        ok: true,
        rows: names.length,
        note: "Creates dept, orders, and ledger. Nothing is assumed to exist.",
        op: "{ create: \"dept\" }\n{ create: \"orders\" }\n{ create: \"ledger\" }",
    };
});

const orders = [];
for (let i = 0; i < rows; i++) {
    const job = jobs[i % jobs.length];
    const doc = {
        sku: i,
        dept: i % 4,
        job: job,
        sal: 800 + (i % 40) * 100,
        qty: i % 50,
        note: job + " keeps the ledger " + i,
    };
    if (i % 10 === 0) {
        doc.bonus = i;
    }
    orders.push(doc);
}

function storedOrder(doc) {
    const row = {
        sku: asInt(doc.sku),
        dept: asInt(doc.dept),
        job: doc.job,
        sal: asInt(doc.sal),
        qty: asInt(doc.qty),
        note: doc.note,
    };
    if (doc.bonus != null) {
        row.bonus = asInt(doc.bonus);
    }
    return row;
}

run("mql", "insert departments", () => {
    const reply = database.dept.insertMany(departments.map((row) => ({
        dept: asInt(row.dept),
        name: row.name,
    })));
    return {
        ok: reply.acknowledged !== false,
        rows: departments.length,
        op: "db.dept.insertMany([{ dept, name }, ...])",
    };
});

run("mql", "insert orders", () => {
    const batch = 500;
    let inserted = 0;
    for (let offset = 0; offset < orders.length; offset += batch) {
        const slice = orders.slice(offset, offset + batch).map(storedOrder);
        const reply = database.orders.insertMany(slice);
        if (reply.acknowledged === false) {
            return { ok: false, rows: inserted, error: "insertMany was not acknowledged", op: "db.orders.insertMany(documents, batches of 500)" };
        }
        inserted += slice.length;
    }
    return {
        ok: true,
        rows: inserted,
        op: "db.orders.insertMany(documents, batches of 500)\n{ sku, dept, job, sal, qty, note, bonus on every 10th }",
    };
});

function index(name, spec) {
    run("mql", "create index " + name, () => {
        const reply = fromCommand(command({
            createIndexes: "orders",
            indexes: [spec],
        }), rows);
        reply.op = "{ createIndexes: \"orders\", indexes: " + JSON.stringify([spec]) + " }";
        return reply;
    });
}

index("sku_1 unique", { key: { sku: 1 }, name: "sku_1", unique: true });
index("sal_1", { key: { sal: 1 }, name: "sal_1" });
index("job_hashed", { key: { job: "hashed" }, name: "job_hashed" });
index("dept_1_sal_1", { key: { dept: 1, sal: 1 }, name: "dept_1_sal_1" });
index("bonus_1 sparse", { key: { bonus: 1 }, name: "bonus_1", sparse: true });

run("mql", "updateMany seen", () => {
    const reply = database.orders.updateMany({}, { $set: { seen: asInt(1) } });
    return {
        ok: reply.acknowledged !== false,
        rows: Number(reply.modifiedCount || reply.nModified || 0),
        op: "db.orders.updateMany({}, { $set: { seen: 1 } })",
    };
});

run("mql", "updateOne sku 1", () => {
    const reply = database.orders.updateOne({ sku: asInt(1) }, { $set: { note: "clerk keeps the amended ledger 1" } });
    return {
        ok: reply.acknowledged !== false,
        rows: Number(reply.modifiedCount || reply.nModified || 0),
        op: "db.orders.updateOne({ sku: 1 }, { $set: { note: \"clerk keeps the amended ledger 1\" } })",
    };
});

run("mql", "findAndModify sku 2", () => {
    const reply = fromCommand(command({
        findAndModify: "orders",
        query: { sku: asInt(2) },
        update: { $inc: { qty: 1 } },
    }), 1);
    reply.op = "{ findAndModify: \"orders\", query: { sku: 2 }, update: { $inc: { qty: 1 } } }";
    return reply;
});

run("mql", "deleteOne sku 0", () => {
    const reply = database.orders.deleteOne({ sku: asInt(0) });
    return {
        ok: reply.acknowledged !== false,
        rows: Number(reply.deletedCount || 0),
        op: "db.orders.deleteOne({ sku: 0 })",
    };
});

run("mql", "count orders", () => {
    const reply = fromCommand(command({ count: "orders" }));
    reply.op = "{ count: \"orders\" }";
    return reply;
});

run("mql", "create tickets with autoincrement", () => {
    const reply = fromCommand(command({
        create: "tickets",
        autoincrement: "id",
    }), 1);
    reply.op = "{ create: \"tickets\", autoincrement: \"id\" }";
    return reply;
});

run("mql", "insert tickets", () => {
    const docs = [];
    for (let i = 0; i < 50; i++) {
        docs.push({ kind: "bench" });
    }
    const reply = database.tickets.insertMany(docs);
    return {
        ok: reply.acknowledged !== false,
        rows: docs.length,
        op: "db.tickets.insertMany([{ kind: \"bench\" }, ...], 50 documents)",
    };
});

run("mql", "transaction commit", () => {
    const txn = NumberLong(1);
    const begun = command({ beginTransaction: 1, collection: "ledger", txnId: txn, isolation: "snapshot" });
    if (!begun.ok) {
        return fromCommand(begun, 0);
    }
    const inserted = command({
        insert: "ledger",
        documents: [{ sku: asInt(1), qty: asInt(1) }, { sku: asInt(2), qty: asInt(1) }],
        txnId: txn,
    });
    if (!inserted.ok) {
        return fromCommand(inserted, 0);
    }
    const reply = fromCommand(command({ commitTransaction: 1, collection: "ledger", txnId: txn }), 2);
    reply.op = "{ beginTransaction: 1, collection: \"ledger\", txnId: 1, isolation: \"snapshot\" }\n{ insert: \"ledger\", documents: [{ sku: 1, qty: 1 }, { sku: 2, qty: 1 }], txnId: 1 }\n{ commitTransaction: 1, collection: \"ledger\", txnId: 1 }";
    return reply;
});

run("mql", "transaction abort", () => {
    const txn = NumberLong(2);
    const begun = command({ beginTransaction: 1, collection: "ledger", txnId: txn, isolation: "snapshot" });
    if (!begun.ok) {
        return fromCommand(begun, 0);
    }
    const inserted = command({
        insert: "ledger",
        documents: [{ sku: asInt(9), qty: asInt(1) }],
        txnId: txn,
    });
    if (!inserted.ok) {
        return fromCommand(inserted, 0);
    }
    const reply = fromCommand(command({ abortTransaction: 1, collection: "ledger", txnId: txn }), 1);
    reply.op = "{ beginTransaction: 1, collection: \"ledger\", txnId: 2, isolation: \"snapshot\" }\n{ insert: \"ledger\", documents: [{ sku: 9, qty: 1 }], txnId: 2 }\n{ abortTransaction: 1, collection: \"ledger\", txnId: 2 }";
    return reply;
});

run("mql", "create view well_paid", () => {
    const reply = fromCommand(command({
        create: "well_paid",
        viewOn: "orders",
        pipeline: [{ $match: { sal: { $gt: asInt(2000) } } }],
    }), 1);
    reply.op = "{ create: \"well_paid\", viewOn: \"orders\", pipeline: [{ $match: { sal: { $gt: 2000 } } }] }";
    return reply;
});

run("mql", "create text view orders_search", () => {
    const reply = fromCommand(command({
        create: "orders_search",
        viewOn: "orders",
        pipeline: [],
        kova: { inverted: true, analyzer: "en_stem" },
    }), rows);
    reply.op = "{ create: \"orders_search\", viewOn: \"orders\", pipeline: [], kova: { inverted: true, analyzer: \"en_stem\" } }";
    return reply;
});

run("mql", "mapReduce orders", () => {
    const reply = fromCommand(command({
        mapReduce: "orders",
        query: { dept: asInt(1) },
        map: { key: "$job", value: "$sal" },
    }));
    reply.op = "{ mapReduce: \"orders\", query: { dept: 1 }, map: { key: \"$job\", value: \"$sal\" } }";
    return reply;
});

function pointFind(name) {
    run("mql", name, () => {
        let found = 0;
        for (let i = 1; i <= lookups; i++) {
            if (database.orders.findOne({ sku: asInt(i) }, { sku: 1 })) {
                found += 1;
            }
        }
        return {
            ok: found === lookups,
            rows: found,
            error: found === lookups ? "" : "matched " + found + " of " + lookups,
            note: lookups + " findOne by sku",
            op: "db.orders.findOne({ sku: NumberInt(n) }, { sku: 1 })\nn = 1.." + lookups,
        };
    });
}

pointFind("findOne sku, first pass");
pointFind("findOne sku, second pass");

run("mql", "find sal > 2000", () => {
    const found = database.orders.find({ sal: { $gt: asInt(2000) } }, { sku: 1, sal: 1 }).toArray();
    return {
        ok: true,
        rows: found.length,
        op: "db.orders.find({ sal: { $gt: 2000 } }, { sku: 1, sal: 1 })",
    };
});

run("mql", "find qty = 7", () => {
    const found = database.orders.find({ qty: asInt(7) }, { sku: 1 }).toArray();
    return {
        ok: true,
        rows: found.length,
        note: "qty has no index",
        op: "db.orders.find({ qty: 7 }, { sku: 1 })",
    };
});

run("mql", "aggregate group by job", () => {
    const found = database.orders.aggregate([
        { $group: { _id: "$job", n: { $sum: 1 }, payroll: { $sum: "$sal" } } },
    ]).toArray();
    return {
        ok: found.length === jobs.length,
        rows: found.length,
        op: "db.orders.aggregate([{ $group: { _id: \"$job\", n: { $sum: 1 }, payroll: { $sum: \"$sal\" } } }])",
    };
});

run("mql", "aggregate lookup dept", () => {
    const limit = Math.min(100, rows - 1);
    const found = database.orders.aggregate([
        { $match: { sku: { $gt: 0, $lt: asInt(limit + 1) } } },
        { $lookup: { from: "dept", localField: "dept", foreignField: "dept", as: "department" } },
    ]).toArray();
    return {
        ok: found.length === limit,
        rows: found.length,
        op: "db.orders.aggregate([{ $match: { sku: { $gt: 0, $lt: " + (limit + 1) + " } } }, { $lookup: { from: \"dept\", localField: \"dept\", foreignField: \"dept\", as: \"department\" } }])",
    };
});

run("mql", "aggregate out high_pay", () => {
    database.orders.aggregate([
        { $match: { sal: { $gt: asInt(2000) } } },
        { $project: { sku: 1, sal: 1, job: 1 } },
        { $out: "high_pay" },
    ]).toArray();
    const counted = command({ count: "high_pay" });
    if (!counted.ok) {
        return fromCommand(counted, 0);
    }
    return {
        ok: true,
        rows: Number(counted.n || 0),
        op: "db.orders.aggregate([{ $match: { sal: { $gt: 2000 } } }, { $project: { sku: 1, sal: 1, job: 1 } }, { $out: \"high_pay\" }])",
    };
});

run("mql", "text search clerk", () => {
    const found = database.orders_search.find({ $text: { $search: "clerk" } }, { sku: 1, job: 1 }).toArray();
    return {
        ok: found.length > 0,
        rows: found.length,
        op: "db.orders_search.find({ $text: { $search: \"clerk\" } }, { sku: 1, job: 1 })",
    };
});

run("mql", "text search clrek fuzzy 0.3", () => {
    const found = database.orders_search.find(
        { $text: { $search: "clrek", $fuzzy: 0.3 } },
        { sku: 1, job: 1 }
    ).toArray();
    return {
        ok: found.length > 0,
        rows: found.length,
        op: "db.orders_search.find({ $text: { $search: \"clrek\", $fuzzy: 0.3 } }, { sku: 1, job: 1 })",
    };
});

run("sql", "SELECT 1", () => {
    const reply = sql("SELECT 1 AS n");
    reply.op = "SELECT 1 AS n";
    return reply;
});

function sqlPoint(name) {
    run("sql", name, () => {
        let found = 0;
        let error = "";
        for (let i = 1; i <= lookups; i++) {
            const reply = command({ sql: "SELECT sku, job, sal FROM orders WHERE sku = " + i });
            if (!reply.ok) {
                error = reply.errmsg || "no reply";
                break;
            }
            const batch = (reply.cursor && reply.cursor.firstBatch) || [];
            if (batch.length === 1) {
                found += 1;
            }
        }
        return {
            ok: found === lookups,
            rows: found,
            error: error || (found === lookups ? "" : "matched " + found + " of " + lookups),
            note: lookups + " statements, WHERE sku = n",
            op: "SELECT sku, job, sal FROM orders WHERE sku = n\nn = 1.." + lookups,
        };
    });
}

sqlPoint("SELECT sku, first pass");
sqlPoint("SELECT sku, second pass");

function sqlStep(name, statement) {
    run("sql", name, () => {
        const reply = sql(statement);
        reply.op = statement;
        return reply;
    });
}

sqlStep("SELECT sal > 2000", "SELECT sku, job, sal FROM orders WHERE sal > 2000");
sqlStep("SELECT COUNT and GROUP BY job", "SELECT job, COUNT(*) AS n, SUM(sal) AS payroll FROM orders GROUP BY job");
sqlStep("SELECT COUNT(*)", "SELECT COUNT(*) AS n FROM orders");
sqlStep("INNER JOIN dept", "SELECT a.sku, b.name FROM orders a INNER JOIN dept b ON a.dept = b.dept");
sqlStep("WHERE TEXT clerk", "SELECT sku, job, sal FROM orders_search WHERE TEXT 'clerk'");
sqlStep("WHERE TEXT clrek FUZZY 0.3", "SELECT sku, job, sal FROM orders_search WHERE TEXT 'clrek' FUZZY 0.3");
sqlStep("FACETS job and sal", "SELECT facet, bucket, doc_count FROM orders_search WHERE TEXT 'clerk' FACETS job TERMS, sal RANGES (1500), (1500, 3000), (3000)");
sqlStep("SHOW INDEXES FROM orders", "SHOW INDEXES FROM orders");
sqlStep("SHOW TABLES", "SHOW TABLES");
sqlStep("SELECT from well_paid", "SELECT sku, sal FROM well_paid");
sqlStep("SELECT from high_pay", "SELECT sku, sal FROM high_pay");

function sqlWrite(label, statement) {
    run("sql", label, () => {
        const reply = command({ sql: statement });
        const info = fromCommand(reply, reply && reply.n != null ? Number(reply.n) : undefined);
        info.op = statement;
        return info;
    });
}

sqlWrite("Create a table with SQL", "CREATE TABLE extra (sku INT)");
sqlWrite("Insert a row with SQL", "INSERT INTO extra (sku) VALUES (1)");
sqlWrite("Update one row with SQL", "UPDATE orders SET qty = 1 WHERE sku = 1");
sqlWrite("Update many rows with SQL", "UPDATE orders SET bonus = 7 WHERE qty = 7");
sqlWrite("Delete one row with SQL", "DELETE FROM extra WHERE sku = 1");
sqlWrite("Delete many rows with SQL", "DELETE FROM orders WHERE qty = 7");

run("mql", "listCollections", () => {
    const reply = fromCommand(command({ listCollections: 1 }));
    reply.op = "{ listCollections: 1 }";
    return reply;
});

const dbStats = command({ dbStats: 1 });
const collStats = command({ collStats: "orders" });
const status = command({ serverStatus: 1 });

function numbers(reply) {
    const out = {};
    if (!reply) {
        return out;
    }
    for (const key of Object.keys(reply)) {
        if (key === "ok" || key === "$db") {
            continue;
        }
        const value = reply[key];
        if (typeof value === "number") {
            out[key] = value;
        }
    }
    return out;
}

let engine = "";
try {
    engine = db.version();
} catch (err) {
    engine = "";
}

const report = {
    ok: steps.every((step) => step.ok),
    engine: engine,
    database: "benchmark",
    rows: rows,
    lookups: lookups,
    jobs: jobs,
    departments: departments,
    orders: orders,
    steps: steps,
    dbStats: numbers(dbStats),
    collStats: numbers(collStats),
    serverStatus: numbers(status),
};

print("BENCHMARK_JSON " + JSON.stringify(report));
if (!report.ok) {
    quit(1);
}
