// Checks the seventeen serverStatus counters against a running Kova.
//
// Inserts two documents, builds an index, runs an indexed find and a
// collection scan, then reads serverStatus again. Counters are for the
// whole process, so the script asserts a minimum increase.
// Security stays off for this script (the default in kova.toml).
//
//   make server-status

const database = db.getSiblingDB("statusdemo");
const n = (value) => NumberInt(value);

const COUNTERS = [
    "queries",
    "inserts",
    "updates",
    "deletes",
    "connectionsActive",
    "connectionsTotal",
    "bytesIn",
    "bytesOut",
    "walWrites",
    "bufferPoolHits",
    "bufferPoolMisses",
    "bufferPoolEvictions",
    "indexLookups",
    "collectionScans",
    "transactionsStarted",
    "transactionsCommitted",
    "transactionsAborted",
];

function fail(label, detail) {
    print("FAIL " + label + (detail ? ": " + detail : ""));
    quit(1);
}

function ok(label, detail) {
    print("ok   " + label + (detail ? ": " + detail : ""));
}

function command(spec) {
    let reply;
    try {
        reply = database.runCommand(spec);
    } catch (err) {
        reply = { ok: 0, errmsg: String(err.message || err) };
    }
    if (reply && reply.ok) {
        return reply;
    }
    const message = (reply && reply.errmsg) || "no reply";
    if (String(message).includes("authentication required")) {
        fail(spec[Object.keys(spec)[0]] || "command", "security is on; this script expects it off");
    }
    fail(spec[Object.keys(spec)[0]] || "command", message);
}

function counter(status, key) {
    if (status[key] === undefined || status[key] === null) {
        fail("serverStatus", key + " missing");
    }
    const value = Number(status[key]);
    if (Number.isNaN(value)) {
        fail("serverStatus", key + " is not a number: " + status[key]);
    }
    return value;
}

function grew(before, after, key, least) {
    const delta = counter(after, key) - counter(before, key);
    if (delta < least) {
        fail(key, "grew by " + delta + ", want at least " + least);
    }
    ok(key, "+" + delta);
}

database.samples.drop();

const before = command({ serverStatus: 1 });
if (Number(before.uptime) < 0 || Number.isNaN(Number(before.uptime))) {
    fail("uptime", before.uptime);
}
ok("uptime", before.uptime + "s");
COUNTERS.forEach((key) => counter(before, key));

command({
    insert: "samples",
    documents: [
        { _id: "1", name: "SMITH", sal: n(800) },
        { _id: "2", name: "KING", sal: n(5000) },
    ],
});
command({
    createIndexes: "samples",
    indexes: [{ name: "name_1", key: { name: 1 } }],
});
command({ update: "samples", updates: [{ q: { _id: "2" }, u: { $set: { sal: n(5100) } } }] });
command({ delete: "samples", deletes: [{ q: { _id: "2" }, limit: 1 }] });

const byName = database.samples.find({ name: "SMITH" }).toArray();
if (byName.length !== 1) {
    fail("indexed find", "got " + byName.length);
}
ok("indexed find", "SMITH");

const scanned = database.samples.find({ sal: { $gt: n(0) } }).toArray();
if (scanned.length !== 1) {
    fail("collection scan", "got " + scanned.length);
}
ok("collection scan", String(scanned.length));

const after = command({ serverStatus: 1 });
COUNTERS.forEach((key) => counter(after, key));
grew(before, after, "inserts", 1);
grew(before, after, "updates", 1);
grew(before, after, "deletes", 1);
grew(before, after, "queries", 2);
grew(before, after, "indexLookups", 1);
grew(before, after, "collectionScans", 1);
grew(before, after, "walWrites", 1);

database.samples.drop();
ok("done", "statusdemo.samples dropped");
