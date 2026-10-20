// Checks the two views `make seed` leaves on the Scott sample in `db`.
//
//   well_paid    emp rows with sal > 2000. A pipeline, not a copy.
//   emp_search   every emp row, with a Tantivy index on disk.
//
// The script does not drop those views. It inserts TEMP, checks that
// both views see the row, then deletes TEMP. A `$group` inverted view
// and a project that hides `_id` are rejected and are not kept.
// Dropping emp fails while a view still reads it.
//
//   make seed
//   make views

const database = db.getSiblingDB("db");
const n = (value) => NumberInt(value);

const wellPaid = ["BLAKE", "CLARK", "FORD", "JONES", "KING", "SCOTT"];
const clerks = ["ADAMS", "JAMES", "MILLER", "SMITH"];

function fail(label, detail) {
    print("FAIL " + label + (detail ? ": " + detail : ""));
    quit(1);
}

function ok(label, detail) {
    print("ok   " + label + (detail ? ": " + detail : ""));
}

function command(spec) {
    try {
        const reply = database.runCommand(spec);
        return reply || { ok: 0, errmsg: "no reply" };
    } catch (err) {
        return { ok: 0, errmsg: String(err.message || err) };
    }
}

function expectError(label, reply, needle) {
    const message = (reply && reply.errmsg) || "";
    if (reply && reply.ok) {
        fail(label, "the command was accepted");
    }
    if (!message.includes(needle)) {
        fail(label, message || "no error");
    }
    ok(label, message);
}

function names(collection, filter) {
    return collection
        .find(filter || {}, { _id: 0, ename: 1 })
        .toArray()
        .map((row) => row.ename)
        .sort();
}

function same(label, actual, expected) {
    const matched =
        actual.length === expected.length && actual.every((name, index) => name === expected[index]);
    if (!matched) {
        fail(label, "got [" + actual.join(", ") + "], want [" + expected.join(", ") + "]");
    }
    ok(label, actual.join(", ") || "(empty)");
}

if (database.emp.countDocuments({}) !== 14) {
    fail("emp sample", "run make seed first");
}

const listed = command({ listCollections: 1 });
if (!listed.ok) {
    fail("listCollections", listed.errmsg || "no reply");
}
const infos = (listed.cursor && listed.cursor.firstBatch) || [];
function info(name) {
    return infos.find((entry) => entry.name === name);
}

const paid = info("well_paid");
if (!paid || paid.type !== "view" || !paid.options || paid.options.viewOn !== "emp") {
    fail("well_paid", "missing; run make seed");
}
ok("well_paid", "view on emp");
same("sal above 2000", names(database.well_paid), wellPaid);

const search = info("emp_search");
if (!search || search.type !== "view" || !search.options || search.options.viewOn !== "emp") {
    fail("emp_search", "missing; run make seed");
}
const kova = search.options.kova || {};
if (kova.inverted !== true) {
    fail("emp_search inverted", JSON.stringify(kova));
}
const fields = kova.fields || {};
if (fields.ename !== "text" || fields.job !== "text" || fields.sal !== "i64" || fields.hiredate !== "date") {
    fail("discovered fields", JSON.stringify(fields));
}
ok("discovered fields", "ename=text job=text sal=i64 hiredate=date");
same("clerks", names(database.emp_search, { $text: { $search: "clerk" } }), clerks);
same("president", names(database.emp_search, { $text: { $search: "president" } }), ["KING"]);

const faceted = command({
    find: "emp_search",
    filter: {
        $text: { $search: "clerk" },
        facets: {
            job: { terms: { field: "job" } },
            sal: {
                range: {
                    field: "sal",
                    ranges: [{ to: n(1500) }, { from: n(1500), to: n(3000) }, { from: n(3000) }],
                },
            },
        },
    },
});
if (!faceted.ok) {
    fail("facets", faceted.errmsg || "no reply");
}
const facetBatch = (faceted.cursor && faceted.cursor.firstBatch) || [];
same(
    "facet documents",
    facetBatch.map((row) => row.ename).sort(),
    clerks
);
const jobBuckets = faceted.facets && faceted.facets.job && faceted.facets.job.buckets;
const clerkBucket = (jobBuckets || []).find((bucket) => bucket.key === "CLERK");
if (!clerkBucket || Number(clerkBucket.doc_count) !== 4) {
    fail("job facet", JSON.stringify(faceted.facets));
}
const salBuckets = faceted.facets && faceted.facets.sal && faceted.facets.sal.buckets;
const under1500 = (salBuckets || []).find((bucket) => bucket.to === 1500 || Number(bucket.to) === 1500);
if (!under1500 || Number(under1500.doc_count) !== 4) {
    fail("sal facet", JSON.stringify(salBuckets));
}
ok("facets", "job CLERK=4, sal below 1500=4");

const fuzzy = command({
    find: "emp_search",
    filter: {
        $text: { $search: "clrek", $fuzzy: 0.3 },
        facets: { job: { terms: { field: "job" } } },
    },
});
if (!fuzzy.ok) {
    fail("fuzzy", fuzzy.errmsg || "no reply");
}
const fuzzyJobs = fuzzy.facets && fuzzy.facets.job && fuzzy.facets.job.buckets;
const fuzzyClerk = (fuzzyJobs || []).find((bucket) => bucket.key === "CLERK");
if (!fuzzyClerk || Number(fuzzyClerk.doc_count) !== 4) {
    fail("fuzzy clerk", JSON.stringify(fuzzy.facets));
}
ok("fuzzy", "clrek at 0.3 finds CLERK");

database.emp.insertOne({
    empno: n(1),
    ename: "TEMP",
    job: "ANALYST",
    sal: n(9000),
    deptno: n(20),
    hiredate: ISODate("1990-01-01"),
});
try {
    same("well_paid sees TEMP", names(database.well_paid), wellPaid.concat(["TEMP"]).sort());
    same("search sees TEMP", names(database.emp_search, { $text: { $search: "temp" } }), ["TEMP"]);
} finally {
    database.emp.deleteOne({ ename: "TEMP" });
}
same("well_paid restored", names(database.well_paid), wellPaid);
same("TEMP left the index", names(database.emp_search, { $text: { $search: "temp" } }), []);
if (database.emp.countDocuments({}) !== 14) {
    fail("emp sample", "TEMP was not removed");
}

expectError(
    "inverted $group rejected",
    command({
        create: "grouped",
        viewOn: "emp",
        pipeline: [{ $group: { _id: "$job", n: { $sum: 1 } } }],
        kova: { inverted: true },
    }),
    "$group"
);
expectError(
    "inverted project must keep _id",
    command({
        create: "no_id",
        viewOn: "emp",
        pipeline: [{ $project: { _id: 0, ename: 1 } }],
        kova: { inverted: true },
    }),
    "_id"
);
expectError("emp is still read", command({ drop: "emp" }), "reads it");
ok("done", "well_paid and emp_search still on db.emp");
