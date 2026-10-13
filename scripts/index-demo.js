// Index tour over the Scott sample in database `db`.
//
// Assumes `make seed` already loaded emp. Creates five indexes, checks
// the people each one returns, rejects a second SMITH, moves SMITH's
// salary through a range and puts it back, then drops and recreates the
// salary index. The sample is unchanged when the script finishes.
//
//   make indexes

const database = db.getSiblingDB("db");
const n = (value) => NumberInt(value);

const above2000 = ["BLAKE", "CLARK", "FORD", "JONES", "KING", "SCOTT"];
const clerks = ["ADAMS", "JAMES", "MILLER", "SMITH"];
const researchAbove2000 = ["FORD", "JONES", "SCOTT"];

function fail(label, detail) {
    print("FAIL " + label + (detail ? ": " + detail : ""));
    quit(1);
}

function ok(label, detail) {
    print("ok   " + label + (detail ? ": " + detail : ""));
}

function same(label, actual, expected) {
    const matched =
        actual.length === expected.length && actual.every((name, index) => name === expected[index]);
    if (!matched) {
        fail(label, "got [" + actual.join(", ") + "], want [" + expected.join(", ") + "]");
    }
    ok(label, actual.join(", "));
}

function roster(filter) {
    return database.emp
        .find(filter, { _id: 0, ename: 1 })
        .toArray()
        .map((row) => row.ename)
        .sort();
}

function plan(filter) {
    const reply = database.runCommand({ explain: { find: "emp", filter: filter } });
    if (!reply.ok) {
        fail("explain", reply.errmsg || "no reply");
    }
    const winning = reply.queryPlanner && reply.queryPlanner.winningPlan;
    return {
        stage: winning && winning.stage,
        index: winning && winning.indexName,
    };
}

function expectPlan(label, filter, stage, index) {
    const chosen = plan(filter);
    if (chosen.stage !== stage || (index && chosen.index !== index)) {
        fail(label, chosen.stage + " " + (chosen.index || "") + ", want " + stage + " " + (index || ""));
    }
    ok(label, chosen.stage + (chosen.index ? " " + chosen.index : ""));
}

const headcount = database.emp.countDocuments({});
if (headcount !== 14) {
    fail("emp sample", headcount + " documents; run make seed first");
}

database.emp.createIndex({ sal: 1 }, { name: "sal_1" });
database.emp.createIndex({ job: "hashed" }, { name: "job_hashed" });
database.emp.createIndex({ ename: 1 }, { name: "ename_1", unique: true });
database.emp.createIndex({ deptno: 1, sal: 1 }, { name: "deptno_1_sal_1" });
database.emp.createIndex({ comm: 1 }, { name: "comm_1", sparse: true });
ok("indexes", database.emp.getIndexes().map((spec) => spec.name).sort().join(", "));

const salaryFilter = { sal: { $gt: n(2000) } };
expectPlan("salary range", salaryFilter, "IXSCAN", "sal_1");
same("earns more than 2000", roster(salaryFilter), above2000);

const clerkFilter = { job: "CLERK" };
expectPlan("job hash", clerkFilter, "IXSCAN", "job_hashed");
same("clerks", roster(clerkFilter), clerks);

const researchFilter = { deptno: n(20), sal: { $gt: n(2000) } };
expectPlan("department and salary", researchFilter, "IXSCAN", "deptno_1_sal_1");
same("research above 2000", roster(researchFilter), researchAbove2000);

const awardFilter = { comm: { $gt: n(1000) } };
expectPlan("sparse commission", awardFilter, "IXSCAN", "comm_1");
same("commission above 1000", roster(awardFilter), ["MARTIN"]);

const smith = database.emp.findOne({ ename: "SMITH" });
expectPlan("id lookup", { _id: smith._id }, "IDHACK", "_id_");

const hiredAfter = { hiredate: { $gt: ISODate("1985-01-01") } };
expectPlan("hire date has no index", hiredAfter, "COLLSCAN");
same("hired after 1985", roster(hiredAfter), ["ADAMS", "SCOTT"]);

// mongosh throws on a write error, so the rejection arrives as an exception.
let duplicateMessage = "";
try {
    database.emp.insertOne({ ename: "SMITH", job: "CLERK", sal: n(1), deptno: n(20) });
} catch (err) {
    duplicateMessage = String(err.message || err);
}
if (!duplicateMessage.includes("duplicate")) {
    fail("second SMITH", duplicateMessage || "the insert was accepted");
}
ok("second SMITH rejected", duplicateMessage);
same("SMITH still unique", roster({ ename: "SMITH" }), ["SMITH"]);

database.emp.updateOne({ ename: "SMITH" }, { $set: { sal: n(2500) } });
try {
    same("SMITH joins the range", roster(salaryFilter), ["BLAKE", "CLARK", "FORD", "JONES", "KING", "SCOTT", "SMITH"]);
} finally {
    database.emp.updateOne({ ename: "SMITH" }, { $set: { sal: n(800) } });
}
same("SMITH salary restored", roster(salaryFilter), above2000);

database.emp.dropIndex("sal_1");
expectPlan("salary index dropped", salaryFilter, "COLLSCAN");
same("range still answers", roster(salaryFilter), above2000);
database.emp.createIndex({ sal: 1 }, { name: "sal_1" });
expectPlan("salary index restored", salaryFilter, "IXSCAN", "sal_1");

const sql = database.runCommand({ sql: "select ename from emp where sal > 2000" });
if (!sql.ok) {
    fail("sql", sql.errmsg || "no reply");
}
const sqlPlan = sql.queryPlanner && sql.queryPlanner.winningPlan;
if (!sqlPlan || sqlPlan.stage !== "IXSCAN" || sqlPlan.indexName !== "sal_1") {
    fail("sql plan", JSON.stringify(sqlPlan));
}
const sqlNames = (sql.cursor.firstBatch || [])
    .map((row) => row.ename)
    .sort();
same("sql where sal > 2000", sqlNames, above2000);
ok("done", "emp left with the five indexes; salaries unchanged");
