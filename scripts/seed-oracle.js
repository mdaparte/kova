// Loads the Oracle Scott sample (emp, dept, salgrade, bonus) into database `db`.
//
// Names are lowercase on purpose: Kova collections are case-sensitive, and
// `select * from emp where sal > 2000` must hit this data.
// Integers are NumberInt so SQL returns 7369, not 7369.0 (plain JS numbers
// are doubles). hiredate is a BSON date. Null mgr/comm fields are omitted.
//
// How the four collections relate:
//   emp.deptno  = dept.deptno          (each employee belongs to one department)
//   emp.mgr     = emp.empno            (manager is another employee; KING has none)
//   salgrade    has no foreign key. An employee is in a grade when
//                 losal <= emp.sal <= hisal. It is a range, not an equality join.
//   bonus       ships empty in Oracle. The rows below are discretionary awards
//                 keyed by empno so a $lookup can attach them. `comm` here is
//                 the award amount, not the commission stored on emp.
//
// After the rows, the script leaves the five emp indexes that
// `scripts/index-demo.js` checks, plus two views:
//   well_paid   emp rows with sal > 2000
//   emp_search  every emp row, with a Tantivy index on disk (`kova.inverted`)

const database = db.getSiblingDB("db");
const n = (value) => NumberInt(value);
const day = (iso) => ISODate(iso);

function command(spec) {
    let reply;
    try {
        reply = database.runCommand(spec);
    } catch (err) {
        print("FAIL " + (err.message || err));
        quit(1);
    }
    if (!reply || !reply.ok) {
        print("FAIL " + ((reply && reply.errmsg) || "no reply"));
        quit(1);
    }
    return reply;
}

// Views read emp, so they have to go before the database drop. A missing
// name is not an error: the first seed has nothing to remove.
for (const name of ["well_paid", "emp_search"]) {
    try {
        database.runCommand({ drop: name });
    } catch (err) {
        const message = String(err.message || err);
        if (!message.includes("not found")) {
            print("FAIL drop " + name + ": " + message);
            quit(1);
        }
    }
}
database.dropDatabase();

const dept = database.dept.insertMany([
    { deptno: n(10), dname: "ACCOUNTING", loc: "NEW YORK" },
    { deptno: n(20), dname: "RESEARCH", loc: "DALLAS" },
    { deptno: n(30), dname: "SALES", loc: "CHICAGO" },
    { deptno: n(40), dname: "OPERATIONS", loc: "BOSTON" },
]);

const emp = database.emp.insertMany([
    { empno: n(7369), ename: "SMITH", job: "CLERK", mgr: n(7902), hiredate: day("1980-12-17"), sal: n(800), deptno: n(20) },
    { empno: n(7499), ename: "ALLEN", job: "SALESMAN", mgr: n(7698), hiredate: day("1981-02-20"), sal: n(1600), comm: n(300), deptno: n(30) },
    { empno: n(7521), ename: "WARD", job: "SALESMAN", mgr: n(7698), hiredate: day("1981-02-22"), sal: n(1250), comm: n(500), deptno: n(30) },
    { empno: n(7566), ename: "JONES", job: "MANAGER", mgr: n(7839), hiredate: day("1981-04-02"), sal: n(2975), deptno: n(20) },
    { empno: n(7654), ename: "MARTIN", job: "SALESMAN", mgr: n(7698), hiredate: day("1981-09-28"), sal: n(1250), comm: n(1400), deptno: n(30) },
    { empno: n(7698), ename: "BLAKE", job: "MANAGER", mgr: n(7839), hiredate: day("1981-05-01"), sal: n(2850), deptno: n(30) },
    { empno: n(7782), ename: "CLARK", job: "MANAGER", mgr: n(7839), hiredate: day("1981-06-09"), sal: n(2450), deptno: n(10) },
    { empno: n(7788), ename: "SCOTT", job: "ANALYST", mgr: n(7566), hiredate: day("1987-04-19"), sal: n(3000), deptno: n(20) },
    { empno: n(7839), ename: "KING", job: "PRESIDENT", hiredate: day("1981-11-17"), sal: n(5000), deptno: n(10) },
    { empno: n(7844), ename: "TURNER", job: "SALESMAN", mgr: n(7698), hiredate: day("1981-09-08"), sal: n(1500), comm: n(0), deptno: n(30) },
    { empno: n(7876), ename: "ADAMS", job: "CLERK", mgr: n(7788), hiredate: day("1987-05-23"), sal: n(1100), deptno: n(20) },
    { empno: n(7900), ename: "JAMES", job: "CLERK", mgr: n(7698), hiredate: day("1981-12-03"), sal: n(950), deptno: n(30) },
    { empno: n(7902), ename: "FORD", job: "ANALYST", mgr: n(7566), hiredate: day("1981-12-03"), sal: n(3000), deptno: n(20) },
    { empno: n(7934), ename: "MILLER", job: "CLERK", mgr: n(7782), hiredate: day("1982-01-23"), sal: n(1300), deptno: n(10) },
]);

const salgrade = database.salgrade.insertMany([
    { grade: n(1), losal: n(700), hisal: n(1200) },
    { grade: n(2), losal: n(1201), hisal: n(1400) },
    { grade: n(3), losal: n(1401), hisal: n(2000) },
    { grade: n(4), losal: n(2001), hisal: n(3000) },
    { grade: n(5), losal: n(3001), hisal: n(9999) },
]);

// Discretionary awards. empno joins back to emp; the other columns match
// Oracle's BONUS table (ename, job, sal, comm).
const bonus = database.bonus.insertMany([
    { empno: n(7499), ename: "ALLEN", job: "SALESMAN", sal: n(1600), comm: n(200) },
    { empno: n(7521), ename: "WARD", job: "SALESMAN", sal: n(1250), comm: n(150) },
    { empno: n(7654), ename: "MARTIN", job: "SALESMAN", sal: n(1250), comm: n(100) },
    { empno: n(7788), ename: "SCOTT", job: "ANALYST", sal: n(3000), comm: n(250) },
    { empno: n(7839), ename: "KING", job: "PRESIDENT", sal: n(5000), comm: n(500) },
    { empno: n(7934), ename: "MILLER", job: "CLERK", sal: n(1300), comm: n(80) },
]);

function inserted(result) {
    if (typeof result.insertedCount === "number") {
        return result.insertedCount;
    }
    return Object.keys(result.insertedIds).length;
}

database.emp.createIndex({ sal: 1 }, { name: "sal_1" });
database.emp.createIndex({ job: "hashed" }, { name: "job_hashed" });
database.emp.createIndex({ ename: 1 }, { name: "ename_1", unique: true });
database.emp.createIndex({ deptno: 1, sal: 1 }, { name: "deptno_1_sal_1" });
database.emp.createIndex({ comm: 1 }, { name: "comm_1", sparse: true });

command({
    create: "well_paid",
    viewOn: "emp",
    pipeline: [{ $match: { sal: { $gt: n(2000) } } }],
});
command({
    create: "emp_search",
    viewOn: "emp",
    pipeline: [],
    kova: { inverted: true, analyzer: "en_stem" }
});

print("seeded database db");
print("  dept     " + inserted(dept));
print("  emp      " + inserted(emp));
print("  salgrade " + inserted(salgrade));
print("  bonus    " + inserted(bonus));
print("  indexes  sal_1, job_hashed, ename_1, deptno_1_sal_1, comm_1");
print("  views    well_paid, emp_search");
