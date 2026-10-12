// Loads the Oracle Scott sample (emp, dept, salgrade, bonus) into database `db`.
//
// Names are lowercase on purpose: Kova collections are case-sensitive, and
// `select * from emp where sal > 2000` must hit this data.
// Integers are NumberInt so SQL returns 7369, not 7369.0 (plain JS numbers
// are doubles). hiredate is a BSON date. Null mgr/comm fields are omitted.

const database = db.getSiblingDB("db");
const n = (value) => NumberInt(value);
const day = (iso) => ISODate(iso);

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

// Oracle ships BONUS empty. Create it so the collection is there to fill in.
database.createCollection("bonus");

function inserted(result) {
    if (typeof result.insertedCount === "number") {
        return result.insertedCount;
    }
    return Object.keys(result.insertedIds).length;
}

print("seeded database db");
print("  dept     " + inserted(dept));
print("  emp      " + inserted(emp));
print("  salgrade " + inserted(salgrade));
print("  bonus    0");
