// Department pay over the Scott sample in database `db`.
//
// Requires `make seed`. Reads emp, dept, bonus, and salgrade. Writes
// the department rows to `deptPay` so a second aggregate can read them.
//
// The first $group keeps each department's average salary next to the
// payroll sum and the headcount. The second $group averages those
// averages. That number is not the average of the 14 salaries: a
// department of three people weighs the same as a department of six.
// The script divides the summed payroll by the summed headcount and
// prints both figures.

const database = db.getSiblingDB("db");

if (database.emp.countDocuments({}) !== 14) {
    print("FAIL emp sample: run make seed first");
    quit(1);
}

const byDepartment = [
    {
        $lookup: {
            from: "dept",
            localField: "deptno",
            foreignField: "deptno",
            as: "dept",
        },
    },
    { $unwind: "$dept" },
    {
        $lookup: {
            from: "bonus",
            localField: "empno",
            foreignField: "empno",
            as: "awards",
        },
    },
    { $unwind: { path: "$awards", preserveNullAndEmptyArrays: true } },
    {
        $lookup: {
            from: "salgrade",
            let: { salary: "$sal" },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                { $gte: ["$$salary", "$losal"] },
                                { $lte: ["$$salary", "$hisal"] },
                            ],
                        },
                    },
                },
            ],
            as: "grade",
        },
    },
    { $unwind: "$grade" },
    {
        $addFields: {
            department: "$dept.dname",
            location: "$dept.loc",
            award: "$awards.comm",
            grade: "$grade.grade",
        },
    },
    { $sort: { sal: -1, ename: 1 } },
    {
        $group: {
            _id: "$department",
            location: { $first: "$location" },
            headcount: { $count: {} },
            payroll: { $sum: "$sal" },
            avgSal: { $avg: "$sal" },
            awardTotal: { $sum: "$award" },
            grades: { $addToSet: "$grade" },
            highestPaid: { $first: "$ename" },
            lowestPaid: { $last: "$ename" },
        },
    },
    { $sort: { _id: 1 } },
    {
        $project: {
            _id: 0,
            department: "$_id",
            location: 1,
            headcount: 1,
            payroll: 1,
            avgSal: 1,
            awardTotal: 1,
            grades: 1,
            highestPaid: 1,
            lowestPaid: 1,
        },
    },
];

print("by department");
printjson(database.emp.aggregate(byDepartment).toArray());

database.emp.aggregate(byDepartment.concat([{ $out: "deptPay" }]));

const company = database.deptPay
    .aggregate([
        {
            $group: {
                _id: null,
                departments: { $sum: 1 },
                headcount: { $sum: "$headcount" },
                payroll: { $sum: "$payroll" },
                averageOfAverages: { $avg: "$avgSal" },
            },
        },
    ])
    .toArray()[0];

const weighted = company.payroll / company.headcount;
print("company");
print("  departments            " + company.departments);
print("  headcount              " + company.headcount);
print("  payroll                " + company.payroll);
print("  average of averages    " + company.averageOfAverages);
print("  average of salaries    " + weighted);
print("saved to deptPay");
