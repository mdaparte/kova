// Discretionary-award report over the Scott sample in database `db`.
//
// Page 1 is the two departments that paid the most in awards.
// Change `skip` to 2 for page 2 (RESEARCH). `$count` and `$out` are last
// because each one replaces the rows the stages above produced:
// `$count` collapses them to a single number, and `$out` writes them.

const database = db.getSiblingDB("db");
const page = 1;
const pageSize = 2;
const skip = (page - 1) * pageSize;

const pipeline = [
    { $match: { deptno: { $in: [10, 20, 30] } } },
    { $lookup: { from: "bonus", localField: "empno", foreignField: "empno", as: "awards" } },
    { $unwind: "$awards" },
    { $lookup: { from: "dept", localField: "deptno", foreignField: "deptno", as: "dept" } },
    { $unwind: "$dept" },
    {
        $addFields: {
            award: "$awards.comm",
            department: "$dept.dname",
            location: "$dept.loc",
        },
    },
    {
        $project: {
            _id: 0,
            ename: 1,
            job: 1,
            sal: 1,
            award: 1,
            department: 1,
            location: 1,
        },
    },
    { $sort: { sal: -1 } },
    {
        $group: {
            _id: "$department",
            location: { $first: "$location" },
            headcount: { $count: {} },
            payroll: { $sum: "$sal" },
            awardTotal: { $sum: "$award" },
            minSal: { $min: "$sal" },
            maxSal: { $max: "$sal" },
            avgSal: { $avg: "$sal" },
            names: { $push: "$ename" },
            jobs: { $addToSet: "$job" },
            highestPaid: { $first: "$ename" },
            lowestPaid: { $last: "$ename" },
        },
    },
    { $sort: { awardTotal: -1, _id: 1 } },
    { $skip: skip },
    { $limit: pageSize },
    {
        $replaceRoot: {
            newRoot: {
                department: "$_id",
                location: "$location",
                headcount: "$headcount",
                payroll: "$payroll",
                awards: "$awardTotal",
                averageSalary: "$avgSal",
                salaryBand: { low: "$minSal", high: "$maxSal" },
                recipients: "$names",
                roles: "$jobs",
                highestPaid: "$highestPaid",
                lowestPaid: "$lowestPaid",
            },
        },
    },
];

print("page " + page);
printjson(database.emp.aggregate(pipeline).toArray());

database.emp.aggregate(pipeline.concat([{ $out: "bonusReport" }]));
print("saved to bonusReport");
printjson(database.bonusReport.find().toArray());
print("departments on this page:");
printjson(database.bonusReport.aggregate([{ $count: "departmentsOnPage" }]).toArray());
