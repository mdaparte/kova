// MongoDB-style views, plus a Tantivy index on disk when kova.inverted is true.
//
// Uses database `searchdemo`, so the Scott sample in `db` stays put.
// Drops its own view and collection at the start and at the end.
// Security stays off for this script (the default in kova.toml).
// A restart check lives in cargo test: the index is on disk, and this
// script talks to the process that is already running.
//
//   make views

const database = db.getSiblingDB("searchdemo");
const n = (value) => NumberInt(value);

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

function expectOk(label, reply) {
    if (reply && reply.ok) {
        ok(label);
        return reply;
    }
    const message = (reply && reply.errmsg) || "no reply";
    if (String(message).includes("authentication required")) {
        fail(label, "security is on; this script expects it off");
    }
    fail(label, message);
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

function titles(collection, filter) {
    return collection
        .find(filter || {})
        .toArray()
        .map((row) => row.title)
        .sort();
}

function same(label, actual, expected) {
    const matched =
        actual.length === expected.length && actual.every((title, index) => title === expected[index]);
    if (!matched) {
        fail(label, "got [" + actual.join(", ") + "], want [" + expected.join(", ") + "]");
    }
    ok(label, actual.join(", ") || "(empty)");
}

database.published_search.drop();
database.published_only.drop();
database.articles.drop();

expectOk(
    "insert articles",
    command({
        insert: "articles",
        documents: [
            { _id: "1", title: "rust engine", sal: n(10), published: true },
            { _id: "2", title: "other", sal: n(3), published: false },
        ],
    })
);

expectOk(
    "plain view",
    command({
        create: "published_only",
        viewOn: "articles",
        pipeline: [{ $match: { published: true } }],
    })
);
same("plain view find", titles(database.published_only), ["rust engine"]);

expectError(
    "inverted $group rejected",
    command({
        create: "grouped",
        viewOn: "articles",
        pipeline: [{ $group: { _id: "$published", n: { $sum: 1 } } }],
        kova: { inverted: true },
    }),
    "$group"
);

expectError(
    "inverted project must keep _id",
    command({
        create: "no_id",
        viewOn: "articles",
        pipeline: [{ $project: { _id: 0, title: 1 } }],
        kova: { inverted: true },
    }),
    "_id"
);

expectOk(
    "inverted view",
    command({
        create: "published_search",
        viewOn: "articles",
        pipeline: [{ $match: { published: true } }],
        kova: { inverted: true, later: "kept" },
    })
);

same("inverted find", titles(database.published_search), ["rust engine"]);
same("text search rust", titles(database.published_search, { $text: { $search: "rust" } }), ["rust engine"]);
same("text search misses a draft", titles(database.published_search, { $text: { $search: "other" } }), []);

const listed = command({ listCollections: 1, filter: { name: "published_search" } });
expectOk("listCollections", listed);
const info = listed.cursor && listed.cursor.firstBatch && listed.cursor.firstBatch[0];
if (!info || info.type !== "view") {
    fail("view type", JSON.stringify(info));
}
const options = info.options || {};
if (options.viewOn !== "articles") {
    fail("viewOn", JSON.stringify(options));
}
const kova = options.kova || {};
if (kova.later !== "kept" || kova.inverted !== true) {
    fail("kova", JSON.stringify(kova));
}
const fields = kova.fields || {};
if (fields.title !== "text" || fields.sal !== "i64" || fields.published !== "bool") {
    fail("discovered fields", JSON.stringify(fields));
}
ok("discovered fields", "title=text sal=i64 published=bool later=kept");

expectOk(
    "insert a published rust book",
    command({
        insert: "articles",
        documents: [{ _id: "3", title: "rust book", sal: n(4), published: true }],
    })
);
same("index follows the insert", titles(database.published_search, { $text: { $search: "book" } }), ["rust book"]);

expectOk(
    "insert an unpublished rust draft",
    command({
        insert: "articles",
        documents: [{ _id: "4", title: "rust draft", sal: n(1), published: false }],
    })
);
same("pipeline hides the draft", titles(database.published_search, { $text: { $search: "draft" } }), []);

expectOk("delete the book", command({ delete: "articles", deletes: [{ q: { _id: "3" }, limit: 1 }] }));
same("index drops the book", titles(database.published_search, { $text: { $search: "book" } }), []);
same("rust engine remains", titles(database.published_search, { $text: { $search: "rust" } }), ["rust engine"]);

expectError("source is still read", command({ drop: "articles" }), "reads it");

expectOk("drop plain view", command({ drop: "published_only" }));
expectOk("drop inverted view", command({ drop: "published_search" }));
expectOk("drop source", command({ drop: "articles" }));
ok("done", "searchdemo left empty");
