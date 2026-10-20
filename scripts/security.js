// Users, custom roles, and authenticate against a running Kova.
//
// The catalog starts empty. The first user has to be root. A wrong
// password and an unknown user return the same error. Built-in roles
// cannot be dropped, and the last root cannot be disabled.
//
// With security off (the default) every command is still accepted.
// With security on, the script logs in as root and checks that `ada`
// cannot insert.
//
// There is no dropUser. A second run reuses root, ada, and analyst.
// Passwords: root is "secret", ada is "ada-secret".
//
//   make security

const database = db.getSiblingDB("admin");

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

function message(reply) {
    return String((reply && (reply.errmsg || reply.message)) || "");
}

function failed(reply) {
    return !reply || !reply.ok;
}

function expectError(label, reply, needle) {
    const text = message(reply);
    if (!failed(reply)) {
        fail(label, "the command was accepted");
    }
    if (!text.includes(needle)) {
        fail(label, text || "no error");
    }
    ok(label, text);
}

function ensure(label, spec, already) {
    const reply = command(spec);
    if (!failed(reply)) {
        ok(label, "created");
        return;
    }
    if (message(reply).includes(already)) {
        ok(label, "already exists");
        return;
    }
    fail(label, message(reply) || "no reply");
}

function login(user, pwd) {
    const reply = command({ authenticate: 1, user: user, pwd: pwd });
    if (failed(reply)) {
        fail("authenticate " + user, message(reply) || "no reply");
    }
    return reply;
}

function hasAction(reply, action) {
    const actions = reply.actions || [];
    return actions.some((name) => name === action);
}

const probe = command({ count: "probe" });
const securityOn = failed(probe) && message(probe).includes("authentication required");
ok("security", securityOn ? "on" : "off");

const rootLogin = command({ authenticate: 1, user: "root", pwd: "secret" });
if (!failed(rootLogin)) {
    ok("root login", (rootLogin.actions || []).length + " actions");
} else {
    const guest = command({
        createUser: "guest",
        pwd: "nope",
        roles: ["read"],
    });
    if (!failed(guest)) {
        fail("first user", "guest was created; root exists with a different password");
    }
    if (message(guest).includes("root")) {
        ok("first user must be root", message(guest));
    } else if (!message(guest).includes("authentication required")) {
        fail("first user", message(guest) || "no reply");
    }
    const created = command({
        createUser: "root",
        pwd: "secret",
        roles: ["root"],
    });
    if (failed(created)) {
        fail("create root", message(created) || "no reply");
    }
    const session = login("root", "secret");
    if ((session.actions || []).length !== 11) {
        fail("root actions", JSON.stringify(session.actions));
    }
    ok("root created", "11 actions");
}

expectError(
    "unknown action",
    command({
        createRole: "analyst_bad",
        actions: ["dropDatabase"],
    }),
    "dropDatabase"
);

expectError(
    "built-in role",
    command({ dropRole: "read" }),
    "built-in"
);

expectError(
    "last root stays enabled",
    command({ updateUser: "root", enabled: false }),
    "last root"
);

ensure("analyst role", { createRole: "analyst", actions: ["find", "viewStatus"] }, "already exists");
command({ updateUser: "ada", enabled: true });
ensure("ada", { createUser: "ada", pwd: "ada-secret", roles: ["read"] }, "already exists");
const granted = command({
    grantRolesToUser: "ada",
    roles: ["analyst"],
});
if (failed(granted)) {
    fail("grant analyst", message(granted));
}
ok("grant analyst");

const bad = command({ authenticate: 1, user: "ada", pwd: "nope" });
const unknown = command({ authenticate: 1, user: "nobody", pwd: "nope" });
expectError("wrong password", bad, "authentication failed");
expectError("unknown user", unknown, "authentication failed");
if (message(bad) !== message(unknown)) {
    fail("same failure", message(bad) + " vs " + message(unknown));
}
ok("failures match");

const ada = login("ada", "ada-secret");
if (!hasAction(ada, "find") || !hasAction(ada, "viewStatus") || hasAction(ada, "insert")) {
    fail("ada actions", JSON.stringify(ada.actions));
}
ok("ada actions", (ada.actions || []).join(", "));

if (securityOn) {
    expectError(
        "ada cannot insert",
        command({
            insert: "notes",
            documents: [{ _id: "1", text: "nope" }],
        }),
        "unauthorized: insert"
    );
}

login("root", "secret");
const disabled = command({ updateUser: "ada", enabled: false });
if (failed(disabled)) {
    fail("disable ada", message(disabled));
}
expectError(
    "disabled ada",
    command({ authenticate: 1, user: "ada", pwd: "ada-secret" }),
    "authentication failed"
);
const enabled = command({ updateUser: "ada", enabled: true });
if (failed(enabled)) {
    fail("enable ada", message(enabled));
}
login("ada", "ada-secret");
ok("ada enabled again");
ok("done", "root and ada remain in the catalog");
