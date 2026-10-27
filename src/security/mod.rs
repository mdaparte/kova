//! Roles, passwords, and which command each action allows.
//!
//! Built-in roles are fixed. A custom role is a name plus a subset of
//! the eleven actions. Passwords are stored as Argon2id hashes.

use std::collections::BTreeSet;

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::{Algorithm, Argon2, Params, Version};
use bson::Document;

/// One privilege a role can grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Action {
    Find,
    Insert,
    Update,
    Remove,
    CreateCollection,
    DropCollection,
    CreateIndex,
    DropIndex,
    ViewStatus,
    CreateUser,
    GrantRole,
}

impl Action {
    pub fn name(self) -> &'static str {
        match self {
            Self::Find => "find",
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Remove => "remove",
            Self::CreateCollection => "createCollection",
            Self::DropCollection => "dropCollection",
            Self::CreateIndex => "createIndex",
            Self::DropIndex => "dropIndex",
            Self::ViewStatus => "viewStatus",
            Self::CreateUser => "createUser",
            Self::GrantRole => "grantRole",
        }
    }

    pub fn parse(name: &str) -> Result<Self, String> {
        Ok(match name {
            "find" => Self::Find,
            "insert" => Self::Insert,
            "update" => Self::Update,
            "remove" => Self::Remove,
            "createCollection" => Self::CreateCollection,
            "dropCollection" => Self::DropCollection,
            "createIndex" => Self::CreateIndex,
            "dropIndex" => Self::DropIndex,
            "viewStatus" => Self::ViewStatus,
            "createUser" => Self::CreateUser,
            "grantRole" => Self::GrantRole,
            other => return Err(format!("unknown action {other}")),
        })
    }
}

const BUILTIN: &[&str] = &["read", "readWrite", "dbAdmin", "userAdmin", "root"];

pub fn is_builtin(role: &str) -> bool {
    BUILTIN.contains(&role)
}

/// Actions a built-in role grants. Custom roles are resolved by the caller.
pub fn builtin_actions(role: &str) -> Option<BTreeSet<Action>> {
    let mut actions = BTreeSet::new();
    match role {
        "read" => {
            actions.insert(Action::Find);
        }
        "readWrite" => {
            actions.insert(Action::Find);
            actions.insert(Action::Insert);
            actions.insert(Action::Update);
            actions.insert(Action::Remove);
        }
        "dbAdmin" => {
            actions.extend(builtin_actions("readWrite").unwrap());
            actions.insert(Action::CreateCollection);
            actions.insert(Action::DropCollection);
            actions.insert(Action::CreateIndex);
            actions.insert(Action::DropIndex);
            actions.insert(Action::ViewStatus);
        }
        "userAdmin" => {
            actions.insert(Action::CreateUser);
            actions.insert(Action::GrantRole);
        }
        "root" => {
            actions.extend([
                Action::Find,
                Action::Insert,
                Action::Update,
                Action::Remove,
                Action::CreateCollection,
                Action::DropCollection,
                Action::CreateIndex,
                Action::DropIndex,
                Action::ViewStatus,
                Action::CreateUser,
                Action::GrantRole,
            ]);
        }
        _ => return None,
    }
    Some(actions)
}

/// Expands role names through built-ins and `custom` (`name`, action names).
pub fn expand_roles(
    roles: &[String],
    custom: &[(String, Vec<String>)],
) -> Result<BTreeSet<Action>, String> {
    let mut actions = BTreeSet::new();
    for role in roles {
        if let Some(builtin) = builtin_actions(role) {
            actions.extend(builtin);
            continue;
        }
        let Some((_, names)) = custom.iter().find(|(name, _)| name == role) else {
            return Err(format!("unknown role {role}"));
        };
        for name in names {
            actions.insert(Action::parse(name)?);
        }
    }
    Ok(actions)
}

/// Privilege a client command needs, if it needs one.
///
/// Handshake commands and `authenticate` return `None`. `createUser`
/// returns `Some(CreateUser)` and the caller still allows it with no
/// session when the catalog has no users.
pub fn required_action(command: &Document) -> Option<Action> {
    if is_open(command) || command.contains_key("authenticate") {
        return None;
    }
    Some(action_for(command))
}

fn is_open(command: &Document) -> bool {
    const KEYS: &[&str] = &[
        "ping",
        "hello",
        "isMaster",
        "ismaster",
        "buildInfo",
        "buildinfo",
        "getParameter",
        "endSessions",
        "whatsmyuri",
        "connectionStatus",
        "hostInfo",
        "atlasVersion",
    ];
    KEYS.iter().any(|key| command.contains_key(*key))
}

fn action_for(command: &Document) -> Action {
    if command.contains_key("insert") {
        return Action::Insert;
    }
    if command.contains_key("update")
        || command.contains_key("findAndModify")
        || command.contains_key("findandmodify")
    {
        return Action::Update;
    }
    if command.contains_key("delete") {
        return Action::Remove;
    }
    if command.contains_key("create") || command.contains_key("collMod") {
        return Action::CreateCollection;
    }
    if command.contains_key("drop") || command.contains_key("dropDatabase") {
        return Action::DropCollection;
    }
    if command.contains_key("createIndexes") {
        return Action::CreateIndex;
    }
    if command.contains_key("dropIndexes") || command.contains_key("deleteIndexes") {
        return Action::DropIndex;
    }
    if command.contains_key("collStats")
        || command.contains_key("dbStats")
        || command.contains_key("listCollections")
        || command.contains_key("listIndexes")
        || command.contains_key("listDatabases")
        || command.contains_key("serverStatus")
    {
        return Action::ViewStatus;
    }
    if command.contains_key("createUser") || command.contains_key("updateUser") {
        return Action::CreateUser;
    }
    if command.contains_key("createRole")
        || command.contains_key("dropRole")
        || command.contains_key("grantRolesToUser")
        || command.contains_key("revokeRolesFromUser")
    {
        return Action::GrantRole;
    }
    if let Ok(sql) = command.get_str("sql") {
        if let Ok(Some(actions)) = crate::dump::sql_actions(sql) {
            if let Some(name) = actions.first() {
                return action_named(name);
            }
        }
    }
    Action::Find
}

fn action_named(name: &str) -> Action {
    match name {
        "insert" => Action::Insert,
        "update" => Action::Update,
        "remove" => Action::Remove,
        "createCollection" => Action::CreateCollection,
        "dropCollection" => Action::DropCollection,
        "createIndex" => Action::CreateIndex,
        "dropIndex" => Action::DropIndex,
        _ => Action::Find,
    }
}

/// Argon2id hash. Memory is 19 MiB, two iterations, parallelism 1.
pub fn hash_password(password: &str) -> Result<String, String> {
    let salt = SaltString::generate(&mut argon2::password_hash::rand_core::OsRng);
    hasher()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|err| format!("password hash: {err}"))
}

pub fn passwords_match(password: &str, encoded: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(encoded) else {
        return false;
    };
    hasher()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

/// A valid hash used so an unknown user takes the same work as a real one.
pub fn dummy_hash() -> &'static str {
    use std::sync::OnceLock;
    static DUMMY: OnceLock<String> = OnceLock::new();
    DUMMY
        .get_or_init(|| hash_password("kova-unknown-user").expect("dummy hash"))
        .as_str()
}

fn hasher() -> Argon2<'static> {
    let params = Params::new(19 * 1024, 2, 1, None).expect("argon2 params");
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn test_each_builtin_role_grants_its_actions() {
        let read = builtin_actions("read").unwrap();
        assert_eq!(read, BTreeSet::from([Action::Find]));
        assert!(!read.contains(&Action::Insert));

        let write = builtin_actions("readWrite").unwrap();
        assert!(write.contains(&Action::Update));
        assert!(!write.contains(&Action::DropCollection));

        let admin = builtin_actions("dbAdmin").unwrap();
        assert!(admin.contains(&Action::ViewStatus));
        assert!(admin.contains(&Action::Find));
        assert!(!admin.contains(&Action::GrantRole));

        let users = builtin_actions("userAdmin").unwrap();
        assert_eq!(
            users,
            BTreeSet::from([Action::CreateUser, Action::GrantRole])
        );

        let root = builtin_actions("root").unwrap();
        assert_eq!(root.len(), 11);
    }

    #[test]
    fn test_custom_role_expands_and_unknown_action_is_named() {
        let custom = vec![("analyst".into(), vec!["find".into(), "viewStatus".into()])];
        let actions = expand_roles(&["analyst".into()], &custom).unwrap();
        assert!(actions.contains(&Action::Find));
        assert!(actions.contains(&Action::ViewStatus));
        assert!(!actions.contains(&Action::Insert));

        let bad = vec![("odd".into(), vec!["dropDatabase".into()])];
        let err = expand_roles(&["odd".into()], &bad).unwrap_err();
        assert!(err.contains("dropDatabase"), "{err}");
    }

    #[test]
    fn test_handshake_needs_no_action_and_insert_needs_insert() {
        assert!(required_action(&doc! { "ping": 1 }).is_none());
        assert!(required_action(&doc! { "authenticate": 1 }).is_none());
        assert_eq!(
            required_action(&doc! { "insert": "emp" }),
            Some(Action::Insert)
        );
        assert_eq!(
            required_action(&doc! { "sql": "select 1" }),
            Some(Action::Find)
        );
        assert_eq!(
            required_action(&doc! { "sql": "INSERT INTO emp (a) VALUES (1)" }),
            Some(Action::Insert)
        );
        assert_eq!(
            required_action(&doc! { "sql": "CREATE TABLE emp (a INT)" }),
            Some(Action::CreateCollection)
        );
        assert_eq!(
            required_action(&doc! { "sql": "UPDATE emp SET a = 1 WHERE a = 1" }),
            Some(Action::Update)
        );
        assert_eq!(
            required_action(&doc! { "sql": "DELETE FROM emp WHERE a = 1" }),
            Some(Action::Remove)
        );
    }

    #[test]
    fn test_password_round_trip_and_a_wrong_password() {
        let hash = hash_password("correct horse").unwrap();
        assert!(passwords_match("correct horse", &hash));
        assert!(!passwords_match("wrong", &hash));
        assert!(!hash.contains("correct horse"));
    }
}
