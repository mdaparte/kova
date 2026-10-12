//! Aggregation pipeline and the cursor shape shared with `find`.
//!
//! Stages run over documents the caller already read from pages. A
//! leading `$match` is peeled off so the page scan can drop
//! non-matching documents before they are kept. `$lookup` of a
//! collection that lives on another partition returns
//! [`Output::Remote`]; the network layer loads that collection and
//! retries. `$out` does not write: the network layer inserts the
//! result through the normal path so the WAL records it.

use bson::{doc, Bson, Document};

use super::{bson_cmp, bson_eq, document_matches, validate_filter};

/// Foreign documents for one `$lookup`, or a signal that another
/// partition owns that collection.
pub(super) enum Foreign {
    Ready(Vec<Document>),
    Remote,
}

/// What a pipeline produced.
pub(super) enum Output {
    Docs(Vec<Document>),
    /// `$lookup` named a collection this partition does not own.
    Remote {
        namespace: String,
    },
    /// `$out` is the last stage. `namespace` is `db.collection`.
    Out {
        namespace: String,
        documents: Vec<Document>,
    },
}

/// Sort, skip, limit, then project. Sort runs before projection so a
/// later exclusion cannot hide a field the sort needs.
pub(super) fn apply_view(
    mut docs: Vec<Document>,
    projection: Option<&Document>,
    sort: Option<&Document>,
    skip: u64,
    limit: Option<u64>,
) -> Result<Vec<Document>, String> {
    if let Some(spec) = sort {
        let keys = sort_keys(spec)?;
        if !keys.is_empty() {
            docs.sort_by(|left, right| compare_docs(left, right, &keys));
        }
    }
    if skip > 0 {
        let skip = usize::try_from(skip).unwrap_or(usize::MAX);
        if skip >= docs.len() {
            docs.clear();
        } else {
            docs.drain(..skip);
        }
    }
    if let Some(limit) = limit {
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        docs.truncate(limit);
    }
    if let Some(spec) = projection {
        docs = docs
            .into_iter()
            .map(|doc| project_document(&doc, spec))
            .collect::<Result<Vec<_>, _>>()?;
    }
    Ok(docs)
}

/// Splits a prefix of `$match` stages into one filter and the index of
/// the first stage that still has to run in memory.
pub(super) fn split_leading_matches(pipeline: &[Document]) -> Result<(Document, usize), String> {
    let mut clauses = Vec::new();
    let mut index = 0;
    while index < pipeline.len() {
        let (name, value) = stage_pair(&pipeline[index])?;
        if name != "$match" {
            break;
        }
        let filter = value
            .as_document()
            .ok_or_else(|| "$match requires a document".to_string())?
            .clone();
        validate_filter(&filter)?;
        clauses.push(filter);
        index += 1;
    }
    let filter = match clauses.len() {
        0 => doc! {},
        1 => clauses.pop().expect("one clause"),
        _ => doc! { "$and": clauses },
    };
    Ok((filter, index))
}

/// Rejects a stage this phase does not implement, and a `$out` that is
/// not last.
pub(super) fn validate_pipeline(pipeline: &[Document]) -> Result<(), String> {
    let mut saw_out = false;
    for stage in pipeline {
        let (name, _) = stage_pair(stage)?;
        if saw_out {
            return Err("$out must be the last stage".to_string());
        }
        match name {
            "$match" | "$project" | "$addFields" | "$replaceRoot" | "$group" | "$sort"
            | "$skip" | "$limit" | "$count" | "$unwind" | "$lookup" | "$out" => {}
            other => return Err(format!("unknown stage '{other}'")),
        }
        if name == "$out" {
            saw_out = true;
        }
    }
    Ok(())
}

pub(super) fn run(
    input: Vec<Document>,
    stages: &[Document],
    lookup: &mut dyn FnMut(&str) -> Result<Foreign, String>,
) -> Result<Output, String> {
    let mut docs = input;
    for (index, stage) in stages.iter().enumerate() {
        let (name, value) = stage_pair(stage)?;
        match name {
            "$match" => {
                let filter = value
                    .as_document()
                    .ok_or_else(|| "$match requires a document".to_string())?;
                validate_filter(filter)?;
                docs.retain(|doc| document_matches(doc, filter));
            }
            "$project" => {
                let spec = value
                    .as_document()
                    .ok_or_else(|| "$project requires a document".to_string())?;
                docs = docs
                    .into_iter()
                    .map(|doc| project_document(&doc, spec))
                    .collect::<Result<Vec<_>, _>>()?;
            }
            "$addFields" => {
                let spec = value
                    .as_document()
                    .ok_or_else(|| "$addFields requires a document".to_string())?;
                docs = docs
                    .into_iter()
                    .map(|doc| add_fields(&doc, spec))
                    .collect::<Result<Vec<_>, _>>()?;
            }
            "$replaceRoot" => {
                let spec = value
                    .as_document()
                    .ok_or_else(|| "$replaceRoot requires a document".to_string())?;
                let expr = spec
                    .get("newRoot")
                    .ok_or_else(|| "$replaceRoot requires 'newRoot'".to_string())?;
                docs = docs
                    .into_iter()
                    .map(|doc| replace_root(&doc, expr))
                    .collect::<Result<Vec<_>, _>>()?;
            }
            "$group" => {
                let spec = value
                    .as_document()
                    .ok_or_else(|| "$group requires a document".to_string())?;
                docs = group_documents(&docs, spec)?;
            }
            "$sort" => {
                let spec = value
                    .as_document()
                    .ok_or_else(|| "$sort requires a document".to_string())?;
                docs = apply_view(docs, None, Some(spec), 0, None)?;
            }
            "$skip" => {
                let skip = non_negative(value, "$skip")?;
                docs = apply_view(docs, None, None, skip, None)?;
            }
            "$limit" => {
                let limit = non_negative(value, "$limit")?;
                docs = apply_view(docs, None, None, 0, Some(limit))?;
            }
            "$count" => {
                let field = value
                    .as_str()
                    .filter(|name| !name.is_empty() && !name.starts_with('$'))
                    .ok_or_else(|| "$count requires a field name".to_string())?;
                let mut row = Document::new();
                row.insert(field, whole_number(docs.len() as f64));
                docs = vec![row];
            }
            "$unwind" => {
                docs = unwind(docs, value)?;
            }
            "$lookup" => {
                let spec = value
                    .as_document()
                    .ok_or_else(|| "$lookup requires a document".to_string())?;
                if spec.contains_key("pipeline") {
                    return Err("$lookup with a nested pipeline is not supported".to_string());
                }
                let from = spec
                    .get_str("from")
                    .map_err(|_| "$lookup requires a 'from' string".to_string())?;
                match lookup(from)? {
                    Foreign::Remote => {
                        return Ok(Output::Remote {
                            namespace: from.to_string(),
                        });
                    }
                    Foreign::Ready(foreign) => {
                        docs = hash_lookup(docs, spec, &foreign)?;
                    }
                }
            }
            "$out" => {
                if index + 1 != stages.len() {
                    return Err("$out must be the last stage".to_string());
                }
                return Ok(Output::Out {
                    namespace: out_namespace(value)?,
                    documents: docs,
                });
            }
            other => return Err(format!("unknown stage '{other}'")),
        }
    }
    Ok(Output::Docs(docs))
}

fn stage_pair(stage: &Document) -> Result<(&str, &Bson), String> {
    if stage.len() != 1 {
        return Err("a pipeline stage must contain exactly one field".to_string());
    }
    let (name, value) = stage.iter().next().expect("one field");
    if !name.starts_with('$') {
        return Err(format!("unknown stage '{name}'"));
    }
    Ok((name.as_str(), value))
}

fn out_namespace(value: &Bson) -> Result<String, String> {
    match value {
        Bson::String(name) if !name.is_empty() => Ok(name.clone()),
        Bson::Document(spec) => {
            let coll = spec
                .get_str("coll")
                .map_err(|_| "$out requires a 'coll' string".to_string())?;
            match spec.get_str("db") {
                Ok(db) if !db.is_empty() => Ok(format!("{db}.{coll}")),
                _ => Ok(coll.to_string()),
            }
        }
        _ => Err("$out requires a collection name".to_string()),
    }
}

fn non_negative(value: &Bson, stage: &str) -> Result<u64, String> {
    let number = bson_i64(value).ok_or_else(|| format!("{stage} requires an integer"))?;
    if number < 0 {
        return Err(format!("{stage} must be non-negative"));
    }
    Ok(number as u64)
}

fn bson_i64(value: &Bson) -> Option<i64> {
    match value {
        Bson::Int32(n) => Some(i64::from(*n)),
        Bson::Int64(n) => Some(*n),
        Bson::Double(n) if n.is_finite() && n.fract() == 0.0 => Some(*n as i64),
        _ => None,
    }
}

struct SortKey {
    path: String,
    descending: bool,
}

fn sort_keys(spec: &Document) -> Result<Vec<SortKey>, String> {
    let mut keys = Vec::with_capacity(spec.len());
    for (path, direction) in spec {
        let number =
            bson_i64(direction).ok_or_else(|| format!("sort on '{path}' must be 1 or -1"))?;
        if number != 1 && number != -1 {
            return Err(format!("sort on '{path}' must be 1 or -1"));
        }
        keys.push(SortKey {
            path: path.clone(),
            descending: number < 0,
        });
    }
    Ok(keys)
}

fn compare_docs(left: &Document, right: &Document, keys: &[SortKey]) -> std::cmp::Ordering {
    for key in keys {
        let mut order = compare_values(get_path(left, &key.path), get_path(right, &key.path));
        if key.descending {
            order = order.reverse();
        }
        if order != std::cmp::Ordering::Equal {
            return order;
        }
    }
    std::cmp::Ordering::Equal
}

fn compare_values(left: Option<&Bson>, right: Option<&Bson>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let left = match left {
        None | Some(Bson::Null) => None,
        Some(value) => Some(value),
    };
    let right = match right {
        None | Some(Bson::Null) => None,
        Some(value) => Some(value),
    };
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => {
            if let Some(order) = bson_cmp(Some(left), right) {
                return order;
            }
            type_rank(left)
                .cmp(&type_rank(right))
                .then_with(|| canonical(left).cmp(&canonical(right)))
        }
    }
}

fn type_rank(value: &Bson) -> u8 {
    match value {
        Bson::Null => 0,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => 1,
        Bson::String(_) => 2,
        Bson::Document(_) => 3,
        Bson::Array(_) => 4,
        Bson::Binary(_) => 5,
        Bson::ObjectId(_) => 6,
        Bson::Boolean(_) => 7,
        Bson::DateTime(_) => 8,
        Bson::RegularExpression(_) => 9,
        _ => 10,
    }
}

fn project_document(doc: &Document, spec: &Document) -> Result<Document, String> {
    if spec.is_empty() {
        return Ok(doc.clone());
    }
    let mut excluding = false;
    let mut including = false;
    for (key, value) in spec {
        if key == "_id" && is_exclusion(value) {
            continue;
        }
        if is_exclusion(value) {
            excluding = true;
        } else {
            including = true;
        }
    }
    if excluding && including {
        return Err("projection mixes inclusion and exclusion".to_string());
    }
    if excluding {
        let mut out = doc.clone();
        for (key, value) in spec {
            if is_exclusion(value) {
                remove_path(&mut out, key);
            }
        }
        return Ok(out);
    }
    let only_drop_id = spec
        .iter()
        .all(|(key, value)| key == "_id" && is_exclusion(value));
    if only_drop_id {
        let mut out = doc.clone();
        out.remove("_id");
        return Ok(out);
    }
    let mut out = Document::new();
    match spec.get("_id") {
        Some(value) if is_exclusion(value) => {}
        Some(value) if !is_inclusion_flag(value) => {
            out.insert("_id", eval_expr(doc, value)?);
        }
        _ => {
            if let Some(id) = doc.get("_id") {
                out.insert("_id", id.clone());
            }
        }
    }
    for (key, value) in spec {
        if key == "_id" {
            continue;
        }
        if is_inclusion_flag(value) {
            if let Some(found) = get_path(doc, key) {
                set_path(&mut out, key, found.clone());
            }
        } else {
            set_path(&mut out, key, eval_expr(doc, value)?);
        }
    }
    Ok(out)
}

fn is_exclusion(value: &Bson) -> bool {
    match value {
        Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false) => true,
        Bson::Double(number) => *number == 0.0,
        _ => false,
    }
}

fn is_inclusion_flag(value: &Bson) -> bool {
    match value {
        Bson::Int32(1) | Bson::Int64(1) | Bson::Boolean(true) => true,
        Bson::Double(number) => *number == 1.0,
        _ => false,
    }
}

fn add_fields(doc: &Document, spec: &Document) -> Result<Document, String> {
    let mut out = doc.clone();
    for (key, value) in spec {
        set_path(&mut out, key, eval_expr(doc, value)?);
    }
    Ok(out)
}

fn replace_root(doc: &Document, expr: &Bson) -> Result<Document, String> {
    match eval_expr(doc, expr)? {
        Bson::Document(root) => Ok(root),
        _ => Err("$replaceRoot 'newRoot' must resolve to a document".to_string()),
    }
}

fn eval_expr(doc: &Document, expr: &Bson) -> Result<Bson, String> {
    match expr {
        Bson::String(path) if path == "$$ROOT" || path == "$$CURRENT" => {
            Ok(Bson::Document(doc.clone()))
        }
        Bson::String(path) if path.starts_with("$$") => {
            Err(format!("unknown expression variable '{path}'"))
        }
        Bson::String(path) if path.starts_with('$') => {
            let name = &path[1..];
            if name.is_empty() {
                return Err("empty field path".to_string());
            }
            Ok(get_path(doc, name).cloned().unwrap_or(Bson::Null))
        }
        Bson::Document(inner) if is_single_operator(inner) => {
            let (op, operand) = inner.iter().next().expect("one operator");
            match op.as_str() {
                "$literal" => Ok(operand.clone()),
                other => Err(format!("unknown expression '{other}'")),
            }
        }
        Bson::Document(inner) => {
            let mut out = Document::new();
            for (key, value) in inner {
                out.insert(key, eval_expr(doc, value)?);
            }
            Ok(Bson::Document(out))
        }
        Bson::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(eval_expr(doc, item)?);
            }
            Ok(Bson::Array(out))
        }
        other => Ok(other.clone()),
    }
}

fn is_single_operator(doc: &Document) -> bool {
    doc.len() == 1 && doc.keys().next().is_some_and(|key| key.starts_with('$'))
}

#[derive(Clone, Copy)]
enum AccOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Push,
    AddToSet,
    First,
    Last,
}

#[derive(Clone)]
struct AccSpec {
    name: String,
    op: AccOp,
    operand: Bson,
}

struct AccState {
    spec: AccSpec,
    sum: f64,
    nums: u64,
    extreme: Option<Bson>,
    first: Option<Bson>,
    last: Option<Bson>,
    values: Vec<Bson>,
}

struct Group {
    key: String,
    id: Bson,
    accs: Vec<AccState>,
}

fn group_documents(docs: &[Document], spec: &Document) -> Result<Vec<Document>, String> {
    let id_expr = spec
        .get("_id")
        .ok_or_else(|| "$group requires '_id'".to_string())?;
    let mut specs = Vec::new();
    for (name, value) in spec {
        if name == "_id" {
            continue;
        }
        let operators = value
            .as_document()
            .ok_or_else(|| format!("$group field '{name}' must be an accumulator"))?;
        if operators.len() != 1 {
            return Err(format!("$group field '{name}' must be one accumulator"));
        }
        let (op, operand) = operators.iter().next().expect("one accumulator");
        let op = match op.as_str() {
            "$sum" => AccOp::Sum,
            "$avg" => AccOp::Avg,
            "$min" => AccOp::Min,
            "$max" => AccOp::Max,
            "$count" => AccOp::Count,
            "$push" => AccOp::Push,
            "$addToSet" => AccOp::AddToSet,
            "$first" => AccOp::First,
            "$last" => AccOp::Last,
            other => return Err(format!("unknown accumulator '{other}'")),
        };
        specs.push(AccSpec {
            name: name.clone(),
            op,
            operand: operand.clone(),
        });
    }

    let mut groups: Vec<Group> = Vec::new();
    for doc in docs {
        let id = eval_expr(doc, id_expr)?;
        let key = canonical(&id);
        let group = if let Some(existing) = groups.iter_mut().find(|group| group.key == key) {
            existing
        } else {
            groups.push(Group {
                key,
                id,
                accs: specs
                    .iter()
                    .cloned()
                    .map(|spec| AccState {
                        spec,
                        sum: 0.0,
                        nums: 0,
                        extreme: None,
                        first: None,
                        last: None,
                        values: Vec::new(),
                    })
                    .collect(),
            });
            groups.last_mut().expect("just pushed")
        };
        for acc in &mut group.accs {
            observe(doc, acc)?;
        }
    }

    let mut out = Vec::with_capacity(groups.len());
    for group in groups {
        let mut row = doc! { "_id": group.id };
        for acc in group.accs {
            let name = acc.spec.name.clone();
            row.insert(name, finish_acc(acc));
        }
        out.push(row);
    }
    Ok(out)
}

fn observe(doc: &Document, acc: &mut AccState) -> Result<(), String> {
    if matches!(acc.spec.op, AccOp::Count) {
        acc.nums += 1;
        return Ok(());
    }
    let value = eval_expr(doc, &acc.spec.operand)?;
    match acc.spec.op {
        AccOp::Sum => {
            if let Some(number) = as_f64(&value) {
                acc.sum += number;
            }
        }
        AccOp::Avg => {
            if let Some(number) = as_f64(&value) {
                acc.sum += number;
                acc.nums += 1;
            }
        }
        AccOp::Min => tighten(acc, value, true),
        AccOp::Max => tighten(acc, value, false),
        AccOp::Push => acc.values.push(value),
        AccOp::AddToSet => {
            if !acc.values.iter().any(|kept| bson_eq(kept, &value)) {
                acc.values.push(value);
            }
        }
        AccOp::First => {
            if acc.first.is_none() {
                acc.first = Some(value);
            }
        }
        AccOp::Last => acc.last = Some(value),
        AccOp::Count => {}
    }
    Ok(())
}

fn tighten(acc: &mut AccState, value: Bson, want_min: bool) {
    if matches!(value, Bson::Null) {
        return;
    }
    match &acc.extreme {
        None => acc.extreme = Some(value),
        Some(current) => {
            let order = compare_values(Some(&value), Some(current));
            let replace = if want_min {
                order.is_lt()
            } else {
                order.is_gt()
            };
            if replace {
                acc.extreme = Some(value);
            }
        }
    }
}

fn finish_acc(acc: AccState) -> Bson {
    match acc.spec.op {
        AccOp::Sum => whole_number(acc.sum),
        AccOp::Avg => {
            if acc.nums == 0 {
                Bson::Null
            } else {
                Bson::Double(acc.sum / acc.nums as f64)
            }
        }
        AccOp::Min | AccOp::Max => acc.extreme.unwrap_or(Bson::Null),
        AccOp::Count => whole_number(acc.nums as f64),
        AccOp::Push | AccOp::AddToSet => Bson::Array(acc.values),
        AccOp::First => acc.first.unwrap_or(Bson::Null),
        AccOp::Last => acc.last.unwrap_or(Bson::Null),
    }
}

fn whole_number(number: f64) -> Bson {
    if number.is_finite() && number.fract() == 0.0 && number.abs() < i64::MAX as f64 {
        let whole = number as i64;
        if (i32::MIN as i64..=i32::MAX as i64).contains(&whole) {
            return Bson::Int32(whole as i32);
        }
        return Bson::Int64(whole);
    }
    Bson::Double(number)
}

fn unwind(docs: Vec<Document>, spec: &Bson) -> Result<Vec<Document>, String> {
    let (path, preserve) = match spec {
        Bson::String(path) => (path.as_str(), false),
        Bson::Document(spec) => {
            let path = spec
                .get_str("path")
                .map_err(|_| "$unwind requires a 'path' string".to_string())?;
            let preserve = spec.get_bool("preserveNullAndEmptyArrays").unwrap_or(false);
            (path, preserve)
        }
        _ => return Err("$unwind requires a path".to_string()),
    };
    let field = path
        .strip_prefix('$')
        .filter(|name| !name.is_empty())
        .ok_or_else(|| "$unwind path must be a field path".to_string())?;
    let mut out = Vec::new();
    for doc in docs {
        match get_path(&doc, field) {
            Some(Bson::Array(items)) if !items.is_empty() => {
                for item in items.clone() {
                    let mut copy = doc.clone();
                    set_path(&mut copy, field, item);
                    out.push(copy);
                }
            }
            Some(Bson::Array(_)) | None | Some(Bson::Null) if preserve => out.push(doc),
            Some(Bson::Array(_)) | None | Some(Bson::Null) => {}
            Some(_) => {
                return Err(format!("$unwind '{path}' is not an array"));
            }
        }
    }
    Ok(out)
}

fn hash_lookup(
    docs: Vec<Document>,
    spec: &Document,
    foreign: &[Document],
) -> Result<Vec<Document>, String> {
    let local_field = spec
        .get_str("localField")
        .map_err(|_| "$lookup requires 'localField'".to_string())?;
    let foreign_field = spec
        .get_str("foreignField")
        .map_err(|_| "$lookup requires 'foreignField'".to_string())?;
    let as_field = spec
        .get_str("as")
        .map_err(|_| "$lookup requires 'as'".to_string())?;

    let mut index: std::collections::HashMap<String, Vec<Document>> =
        std::collections::HashMap::new();
    for doc in foreign {
        let key = match get_path(doc, foreign_field) {
            Some(value) => canonical(value),
            None => canonical(&Bson::Null),
        };
        index.entry(key).or_default().push(doc.clone());
    }

    let mut out = Vec::with_capacity(docs.len());
    for mut doc in docs {
        let key = match get_path(&doc, local_field) {
            Some(value) => canonical(value),
            None => canonical(&Bson::Null),
        };
        let matches = index.get(&key).cloned().unwrap_or_default();
        let array = matches.into_iter().map(Bson::Document).collect::<Vec<_>>();
        doc.insert(as_field, Bson::Array(array));
        out.push(doc);
    }
    Ok(out)
}

pub(super) fn get_path<'a>(doc: &'a Document, path: &str) -> Option<&'a Bson> {
    let (head, tail) = match path.split_once('.') {
        Some((head, tail)) => (head, Some(tail)),
        None => (path, None),
    };
    let value = doc.get(head)?;
    match tail {
        None => Some(value),
        Some(rest) => get_path(value.as_document()?, rest),
    }
}

pub(super) fn set_path(doc: &mut Document, path: &str, value: Bson) {
    if let Some((head, tail)) = path.split_once('.') {
        if !matches!(doc.get(head), Some(Bson::Document(_))) {
            doc.insert(head, Document::new());
        }
        let nested = doc.get_document_mut(head).expect("nested document");
        set_path(nested, tail, value);
    } else {
        doc.insert(path, value);
    }
}

pub(super) fn remove_path(doc: &mut Document, path: &str) -> Option<Bson> {
    if let Some((head, tail)) = path.split_once('.') {
        let nested = doc.get_document_mut(head).ok()?;
        remove_path(nested, tail)
    } else {
        doc.remove(path)
    }
}

pub(super) fn canonical(value: &Bson) -> String {
    match value {
        Bson::Int32(n) => format!("n:{n}"),
        Bson::Int64(n) => format!("n:{n}"),
        Bson::Double(n) if n.is_finite() && n.fract() == 0.0 && n.abs() < i64::MAX as f64 => {
            format!("n:{}", *n as i64)
        }
        Bson::Double(n) => format!("d:{}", n.to_bits()),
        Bson::String(text) => format!("s:{text}"),
        Bson::Boolean(flag) => format!("b:{flag}"),
        Bson::Null => "null".to_string(),
        Bson::ObjectId(id) => format!("o:{id}"),
        Bson::DateTime(date) => format!("t:{}", date.timestamp_millis()),
        Bson::Document(fields) => {
            let body = fields
                .iter()
                .map(|(key, value)| format!("{key}:{}", canonical(value)))
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{body}}}")
        }
        Bson::Array(items) => {
            let body = items.iter().map(canonical).collect::<Vec<_>>().join(",");
            format!("[{body}]")
        }
        other => format!("x:{other:?}"),
    }
}

fn as_f64(value: &Bson) -> Option<f64> {
    match value {
        Bson::Int32(n) => Some(f64::from(*n)),
        Bson::Int64(n) => Some(*n as f64),
        Bson::Double(n) if n.is_finite() => Some(*n),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn num(doc: &Document, key: &str) -> f64 {
        match doc.get(key) {
            Some(Bson::Int32(n)) => f64::from(*n),
            Some(Bson::Int64(n)) => *n as f64,
            Some(Bson::Double(n)) => *n,
            other => panic!("{key} is not a number: {other:?}"),
        }
    }

    fn no_lookup(_: &str) -> Result<Foreign, String> {
        Ok(Foreign::Ready(Vec::new()))
    }

    #[test]
    fn pipeline_matches_projects_sorts_and_groups() {
        let input = vec![
            doc! { "_id": 1, "ename": "SMITH", "job": "CLERK", "sal": 800, "tags": ["a", "b"] },
            doc! { "_id": 2, "ename": "ALLEN", "job": "SALESMAN", "sal": 1600, "tags": ["a"] },
            doc! { "_id": 3, "ename": "KING", "job": "PRESIDENT", "sal": 5000, "tags": [] },
        ];
        let pipeline = vec![
            doc! { "$match": { "sal": { "$gt": 1000 } } },
            doc! { "$project": { "ename": 1, "job": "$job", "sal": 1, "_id": 0 } },
            doc! { "$sort": { "sal": -1 } },
        ];
        let Output::Docs(docs) = run(input, &pipeline, &mut no_lookup).unwrap() else {
            panic!("expected documents");
        };
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].get_str("ename").unwrap(), "KING");
        assert!(docs[0].get("_id").is_none());
        assert_eq!(docs[1].get_str("job").unwrap(), "SALESMAN");

        let grouped = vec![
            doc! { "job": "CLERK", "sal": 800, "ename": "SMITH" },
            doc! { "job": "CLERK", "sal": 1100, "ename": "ADAMS" },
            doc! { "job": "MANAGER", "sal": 2850, "ename": "BLAKE" },
        ];
        let pipeline = vec![doc! {
            "$group": {
                "_id": "$job",
                "total": { "$sum": "$sal" },
                "avg": { "$avg": "$sal" },
                "n": { "$count": {} },
                "min": { "$min": "$sal" },
                "max": { "$max": "$sal" },
                "names": { "$push": "$ename" },
                "jobs": { "$addToSet": "$job" },
                "first": { "$first": "$ename" },
                "last": { "$last": "$ename" },
            }
        }];
        let Output::Docs(rows) = run(grouped, &pipeline, &mut no_lookup).unwrap() else {
            panic!("expected groups");
        };
        let clerks = rows
            .iter()
            .find(|row| row.get_str("_id") == Ok("CLERK"))
            .unwrap();
        assert_eq!(num(clerks, "total"), 1900.0);
        assert_eq!(num(clerks, "avg"), 950.0);
        assert_eq!(num(clerks, "n"), 2.0);
        assert_eq!(num(clerks, "min"), 800.0);
        assert_eq!(num(clerks, "max"), 1100.0);
        assert_eq!(clerks.get_array("names").unwrap().len(), 2);
        assert_eq!(clerks.get_array("jobs").unwrap().len(), 1);
        assert_eq!(clerks.get_str("first").unwrap(), "SMITH");
        assert_eq!(clerks.get_str("last").unwrap(), "ADAMS");
    }

    #[test]
    fn stages_add_fields_unwind_lookup_and_out() {
        let input = vec![
            doc! { "_id": 1, "deptno": 10, "tags": ["a", "b"], "nested": { "ename": "KING" } },
        ];
        let foreign = vec![
            doc! { "deptno": 10, "dname": "ACCOUNTING" },
            doc! { "deptno": 20, "dname": "RESEARCH" },
        ];
        let pipeline = vec![
            doc! { "$addFields": { "bonus": 100 } },
            doc! { "$unwind": "$tags" },
            doc! { "$lookup": { "from": "db.dept", "localField": "deptno", "foreignField": "deptno", "as": "dept" } },
            doc! { "$replaceRoot": { "newRoot": { "tag": "$tags", "dname": { "$literal": "kept" } } } },
        ];
        let Output::Docs(docs) = run(input, &pipeline, &mut |ns| {
            assert_eq!(ns, "db.dept");
            Ok(Foreign::Ready(foreign.clone()))
        })
        .unwrap() else {
            panic!("expected documents");
        };
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].get_str("tag").unwrap(), "a");
        assert_eq!(docs[0].get_str("dname").unwrap(), "kept");

        let counted = run(
            vec![doc! { "n": 1 }, doc! { "n": 2 }],
            &[doc! { "$count": "n" }],
            &mut no_lookup,
        )
        .unwrap();
        let Output::Docs(rows) = counted else {
            panic!("count")
        };
        assert_eq!(rows[0].get_i32("n").unwrap(), 2);

        let remote = run(
            vec![doc! { "deptno": 10 }],
            &[doc! { "$lookup": { "from": "db.dept", "localField": "deptno", "foreignField": "deptno", "as": "dept" } }],
            &mut |_| Ok(Foreign::Remote),
        )
        .unwrap();
        assert!(matches!(remote, Output::Remote { namespace } if namespace == "db.dept"));

        let written = run(
            vec![doc! { "_id": 1 }],
            &[doc! { "$out": "db.copy" }],
            &mut no_lookup,
        )
        .unwrap();
        assert!(
            matches!(written, Output::Out { namespace, documents } if namespace == "db.copy" && documents.len() == 1)
        );
    }

    #[test]
    fn unknown_stage_is_named() {
        let error = validate_pipeline(&[doc! { "$bogus": 1 }]).unwrap_err();
        assert!(error.contains("$bogus"), "{error}");
        let error = validate_pipeline(&[doc! { "$out": "c" }, doc! { "$match": {} }]).unwrap_err();
        assert!(error.contains("last"), "{error}");
    }

    #[test]
    fn find_view_skips_limits_and_projects() {
        let docs = vec![
            doc! { "_id": 1, "sal": 100, "job": "CLERK" },
            doc! { "_id": 2, "sal": 300, "job": "MANAGER" },
            doc! { "_id": 3, "sal": 200, "job": "SALESMAN" },
        ];
        let viewed = apply_view(
            docs,
            Some(&doc! { "job": 1, "_id": 0 }),
            Some(&doc! { "sal": 1 }),
            1,
            Some(1),
        )
        .unwrap();
        assert_eq!(viewed.len(), 1);
        assert_eq!(viewed[0].get_str("job").unwrap(), "SALESMAN");
        assert!(viewed[0].get("sal").is_none());
    }
}
