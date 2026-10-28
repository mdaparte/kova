#!/usr/bin/env python3
"""Time a listening Kova and write the benchmark page.

The server is already running. This drops and recreates only the database
`benchmark`, then fills site/docs/benchmark.html from the timings.

    make benchmark
    make benchmark KOVA_PORT=27019 BENCH_ROWS=1000 BENCH_LOOKUPS=100
"""

import json
import os
import re
import socket
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / "site" / "docs" / "benchmark.html"
DATA_PATH = ROOT / "site" / "docs" / "benchmark-data.json"
RESULT_PATH = ROOT / "site" / "docs" / "benchmark-result.json"
MARKER = "benchmark-results"


def port():
    return int(os.environ.get("KOVA_PORT", "27018"))


def mongo_port():
    return int(os.environ.get("MONGO_PORT", "27017"))


def want_mongo():
    flag = os.environ.get("BENCH_MONGO", "").strip().lower()
    if flag in ("1", "y", "yes", "true"):
        return True
    if flag in ("0", "n", "no", "false"):
        return False
    if not sys.stdin.isatty():
        return False
    number = mongo_port()
    answer = input(
        f"Also time MongoDB on 127.0.0.1:{number}? "
        "This drops only the database named benchmark. [y/N] "
    )
    return answer.strip().lower() in ("y", "yes", "s", "sim")


def listening(number):
    sock = socket.socket()
    sock.settimeout(2)
    try:
        sock.connect(("127.0.0.1", number))
    except OSError:
        return False
    finally:
        sock.close()
    return True


def listen_inodes(number):
    hexport = f"{number:04X}"
    found = set()
    for name in ("tcp", "tcp6"):
        path = Path("/proc/net") / name
        if not path.exists():
            continue
        for line in path.read_text().splitlines()[1:]:
            parts = line.split()
            if len(parts) < 10 or parts[3] != "0A":
                continue
            if parts[1].rsplit(":", 1)[-1].upper() == hexport:
                found.add(parts[9])
    return found


def process_for_port(number):
    inodes = listen_inodes(number)
    if not inodes:
        return None
    proc = Path("/proc")
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        fd_dir = entry / "fd"
        try:
            fds = list(fd_dir.iterdir())
        except OSError:
            continue
        for fd in fds:
            try:
                target = os.readlink(fd)
            except OSError:
                continue
            if target.startswith("socket:[") and target[8:-1] in inodes:
                return int(entry.name)
    return None


def read_status(pid):
    values = {}
    text = Path(f"/proc/{pid}/status").read_text()
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, raw = line.split(":", 1)
        values[key] = raw.strip()
    return values


def kib(status, key):
    raw = status.get(key, "0 kB").split()
    try:
        return int(raw[0])
    except (IndexError, ValueError):
        return 0


def io_bytes(pid):
    path = Path(f"/proc/{pid}/io")
    out = {}
    try:
        text = path.read_text()
    except OSError:
        return out
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, raw = line.split(":", 1)
        if key in ("read_bytes", "write_bytes"):
            out[key] = int(raw.strip())
    return out


def sample(pid):
    status = read_status(pid)
    return {
        "rss_kib": kib(status, "VmRSS"),
        "hwm_kib": kib(status, "VmHWM"),
        "io": io_bytes(pid),
    }


def binary_kind(exe):
    if "/release/" in exe:
        return "release"
    if "/debug/" in exe:
        return "debug"
    return "installed"


def human_bytes(value):
    number = float(value)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if number < 1024 or unit == "GiB":
            if unit == "B":
                return f"{int(number)} {unit}"
            return f"{number:.1f} {unit}"
        number /= 1024
    return f"{int(value)} B"


def esc(value):
    text = str(value)
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def attr(value):
    return esc(value).replace('"', "&quot;")


MONTHS = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()


def format_when(raw):
    moment = datetime.fromisoformat(raw).astimezone(ZoneInfo("Europe/Berlin"))
    zone = "CEST" if moment.dst() else "CET"
    return f"{moment.day:02d} {MONTHS[moment.month - 1]} {moment.year}, {moment:%H:%M:%S} {zone}"


def os_line():
    pretty = "Linux"
    release = Path("/etc/os-release")
    if release.is_file():
        for line in release.read_text().splitlines():
            if line.startswith("PRETTY_NAME="):
                pretty = line.split("=", 1)[1].strip().strip('"')
    info = os.uname()
    return f"{pretty}, {info.sysname} {info.release} {info.machine}"


def machine_ram():
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemTotal:"):
            return int(line.split()[1]) * 1024
    return None


def machine_disk():
    stats = os.statvfs("/")
    total = stats.f_frsize * stats.f_blocks
    free = stats.f_frsize * stats.f_bavail
    return f"{human_bytes(free)} free of {human_bytes(total)}"


def video_memory():
    try:
        completed = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader"],
            capture_output=True,
            text=True,
            timeout=3,
        )
    except (OSError, subprocess.TimeoutExpired):
        completed = None
    if completed and completed.returncode == 0 and completed.stdout.strip():
        cards = []
        for line in completed.stdout.splitlines():
            if "," not in line:
                continue
            name, memory = [part.strip() for part in line.split(",", 1)]
            bits = memory.split()
            amount = float(bits[0]) if bits else 0
            unit = bits[1] if len(bits) > 1 else "MiB"
            scale = {"MiB": 1024 ** 2, "GiB": 1024 ** 3, "KiB": 1024}.get(unit, 1)
            cards.append(f"{name}, {human_bytes(amount * scale)}")
        if cards:
            return "; ".join(cards)
    root = Path("/sys/class/drm")
    if root.is_dir():
        cards = []
        for path in sorted(root.glob("card*/device/mem_info_vram_total")):
            try:
                cards.append(human_bytes(int(path.read_text().strip())))
            except (OSError, ValueError):
                continue
        if cards:
            return "; ".join(cards)
    return "unavailable"


def machine_facts():
    ram = machine_ram()
    return {
        "os": os_line(),
        "ram": human_bytes(ram) if ram is not None else "unavailable",
        "disk": machine_disk(),
        "video": video_memory(),
    }


def per_second(rows, ms):
    if not rows or not ms:
        return ""
    return f"{round(rows / (ms / 1000)):,}"


def human_ms(ms):
    if ms >= 10000:
        return f"{ms / 1000:.1f} s"
    if ms >= 1000:
        return f"{ms / 1000:.2f} s"
    return f"{ms} ms"


def human_note(step):
    note = step.get("note") or step.get("error") or ""
    if "datafusion.public" in note or "SQL writes are not stored" in note:
        return "The statement was not stored."
    if "connection" in note and "closed" in note:
        return "The connection closed before a reply."
    return note


TIPS = {
    "rss": "Resident memory of this Kova process when the run finished. The rest of the machine is not included.",
    "growth": "Resident memory at the end minus resident memory at the start, for this process only.",
    "hwm": "The highest resident memory this process has reached, including what it held before the run.",
    "read": "Bytes this process read during the run.",
    "write": "Bytes this process wrote during the run.",
    "op": "The name opens the full command. A repeated lookup shows the statement and the range of keys.",
    "when": "The clock time of the run, in Central European Time.",
    "address": "The listener this run talked to.",
    "process": "The Kova binary that answered, and whether it was a debug or release build.",
    "workload": "How many orders were inserted, and how many point lookups each pass ran.",
    "os": "The operating system this benchmark ran on.",
    "ram": "Installed RAM on this machine.",
    "disk": "Free and total space on the system disk.",
    "vram": "Memory on the graphics card.",
    "result": "ok means the server finished the step. fail means it returned an error.",
    "rows": "Documents or result rows counted for this step.",
    "time": "Wall-clock time for the step, measured by the client.",
    "rate": "Rows divided by the time. A step with no rows has no rate.",
    "index": "Times this step used an index. The number is the change in indexLookups.",
    "scan": "Collection scans this step started. The number is the change in collectionScans.",
    "hit": "Buffer-pool pages this step served from memory.",
    "miss": "Buffer-pool pages this step had to read because they were not cached.",
    "wal": "Log records this step wrote.",
    "note": "Extra detail for the step.",
}


def ask(key):
    return (
        f'<button type="button" class="ask" data-tip="{key}" aria-expanded="false" '
        f'aria-label="What this figure means">?</button>'
    )


def num_cell(value):
    text = f"{value:,}"
    tone = " zero" if value == 0 else ""
    return f'<td class="num{tone}">{esc(text)}</td>'


def span(text, *classes):
    names = " ".join(name for name in classes if name)
    if names:
        return f'<span class="{names}">{esc(text)}</span>'
    return f"<span>{esc(text)}</span>"


def figure(text, who, *classes):
    names = " ".join(name for name in classes if name)
    attr = f' class="{names}"' if names else ""
    label = "Kova" if who == "kova" else "Mongo"
    return f"<span{attr}><b>{label}</b>{esc(text)}</span>"


def paired(primary, secondary, primary_zero, secondary_zero):
    return (
        '<td class="num compare">'
        f"{figure(primary, 'kova', 'zero' if primary_zero else '')}"
        f"{figure(secondary, 'mongo', 'peer', 'zero' if secondary_zero else '')}"
        "</td>"
    )


def na_figure():
    return '<span class="peer na" title="not available"><b>Mongo</b>N/A</span>'


def na_cell(text, zero):
    return (
        '<td class="num compare">'
        f"{figure(text, 'kova', 'zero' if zero else '')}"
        f"{na_figure()}"
        "</td>"
    )


def pace(kova_ms, mongo_ms):
    if kova_ms < mongo_ms:
        return "fast", "slow"
    if mongo_ms < kova_ms:
        return "slow", "fast"
    return "", ""


def gap_text(kova_ms, mongo_ms):
    delta = int(kova_ms) - int(mongo_ms)
    sign = "+" if delta > 0 else ""
    milliseconds = f"{sign}{delta:,} ms"
    if mongo_ms:
        times = f"{kova_ms / mongo_ms:.1f}×"
        return f"{milliseconds}, {times}"
    if delta == 0:
        return "0 ms, 1.0×"
    return milliseconds


def timed(kova_ms, mongo_ms):
    kova_pace, mongo_pace = pace(kova_ms, mongo_ms)
    return (
        '<td class="num compare">'
        f"{figure(human_ms(kova_ms), 'kova', kova_pace, 'zero' if kova_ms == 0 else '')}"
        f"{figure(human_ms(mongo_ms), 'mongo', 'peer', mongo_pace, 'zero' if mongo_ms == 0 else '')}"
        f"{span(gap_text(kova_ms, mongo_ms), 'gap')}"
        "</td>"
    )


def peer_of(mongo_steps, step):
    if not mongo_steps:
        return None
    other = mongo_steps.get(step.get("name") or "")
    if not other or not other.get("ok"):
        return {"na": True}
    counters = other.get("counters") or {}
    measured = "keysExamined" in counters
    return {
        "na": False,
        "ms": other.get("ms") or 0,
        "index": (counters.get("keysExamined") or 0) if measured else None,
        "scan": (counters.get("collectionScans") or 0) if measured else None,
    }


KNOWN_OPS = {
    "Clear benchmark": '{ dropDatabase: 1 }',
    "Create database benchmark": '{ create: "dept" }\n{ create: "orders" }\n{ create: "ledger" }',
    "insert departments": "db.dept.insertMany([{ dept, name }, ...])",
    "insert orders": "db.orders.insertMany(documents, batches of 500)\n{ sku, dept, job, sal, qty, note, bonus on every 10th }",
    "create index sku_1 unique": '{ createIndexes: "orders", indexes: [{ key: { sku: 1 }, name: "sku_1", unique: true }] }',
    "create index sal_1": '{ createIndexes: "orders", indexes: [{ key: { sal: 1 }, name: "sal_1" }] }',
    "create index job_hashed": '{ createIndexes: "orders", indexes: [{ key: { job: "hashed" }, name: "job_hashed" }] }',
    "create index dept_1_sal_1": '{ createIndexes: "orders", indexes: [{ key: { dept: 1, sal: 1 }, name: "dept_1_sal_1" }] }',
    "create index bonus_1 sparse": '{ createIndexes: "orders", indexes: [{ key: { bonus: 1 }, name: "bonus_1", sparse: true }] }',
    "updateMany seen": "db.orders.updateMany({}, { $set: { seen: 1 } })",
    "updateOne sku 1": 'db.orders.updateOne({ sku: 1 }, { $set: { note: "clerk keeps the amended ledger 1" } })',
    "findAndModify sku 2": '{ findAndModify: "orders", query: { sku: 2 }, update: { $inc: { qty: 1 } } }',
    "deleteOne sku 0": "db.orders.deleteOne({ sku: 0 })",
    "count orders": '{ count: "orders" }',
    "create tickets with autoincrement": '{ create: "tickets", autoincrement: "id" }',
    "insert tickets": 'db.tickets.insertMany([{ kind: "bench" }, ...], 50 documents)',
    "transaction commit": '{ beginTransaction: 1, collection: "ledger", txnId: 1, isolation: "snapshot" }\n{ insert: "ledger", documents: [{ sku: 1, qty: 1 }, { sku: 2, qty: 1 }], txnId: 1 }\n{ commitTransaction: 1, collection: "ledger", txnId: 1 }',
    "transaction abort": '{ beginTransaction: 1, collection: "ledger", txnId: 2, isolation: "snapshot" }\n{ insert: "ledger", documents: [{ sku: 9, qty: 1 }], txnId: 2 }\n{ abortTransaction: 1, collection: "ledger", txnId: 2 }',
    "create view well_paid": '{ create: "well_paid", viewOn: "orders", pipeline: [{ $match: { sal: { $gt: 2000 } } }] }',
    "create text view orders_search": '{ create: "orders_search", viewOn: "orders", pipeline: [], kova: { inverted: true, analyzer: "en_stem" } }',
    "mapReduce orders": '{ mapReduce: "orders", query: { dept: 1 }, map: { key: "$job", value: "$sal" } }',
    "find sal > 2000": "db.orders.find({ sal: { $gt: 2000 } }, { sku: 1, sal: 1 })",
    "find qty = 7": "db.orders.find({ qty: 7 }, { sku: 1 })",
    "aggregate group by job": 'db.orders.aggregate([{ $group: { _id: "$job", n: { $sum: 1 }, payroll: { $sum: "$sal" } } }])',
    "text search clerk": 'db.orders_search.find({ $text: { $search: "clerk" } }, { sku: 1, job: 1 })',
    "text search clrek fuzzy 0.3": 'db.orders_search.find({ $text: { $search: "clrek", $fuzzy: 0.3 } }, { sku: 1, job: 1 })',
    "SELECT 1": "SELECT 1 AS n",
    "SELECT sal > 2000": "SELECT sku, job, sal FROM orders WHERE sal > 2000",
    "SELECT COUNT and GROUP BY job": "SELECT job, COUNT(*) AS n, SUM(sal) AS payroll FROM orders GROUP BY job",
    "SELECT COUNT(*)": "SELECT COUNT(*) AS n FROM orders",
    "INNER JOIN dept": "SELECT a.sku, b.name FROM orders a INNER JOIN dept b ON a.dept = b.dept",
    "WHERE TEXT clerk": "SELECT sku, job, sal FROM orders_search WHERE TEXT 'clerk'",
    "WHERE TEXT clrek FUZZY 0.3": "SELECT sku, job, sal FROM orders_search WHERE TEXT 'clrek' FUZZY 0.3",
    "FACETS job and sal": "SELECT facet, bucket, doc_count FROM orders_search WHERE TEXT 'clerk' FACETS job TERMS, sal RANGES (1500), (1500, 3000), (3000)",
    "SHOW INDEXES FROM orders": "SHOW INDEXES FROM orders",
    "SHOW TABLES": "SHOW TABLES",
    "SELECT from well_paid": "SELECT sku, sal FROM well_paid",
    "SELECT from high_pay": "SELECT sku, sal FROM high_pay",
    "Create a table with SQL": "CREATE TABLE extra (sku INT)",
    "Insert a row with SQL": "INSERT INTO orders (sku) VALUES (1)",
    "Update a row with SQL": "UPDATE orders SET qty = 1 WHERE sku = 1",
    "listCollections": "{ listCollections: 1 }",
}


def operation_text(step, report):
    recorded = step.get("op") or ""
    if recorded:
        return recorded
    lookups = report.get("lookups") or 0
    rows = report.get("rows") or lookups
    name = step.get("name") or ""
    if name.startswith("findOne sku"):
        return f"db.orders.findOne({{ sku: NumberInt(n) }}, {{ sku: 1 }})\nn = 1..{lookups}"
    if name.startswith("SELECT sku"):
        return f"SELECT sku, job, sal FROM orders WHERE sku = n\nn = 1..{lookups}"
    if name == "aggregate lookup dept":
        limit = min(100, max(rows - 1, 1))
        return (
            "db.orders.aggregate(["
            f"{{ $match: {{ sku: {{ $gt: 0, $lt: {limit + 1} }} }} }}, "
            '{ $lookup: { from: "dept", localField: "dept", foreignField: "dept", as: "department" } }'
            "])"
        )
    if name == "aggregate out high_pay":
        return 'db.orders.aggregate([{ $match: { sal: { $gt: 2000 } } }, { $project: { sku: 1, sal: 1, job: 1 } }, { $out: "high_pay" }])'
    return KNOWN_OPS.get(name, "")


def step_row(step, report, mongo_steps):
    rows = step.get("rows") or 0
    ms = step.get("ms") or 0
    rate = per_second(rows, ms)
    note = human_note(step)
    counters = step.get("counters") or {}
    status = "ok" if step.get("ok") or note.startswith(("Answered ok", "Not written")) else "fail"
    rate_cell = f'<td class="num">{esc(rate)}</td>' if rate else '<td class="num zero">0</td>'
    note_cell = f"<td>{esc(note)}</td>" if note else "<td></td>"
    name = esc(step.get("name") or "")
    op = operation_text(step, report)
    if op:
        name_cell = (
            f'<td><button type="button" class="op" data-op="{attr(op)}" '
            f'aria-expanded="false">{name}</button></td>'
        )
    else:
        name_cell = f"<td>{name}</td>"
    peer = peer_of(mongo_steps, step)
    index = counters.get("indexLookups") or 0
    scan = counters.get("collectionScans") or 0
    if peer and peer.get("na"):
        time_cell = na_cell(human_ms(ms), ms == 0)
        index_cell = na_cell(f"{index:,}", index == 0)
        scan_cell = na_cell(f"{scan:,}", scan == 0)
    elif peer:
        time_cell = timed(ms, peer["ms"])
        if peer["index"] is None:
            index_cell = na_cell(f"{index:,}", index == 0)
            scan_cell = na_cell(f"{scan:,}", scan == 0)
        else:
            index_cell = paired(f"{index:,}", f"{peer['index']:,}", index == 0, peer["index"] == 0)
            scan_cell = paired(f"{scan:,}", f"{peer['scan']:,}", scan == 0, peer["scan"] == 0)
    else:
        time_cell = f'<td class="num">{esc(human_ms(ms))}</td>'
        index_cell = num_cell(index)
        scan_cell = num_cell(scan)
    return (
        "<tr>"
        f"{name_cell}"
        f'<td class="status {status}">{status}</td>'
        f"{num_cell(rows)}"
        f"{time_cell}"
        f"{rate_cell}"
        f"{index_cell}"
        f"{scan_cell}"
        f"{num_cell(counters.get('bufferPoolHits') or 0)}"
        f"{num_cell(counters.get('bufferPoolMisses') or 0)}"
        f"{num_cell(counters.get('walWrites') or 0)}"
        f"{note_cell}"
        "</tr>"
    )


def times_table(steps, report, mongo_steps):
    head = (
        "<tr>"
        f"<th>Operation {ask('op')}</th>"
        f"<th>Result {ask('result')}</th>"
        f"<th>Rows {ask('rows')}</th>"
        f"<th>Time {ask('time')}</th>"
        f"<th>Rows/s {ask('rate')}</th>"
        f"<th>Index {ask('index')}</th>"
        f"<th>Scan {ask('scan')}</th>"
        f"<th>Hit {ask('hit')}</th>"
        f"<th>Miss {ask('miss')}</th>"
        f"<th>WAL {ask('wal')}</th>"
        f"<th>Note {ask('note')}</th>"
        "</tr>"
    )
    body = "".join(step_row(step, report, mongo_steps) for step in steps)
    return f'<div class="bench-scroll"><table class="grid bench"><thead>{head}</thead><tbody>{body}</tbody></table></div>'


def glossary(compared):
    tips = dict(TIPS)
    if compared:
        tips["time"] = (
            "Wall-clock time measured by the client. Each figure is labeled Kova or Mongo. "
            "The third line is Kova minus Mongo in milliseconds, then Kova's time divided by Mongo's. "
            "An oxide mark sits beside the faster time. "
            "N/A means not available: MongoDB did not run that step."
        )
        tips["index"] = (
            "The Kova figure is indexLookups. The Mongo figure is index keys examined. "
            "N/A means not available."
        )
        tips["scan"] = (
            "Collection scans started during the step, labeled Kova and Mongo. "
            "N/A means not available."
        )
        tips["mongo"] = (
            "The same steps on a MongoDB that was already listening. "
            "Only the database named benchmark is dropped there."
        )
    items = "".join(f'<p id="tip-{key}">{esc(text)}</p>' for key, text in tips.items())
    return f'<div id="bench-glossary" hidden>{items}</div>'


def run_mongosh(number):
    uri = f"mongodb://127.0.0.1:{number}/benchmark"
    completed = subprocess.run(
        ["mongosh", "--quiet", uri, "--file", str(ROOT / "scripts" / "benchmark.js")],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    sys.stdout.write(completed.stdout)
    sys.stderr.write(completed.stderr)
    payload = None
    for line in completed.stdout.splitlines():
        if line.startswith("BENCHMARK_JSON "):
            payload = json.loads(line[len("BENCHMARK_JSON ") :])
    return completed.returncode, payload


def host_block(pid, before, after):
    try:
        exe = os.readlink(f"/proc/{pid}/exe")
    except OSError:
        exe = ""
    return {
        "when": subprocess.check_output(["date", "-Iseconds"], text=True).strip(),
        "port": port(),
        "pid": pid,
        "exe": exe,
        "build": binary_kind(exe),
        "before": before,
        "after": after,
        "machine": machine_facts(),
    }


def io_delta(before, after, key):
    start = (before.get("io") or {}).get(key)
    end = (after.get("io") or {}).get(key)
    if start is None or end is None:
        return None
    return max(0, end - start)


def results_html(report):
    host = report["host"]
    before = host["before"]
    after = host["after"]
    growth = max(0, after["rss_kib"] - before["rss_kib"])
    disk_read = io_delta(before, after, "read_bytes")
    disk_write = io_delta(before, after, "write_bytes")
    mongo = report.get("mongo") or {}
    mongo_steps = {step.get("name"): step for step in mongo.get("steps") or []}
    groups = []
    for lang, title in (("mql", "MQL"), ("sql", "SQL")):
        steps = [step for step in report["steps"] if step.get("lang") == lang]
        if not steps:
            continue
        groups.append(f"<h3>{title}</h3>{times_table(steps, report, mongo_steps)}")
    read_cell = esc(human_bytes(disk_read)) if disk_read is not None else "unavailable"
    write_cell = esc(human_bytes(disk_write)) if disk_write is not None else "unavailable"
    machine = host.get("machine") or machine_facts()
    workload = f"{report['rows']:,} orders, {report['lookups']} point lookups"
    mongo_row = ""
    compare_note = ""
    if mongo_steps:
        version = mongo.get("version") or ""
        label = f"{version} at " if version else ""
        mongo_row = (
            f"<tr><td>MongoDB {ask('mongo')}</td>"
            f"<td>{esc(label)}127.0.0.1:{esc(mongo.get('port'))}</td></tr>\n"
        )
        compare_note = (
            "<p>Each paired cell names Kova and Mongo. "
            "Under the two times, the difference is Kova minus Mongo in milliseconds, "
            "then how many times Kova's time is Mongo's. "
            "An oxide mark sits beside the faster time. "
            "N/A means not available: MongoDB did not run that step. "
            "Index is indexLookups on Kova and keys examined on Mongo. "
            "Scan is a collection scan on both.</p>"
        )
    return f"""<h2 id="run-facts">This run</h2>
<table class="grid bench-memory">
  <thead><tr><th>Figure</th><th>Value</th></tr></thead>
  <tbody>
    <tr><td>When {ask('when')}</td><td>{esc(format_when(host['when']))}</td></tr>
    <tr><td>Address {ask('address')}</td><td>127.0.0.1:{esc(host['port'])}</td></tr>
    {mongo_row}    <tr><td>Process {ask('process')}</td><td><code>{esc(host.get('exe') or 'unknown')}</code> ({esc(host.get('build') or '')})</td></tr>
    <tr><td>Workload {ask('workload')}</td><td>{esc(workload)}. The run creates the database <code>benchmark</code>.</td></tr>
    <tr><td>Operating system {ask('os')}</td><td>{esc(machine.get('os') or 'unavailable')}</td></tr>
    <tr><td>RAM {ask('ram')}</td><td class="num">{esc(machine.get('ram') or 'unavailable')}</td></tr>
    <tr><td>Disk {ask('disk')}</td><td class="num">{esc(machine.get('disk') or 'unavailable')}</td></tr>
    <tr><td>Video memory {ask('vram')}</td><td class="num">{esc(machine.get('video') or 'unavailable')}</td></tr>
  </tbody>
</table>
<p>The inserted documents are <a href="/docs/benchmark-data.json">benchmark-data.json</a>. The timings and the process figures are <a href="/docs/benchmark-result.json">benchmark-result.json</a>.</p>
<h2 id="machine">Process memory and disk</h2>
<table class="grid bench-memory">
  <thead><tr><th>Figure</th><th>This process</th></tr></thead>
  <tbody>
    <tr><td>Memory in use at the end {ask('rss')}</td><td class="num">{esc(human_bytes(after['rss_kib'] * 1024))}</td></tr>
    <tr><td>Memory growth during the run {ask('growth')}</td><td class="num">{esc(human_bytes(growth * 1024))}</td></tr>
    <tr><td>Memory high-water {ask('hwm')}</td><td class="num">{esc(human_bytes(after['hwm_kib'] * 1024))}</td></tr>
    <tr><td>Bytes read {ask('read')}</td><td class="num">{read_cell}</td></tr>
    <tr><td>Bytes written {ask('write')}</td><td class="num">{write_cell}</td></tr>
  </tbody>
</table>
<h2 id="times">Times</h2>
{compare_note}{''.join(groups)}
{glossary(bool(mongo_steps))}
<p>An operation name opens the command. A question mark explains that column. A second point lookup runs after the first has already read the pages.</p>
"""


def write_page(html):
    text = PAGE.read_text()
    pattern = re.compile(
        rf"<!-- {MARKER} -->.*?<!-- /{MARKER} -->",
        re.DOTALL,
    )
    updated, count = pattern.subn(f"<!-- {MARKER} -->\n{html}<!-- /{MARKER} -->", text, count=1)
    if count != 1:
        sys.exit(f"{PAGE} has no {MARKER} block to fill")
    PAGE.write_text(updated)


def main():
    number = port()
    compare = want_mongo()
    other = mongo_port()
    if compare and other == number:
        sys.exit("MongoDB port is the same as the Kova port. Set MONGO_PORT to the other listener.")
    if not listening(number):
        sys.exit(f"nothing is listening on 127.0.0.1:{number}. Start Kova, then make benchmark.")
    if compare and not listening(other):
        sys.exit(f"nothing is listening on 127.0.0.1:{other}. Start MongoDB, or answer no.")
    pid = process_for_port(number)
    if pid is None:
        sys.exit(f"port {number} is open, and this user cannot see which process owns it.")
    before = sample(pid)
    print(f"Kova 127.0.0.1:{number}", flush=True)
    code, report = run_mongosh(number)
    after = sample(pid)
    if report is None:
        sys.exit(code or 1)
    report["host"] = host_block(pid, before, after)
    if compare:
        print(f"MongoDB 127.0.0.1:{other}", flush=True)
        _, mongo_report = run_mongosh(other)
        if mongo_report is None:
            sys.exit(f"MongoDB on 127.0.0.1:{other} did not return a benchmark result.")
        report["mongo"] = {
            "port": other,
            "version": mongo_report.get("engine") or "",
            "steps": mongo_report.get("steps") or [],
        }
    documents = {
        "database": "benchmark",
        "rows": report.get("rows"),
        "lookups": report.get("lookups"),
        "jobs": report.get("jobs"),
        "departments": report.get("departments"),
        "orders": report.get("orders"),
        "shape": "sku, dept, job, sal, qty, note, and bonus on every 10th order. sku 0 is deleted after the indexes are built.",
    }
    DATA_PATH.write_text(json.dumps(documents, indent=2) + "\n")
    stored = dict(report)
    stored.pop("orders", None)
    stored["data"] = "/docs/benchmark-data.json"
    RESULT_PATH.write_text(json.dumps(stored, indent=2) + "\n")
    write_page(results_html(report))
    print(f"wrote {PAGE.relative_to(ROOT)}")
    print(f"wrote {DATA_PATH.relative_to(ROOT)}")
    print(f"wrote {RESULT_PATH.relative_to(ROOT)}")
    if not report.get("ok", False):
        sys.exit(1)


if __name__ == "__main__":
    main()
