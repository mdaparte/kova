#!/usr/bin/env python3
"""Window for kova-setup. Reads a suggestion JSON on stdin and prints the answers."""

import json
import sys
import tkinter as tk
from tkinter import messagebox


# The page symbol, sampled the same way as the terminal cover: 48×48,
# one row per two pixels, so the gap stays a quarter of a bar.
SLABS = (
    (0, 0, 20, 120, "stone"),
    (70, 0, 50, 20, "stone"),
    (50, 25, 50, 20, "stone"),
    (30, 50, 50, 20, "oxide"),
    (50, 75, 50, 20, "stone"),
    (70, 100, 50, 20, "stone"),
)


def sample(px, py):
    x = (px + 0.5) * 2.5
    y = (py + 0.5) * 2.5
    for sx, sy, width, height, ink in SLABS:
        if sx <= x < sx + width and sy <= y < sy + height:
            return ink
    return None


def mark_lines():
    lines = []
    for py in range(0, 48, 2):
        runs = []
        text = []
        ink = None
        for px in range(48):
            here = sample(px, py)
            if here != ink and text:
                runs.append(("".join(text), ink or "stone"))
                text = []
            ink = here
            text.append("█" if here else " ")
        if text:
            runs.append(("".join(text), ink or "stone"))
        lines.append(runs)
    return lines


def main() -> None:
    raw = sys.stdin.read()
    suggestion = json.loads(raw)
    root = tk.Tk()
    root.title("Kova setup")
    root.resizable(False, False)

    port = tk.StringVar(value=str(suggestion["port"]))
    data_dir = tk.StringVar(value=suggestion["data_dir"])
    run_iggy = tk.BooleanVar(value=bool(suggestion["run_iggy"]))
    iggy_user = tk.StringVar(value=suggestion["iggy_user"])
    iggy_password = tk.StringVar(value=suggestion["iggy_password"])
    keep_password = bool(suggestion["keep_iggy_password"])
    leave_open = tk.BooleanVar(value=not bool(suggestion["security_enabled"]))
    confirmation = tk.StringVar()
    root_user = tk.StringVar(value=suggestion["root_user"])
    root_password = tk.StringVar(value=suggestion["root_password"])
    service = tk.BooleanVar(value=bool(suggestion["service_installed"]))
    log_target = tk.StringVar(value=suggestion["log_target"])
    log_rotate = tk.StringVar(value=suggestion["log_rotate"])
    log_max = tk.StringVar(value=str(suggestion["log_max_bytes"]))
    log_keep = tk.StringVar(value=str(suggestion["log_keep"]))
    was_open = not bool(suggestion["security_enabled"])

    root.configure(bg="#17191c")
    face = "JetBrains Mono"
    body = tk.Frame(root, bg="#17191c")
    body.grid(row=0, column=0, sticky="nsew")

    cover = tk.Frame(body, bg="#17191c", padx=18, pady=16)
    cover.grid(row=0, column=0, sticky="ns")
    mark = tk.Text(
        cover,
        height=26,
        width=50,
        bg="#17191c",
        fg="#eceeeb",
        insertbackground="#eceeeb",
        font=(face, 7),
        bd=0,
        highlightthickness=0,
    )
    mark.tag_configure("oxide", foreground="#c2461b")
    mark.tag_configure("muted", foreground="#9aa1a6")
    mark.tag_configure("stone", foreground="#eceeeb")
    mark.insert("end", "████", "oxide")
    mark.insert("end", "  DATABASE ENGINE\n\n", "muted")
    for line in mark_lines():
        for text, ink in line:
            mark.insert("end", text, ink)
        mark.insert("end", "\n")
    mark.configure(state="disabled")
    mark.grid(row=0, column=0, sticky="nw")
    tk.Label(
        cover,
        text="KOVA",
        bg="#17191c",
        fg="#eceeeb",
        font=(face, 22, "bold"),
        anchor="w",
    ).grid(row=1, column=0, sticky="ew", pady=(8, 0))
    tk.Label(
        cover,
        text="Documents, SQL, and text search.",
        bg="#17191c",
        fg="#d3d7d6",
        font=(face, 10),
        anchor="w",
    ).grid(row=2, column=0, sticky="ew", pady=(4, 0))

    frame = tk.Frame(body, padx=16, pady=16, bg="#eceeeb")
    frame.grid(row=0, column=1, sticky="ns")

    def row(label, variable, r):
        tk.Label(frame, text=label, anchor="w", bg="#eceeeb", fg="#17191c").grid(
            row=r, column=0, sticky="w", pady=3
        )
        tk.Entry(
            frame,
            textvariable=variable,
            width=36,
            bg="#f6f7f5",
            fg="#17191c",
            insertbackground="#17191c",
            relief="flat",
        ).grid(row=r, column=1, sticky="w")

    def check(text, variable, r):
        tk.Checkbutton(
            frame,
            text=text,
            variable=variable,
            bg="#eceeeb",
            fg="#17191c",
            activebackground="#eceeeb",
            activeforeground="#17191c",
            selectcolor="#eceeeb",
        ).grid(row=r, column=0, columnspan=2, sticky="w", pady=3)

    row("Listen port", port, 0)
    row("Data directory", data_dir, 1)
    check("Run the bundled iggy-server", run_iggy, 2)
    row("Iggy user", iggy_user, 3)
    password_label = "Iggy password (blank keeps the current)" if keep_password else "Iggy password"
    row(password_label, iggy_password, 4)
    check("Leave security open", leave_open, 5)
    row("Type OPEN to leave security open", confirmation, 6)
    row("Root user", root_user, 7)
    row("Root password", root_password, 8)
    check("Start with the operating system", service, 9)
    row("Log target", log_target, 10)
    row("Log rotation", log_rotate, 11)
    row("Log max bytes", log_max, 12)
    row("Log files to keep", log_keep, 13)

    saved = {"ok": False}

    def save() -> None:
        if leave_open.get() and not was_open and confirmation.get().strip() != "OPEN":
            messagebox.showerror("Kova setup", "Type OPEN to leave security open.")
            return
        try:
            payload = {
                "port": int(port.get().strip()),
                "data_dir": data_dir.get().strip(),
                "run_iggy": bool(run_iggy.get()),
                "iggy_user": iggy_user.get().strip(),
                "iggy_password": iggy_password.get(),
                "keep_iggy_password": keep_password and iggy_password.get() == "",
                "leave_open": bool(leave_open.get()),
                "confirmation": confirmation.get(),
                "root_user": root_user.get().strip(),
                "root_password": root_password.get(),
                "service": bool(service.get()),
                "log_target": log_target.get().strip() or "stderr",
                "log_rotate": log_rotate.get().strip() or "none",
                "log_max_bytes": int(log_max.get().strip()),
                "log_keep": int(log_keep.get().strip()),
            }
        except ValueError:
            messagebox.showerror("Kova setup", "Port, max bytes, and keep must be numbers.")
            return
        sys.stdout.write(json.dumps(payload))
        sys.stdout.flush()
        saved["ok"] = True
        root.destroy()

    tk.Button(
        frame,
        text="Save",
        command=save,
        bg="#c2461b",
        fg="#eceeeb",
        activebackground="#a33a14",
        activeforeground="#eceeeb",
        relief="flat",
        padx=16,
        pady=6,
    ).grid(row=14, column=1, sticky="e", pady=(12, 0))
    root.mainloop()
    if not saved["ok"]:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except tk.TclError as exc:
        sys.stderr.write(f"{exc}\n")
        sys.exit(1)
