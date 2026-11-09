//! Window onto a running Kova. The page is drawn by the system web view.
//! A machine without a display is told to use kova-shell.
//!
//! This binary is built with `--features window`. The server is not.

use std::process::ExitCode;

use tao::dpi::LogicalSize;
use tao::event::{Event, WindowEvent};
use tao::event_loop::{ControlFlow, EventLoopBuilder};
use tao::window::{Icon, Window, WindowBuilder};
use wry::http::Request;
use wry::WebViewBuilder;

fn main() -> ExitCode {
    // WebKit on Linux draws an empty window unless compositing is off.
    std::env::set_var("WEBKIT_DISABLE_COMPOSITING_MODE", "1");
    std::env::set_var("WEBKIT_DISABLE_DMABUF_RENDERER", "1");
    kova::install_panic_hook();
    if !kova::setup::graphical_session() {
        eprintln!("kova-client: no graphical session. kova-shell is the path");
        return ExitCode::from(1);
    }
    if let Err(err) = open_window() {
        eprintln!("kova-client: {err}");
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}

#[derive(Debug)]
enum UserEvent {
    Ipc(String),
}

fn open_window() -> Result<(), String> {
    let mut builder = EventLoopBuilder::<UserEvent>::with_user_event();
    #[cfg(target_os = "linux")]
    {
        use tao::platform::unix::EventLoopBuilderExtUnix;
        builder.with_app_id("dev.kova.client");
    }
    let event_loop = builder.build();
    let proxy = event_loop.create_proxy();
    let icon = window_icon();
    let window = WindowBuilder::new()
        .with_title("KOVA")
        .with_window_icon(icon)
        .with_inner_size(LogicalSize::new(1100.0, 720.0))
        .build(&event_loop)
        .map_err(|err| err.to_string())?;
    if let Some(icon) = window_icon() {
        window.set_window_icon(Some(icon));
    }
    #[cfg(target_os = "linux")]
    publish_dock_icon(&window);
    // On Linux the X11 overlay leaves the Tao window empty and parks the
    // page in another window. Put the page in the box Tao already added.
    let webview = {
        #[cfg(any(
            target_os = "linux",
            target_os = "dragonfly",
            target_os = "freebsd",
            target_os = "netbsd",
            target_os = "openbsd"
        ))]
        {
            use tao::platform::unix::WindowExtUnix;
            use wry::WebViewBuilderExtUnix;
            let vbox = window
                .default_vbox()
                .ok_or_else(|| "the window has no box for the page".to_string())?;
            WebViewBuilder::new()
                .with_html(kova::shell::page())
                .with_ipc_handler(move |request: Request<String>| {
                    let _ = proxy.send_event(UserEvent::Ipc(request.into_body()));
                })
                .build_gtk(vbox)
                .map_err(|err| err.to_string())?
        }
        #[cfg(not(any(
            target_os = "linux",
            target_os = "dragonfly",
            target_os = "freebsd",
            target_os = "netbsd",
            target_os = "openbsd"
        )))]
        {
            WebViewBuilder::new()
                .with_html(kova::shell::page())
                .with_ipc_handler(move |request: Request<String>| {
                    let _ = proxy.send_event(UserEvent::Ipc(request.into_body()));
                })
                .build(&window)
                .map_err(|err| err.to_string())?
        }
    };

    let mut ui = kova::shell::Ui::new();
    event_loop.run(move |event, _target, control_flow| {
        *control_flow = ControlFlow::Wait;
        match event {
            Event::UserEvent(UserEvent::Ipc(body)) => {
                let request = serde_json::from_str::<serde_json::Value>(&body)
                    .unwrap_or(serde_json::Value::Null);
                let op = request
                    .get("op")
                    .and_then(|value| value.as_str())
                    .unwrap_or("");
                if op == "quit" {
                    *control_flow = ControlFlow::Exit;
                } else if op != "stay" {
                    let answer = dispatch_or_dialog(&window, &mut ui, &body, op);
                    let req = request
                        .get("req")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    let mut payload: serde_json::Value =
                        serde_json::from_str(&answer).unwrap_or(serde_json::json!({
                            "ok": false,
                            "error": "bad answer"
                        }));
                    if let Some(object) = payload.as_object_mut() {
                        object.insert("req".into(), req);
                    }
                    let script = format!("window.kovaAnswer({payload})");
                    let _ = webview.evaluate_script(&script);
                }
            }
            Event::WindowEvent {
                event: WindowEvent::CloseRequested,
                ..
            } => {
                let _ = webview.evaluate_script("if (window.kovaClose) window.kovaClose()");
            }
            _ => {}
        }
    });
}

/// App icon: granite field, a xisto frame, and the mark inside it.
///
/// Xisto is the book's border color. The frame is 6 px. One slab of
/// clear space sits between the frame and the mark, so the dock's
/// rounded mask and the running dot do not cover the K.
fn mark_pixels() -> Vec<u8> {
    const SIZE: u32 = 128;
    let mut pixels = vec![0u8; (SIZE * SIZE * 4) as usize];
    let granite = [0x17, 0x19, 0x1c, 0xff];
    let xisto = [0x9a, 0xa1, 0xa6, 0xff];
    let feldspar = [0xec, 0xee, 0xeb, 0xff];
    let oxide = [0xc2, 0x46, 0x1b, 0xff];
    paint(&mut pixels, 0, 0, SIZE, SIZE, granite);
    paint(&mut pixels, 10, 10, 108, 6, xisto);
    paint(&mut pixels, 10, 112, 108, 6, xisto);
    paint(&mut pixels, 10, 10, 6, 108, xisto);
    paint(&mut pixels, 112, 10, 6, 108, xisto);
    paint(&mut pixels, 32, 32, 16, 64, feldspar);
    paint(&mut pixels, 68, 32, 28, 16, feldspar);
    paint(&mut pixels, 52, 56, 28, 16, oxide);
    paint(&mut pixels, 68, 80, 28, 16, feldspar);
    pixels
}

fn window_icon() -> Option<Icon> {
    Icon::from_rgba(mark_pixels(), 128, 128).ok()
}

#[cfg(target_os = "linux")]
fn publish_dock_icon(window: &Window) {
    use gtk::prelude::*;
    use tao::platform::unix::WindowExtUnix;

    let Some(pixbuf) = gtk::gdk_pixbuf::Pixbuf::new(
        gtk::gdk_pixbuf::Colorspace::Rgb,
        true,
        8,
        128,
        128,
    ) else {
        return;
    };
    let pixels = mark_pixels();
    let row_bytes = 128 * 4;
    let stride = pixbuf.rowstride() as usize;
    let dest = unsafe { pixbuf.pixels() };
    for row in 0..128 {
        let from = row * row_bytes;
        let to = row * stride;
        dest[to..to + row_bytes].copy_from_slice(&pixels[from..from + row_bytes]);
    }
    window.gtk_window().set_icon(Some(&pixbuf));
    let Some(home) = std::env::var_os("HOME") else {
        return;
    };
    let home = std::path::PathBuf::from(home);
    let icon_dir = home.join(".local/share/icons");
    let theme = icon_dir.join("hicolor/128x128/apps");
    let apps = home.join(".local/share/applications");
    if std::fs::create_dir_all(&theme).is_err() || std::fs::create_dir_all(&apps).is_err() {
        return;
    }
    let icon_path = icon_dir.join("kova-client.png");
    // The dock resolves the Gtk application id, dev.kova.client, through
    // the icon theme. The file manager uses the other file.
    let theme_icon = theme.join("dev.kova.client.png");
    if pixbuf.savev(&icon_path, "png", &[]).is_err()
        || pixbuf.savev(&theme_icon, "png", &[]).is_err()
    {
        return;
    }
    window.gtk_window().set_icon_name(Some("dev.kova.client"));
    let Ok(exec) = std::env::current_exe() else {
        return;
    };
    let desktop = format!(
        "[Desktop Entry]\nType=Application\nName=Kova\nExec={exec}\nIcon={icon}\nStartupWMClass=kova-client\nTerminal=false\nCategories=Development;\n",
        exec = exec.display(),
        icon = icon_path.display(),
    );
    let _ = std::fs::write(apps.join("kova-client.desktop"), &desktop);
    let _ = std::fs::write(apps.join("dev.kova.client.desktop"), &desktop);
    let _ = std::process::Command::new("update-desktop-database")
        .arg(&apps)
        .status();
    let _ = std::process::Command::new("gtk-update-icon-cache")
        .args(["-f", "-t"])
        .arg(icon_dir.join("hicolor"))
        .status();
    let exec_path = exec.display().to_string();
    let icon_uri = format!("file://{}", icon_path.display());
    let _ = std::process::Command::new("gio")
        .args(["set", &exec_path, "metadata::custom-icon", &icon_uri])
        .status();
}

fn paint(pixels: &mut [u8], x: u32, y: u32, width: u32, height: u32, color: [u8; 4]) {
    const SIZE: u32 = 128;
    for row in y..y.saturating_add(height).min(SIZE) {
        for col in x..x.saturating_add(width).min(SIZE) {
            let index = ((row * SIZE + col) * 4) as usize;
            pixels[index..index + 4].copy_from_slice(&color);
        }
    }
}

#[cfg(target_os = "linux")]
fn dispatch_or_dialog(window: &Window, ui: &mut kova::shell::Ui, body: &str, op: &str) -> String {
    if op == "pickSave" {
        pick_save(window, body)
    } else {
        ui.dispatch(body)
    }
}

#[cfg(not(target_os = "linux"))]
fn dispatch_or_dialog(
    _window: &Window,
    ui: &mut kova::shell::Ui,
    body: &str,
    _op: &str,
) -> String {
    ui.dispatch(body)
}

#[cfg(target_os = "linux")]
fn pick_save(window: &Window, body: &str) -> String {
    use gtk::prelude::*;
    use tao::platform::unix::WindowExtUnix;

    let request: serde_json::Value = serde_json::from_str(body).unwrap_or(serde_json::Value::Null);
    let name = request
        .get("name")
        .and_then(|value| value.as_str())
        .unwrap_or("kova.csv");
    let name = if name.to_ascii_lowercase().ends_with(".csv") {
        name.to_string()
    } else {
        format!("{name}.csv")
    };
    let dialog = gtk::FileChooserNative::new(
        Some("Export CSV"),
        Some(window.gtk_window()),
        gtk::FileChooserAction::Save,
        Some("Save"),
        Some("Cancel"),
    );
    dialog.set_current_name(&name);
    dialog.set_do_overwrite_confirmation(true);
    if let Some(folder) = save_folder() {
        let _ = dialog.set_current_folder(folder);
    }
    let filter = gtk::FileFilter::new();
    filter.set_name(Some("CSV"));
    filter.add_pattern("*.csv");
    dialog.add_filter(filter);
    let chosen = if dialog.run() == gtk::ResponseType::Accept {
        dialog.file().and_then(|file| file.path())
    } else {
        None
    };
    match chosen {
        Some(path) => serde_json::json!({ "ok": true, "path": path.display().to_string() }).to_string(),
        None => serde_json::json!({ "ok": true, "cancelled": true }).to_string(),
    }
}

#[cfg(target_os = "linux")]
fn save_folder() -> Option<std::path::PathBuf> {
    if let Some(path) = std::env::var_os("KOVA_DOWNLOADS") {
        return Some(std::path::PathBuf::from(path));
    }
    let home = std::env::var_os("HOME")?;
    let downloads = std::path::PathBuf::from(home).join("Downloads");
    if downloads.is_dir() {
        Some(downloads)
    } else {
        None
    }
}
