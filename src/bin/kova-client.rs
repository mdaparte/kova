//! Window onto a running Kova. The page is drawn by the system web view.
//! A machine without a display is told to use kova-shell.
//!
//! This binary is built with `--features window`. The server is not.

use std::process::ExitCode;

use tao::dpi::LogicalSize;
use tao::event::{Event, WindowEvent};
use tao::event_loop::{ControlFlow, EventLoopBuilder};
use tao::window::WindowBuilder;
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
    let event_loop = EventLoopBuilder::<UserEvent>::with_user_event().build();
    let proxy = event_loop.create_proxy();
    let window = WindowBuilder::new()
        .with_title("KOVA")
        .with_inner_size(LogicalSize::new(1100.0, 720.0))
        .build(&event_loop)
        .map_err(|err| err.to_string())?;
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
                let answer = ui.dispatch(&body);
                let id = serde_json::from_str::<serde_json::Value>(&body)
                    .ok()
                    .and_then(|value| value.get("id").cloned())
                    .unwrap_or(serde_json::Value::Null);
                let mut payload: serde_json::Value =
                    serde_json::from_str(&answer).unwrap_or(serde_json::json!({
                        "ok": false,
                        "error": "bad answer"
                    }));
                if let Some(object) = payload.as_object_mut() {
                    object.insert("id".into(), id);
                }
                let script = format!("window.kovaAnswer({payload})");
                let _ = webview.evaluate_script(&script);
            }
            Event::WindowEvent {
                event: WindowEvent::CloseRequested,
                ..
            } => *control_flow = ControlFlow::Exit,
            _ => {}
        }
    });
}
