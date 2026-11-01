//! ODBC setup for 64-bit Windows.
//!
//! `odbccp32.dll` reads and writes data sources. `ConfigDSN` is what the
//! ODBC Data Source Administrator calls when a user adds or removes one.

#![allow(non_snake_case, clippy::missing_safety_doc)]

use std::collections::BTreeMap;
use std::ffi::{c_void, CString};

use super::{setup_attribute_pairs, ProfileFn};

const ODBC_ADD_DSN: u16 = 1;
const ODBC_CONFIG_DSN: u16 = 2;
const ODBC_REMOVE_DSN: u16 = 3;
const ODBC_ADD_SYS_DSN: u16 = 4;
const ODBC_CONFIG_SYS_DSN: u16 = 5;
const ODBC_REMOVE_SYS_DSN: u16 = 6;
const ODBC_USER_DSN: u16 = 1;
const ODBC_SYSTEM_DSN: u16 = 2;

const WM_NCCREATE: u32 = 0x0081;
const WM_COMMAND: u32 = 0x0111;
const WM_CLOSE: u32 = 0x0010;
const WM_SETFONT: u32 = 0x0030;
const WS_OVERLAPPED: u32 = 0x0000_0000;
const WS_CAPTION: u32 = 0x00C0_0000;
const WS_SYSMENU: u32 = 0x0008_0000;
const WS_VISIBLE: u32 = 0x1000_0000;
const WS_CHILD: u32 = 0x4000_0000;
const WS_BORDER: u32 = 0x0080_0000;
const WS_TABSTOP: u32 = 0x0001_0000;
const ES_AUTOHSCROLL: u32 = 0x0080;
const ES_PASSWORD: u32 = 0x0020;
const BS_DEFPUSHBUTTON: u32 = 0x0001;
const CW_USEDEFAULT: i32 = 0x8000_0000_u32 as i32;
const IDOK: u16 = 1;
const IDCANCEL: u16 = 2;
const GWLP_USERDATA: i32 = -21;

type WriteStringFn =
    unsafe extern "system" fn(*const u16, *const u16, *const u16, *const u16) -> i32;
type WriteDsnFn = unsafe extern "system" fn(*const u16, *const u16) -> i32;
type RemoveDsnFn = unsafe extern "system" fn(*const u16) -> i32;
type SetModeFn = unsafe extern "system" fn(u16) -> i32;
type WndProc = unsafe extern "system" fn(*mut c_void, u32, usize, isize) -> isize;

#[repr(C)]
struct WndClassW {
    style: u32,
    wnd_proc: Option<WndProc>,
    class_extra: i32,
    window_extra: i32,
    instance: *mut c_void,
    icon: *mut c_void,
    cursor: *mut c_void,
    background: *mut c_void,
    menu_name: *const u16,
    class_name: *const u16,
}

#[repr(C)]
struct CreateStructW {
    create_params: *mut c_void,
    instance: *mut c_void,
    menu: *mut c_void,
    parent: *mut c_void,
    cy: i32,
    cx: i32,
    y: i32,
    x: i32,
    style: i32,
    name: *const u16,
    class_name: *const u16,
    ex_style: u32,
}

struct Installer {
    set_mode: SetModeFn,
    write_dsn: WriteDsnFn,
    remove_dsn: RemoveDsnFn,
    write_string: WriteStringFn,
}

struct Form {
    dsn: String,
    server: String,
    port: String,
    database: String,
    user: String,
    password: String,
    edits: [*mut c_void; 6],
    accepted: bool,
}

/// `SQLGetPrivateProfileString` from the Windows driver manager.
pub(super) unsafe fn load_profile_fn() -> Option<ProfileFn> {
    let module = installer_module();
    if module.is_null() {
        return None;
    }
    let symbol = c_str("SQLGetPrivateProfileString")?;
    let found = GetProcAddress(module, symbol.as_ptr());
    if found.is_null() {
        None
    } else {
        Some(std::mem::transmute::<*mut c_void, ProfileFn>(found))
    }
}

#[no_mangle]
pub unsafe extern "system" fn ConfigDSN(
    parent: *mut c_void,
    request: u16,
    driver: *const i8,
    attributes: *const i8,
) -> i32 {
    let pairs = setup_attribute_pairs(&ansi_blob(attributes));
    bool_result(apply_request(parent, request, &ansi_text(driver), &pairs))
}

#[no_mangle]
pub unsafe extern "system" fn ConfigDSNW(
    parent: *mut c_void,
    request: u16,
    driver: *const u16,
    attributes: *const u16,
) -> i32 {
    let pairs = wide_pairs(attributes);
    bool_result(apply_request(parent, request, &wide_text(driver), &pairs))
}

fn bool_result(ok: bool) -> i32 {
    if ok {
        1
    } else {
        0
    }
}

fn apply_request(
    parent: *mut c_void,
    request: u16,
    driver: &str,
    pairs: &[(String, String)],
) -> bool {
    let Some(api) = installer_api() else {
        return false;
    };
    let system = matches!(
        request,
        ODBC_ADD_SYS_DSN | ODBC_CONFIG_SYS_DSN | ODBC_REMOVE_SYS_DSN
    );
    let mode = if system {
        ODBC_SYSTEM_DSN
    } else {
        ODBC_USER_DSN
    };
    unsafe { (api.set_mode)(mode) };
    let _restore = ModeRestore {
        set_mode: api.set_mode,
    };
    let map = pair_map(pairs);
    let removing = matches!(request, ODBC_REMOVE_DSN | ODBC_REMOVE_SYS_DSN);
    if removing {
        let Some(dsn) = map.get("DSN") else {
            return false;
        };
        let name = wide(dsn);
        return write_ok(unsafe { (api.remove_dsn)(name.as_ptr()) });
    }
    let adding = matches!(request, ODBC_ADD_DSN | ODBC_ADD_SYS_DSN);
    if !matches!(
        request,
        ODBC_ADD_DSN | ODBC_CONFIG_DSN | ODBC_ADD_SYS_DSN | ODBC_CONFIG_SYS_DSN
    ) {
        return false;
    }
    let mut form = form_from(&map, adding);
    if adding {
        fill_defaults(&mut form);
    } else {
        fill_from_profile(&mut form);
    }
    let original = form.dsn.clone();
    if !parent.is_null() && !prompt(parent, &mut form) {
        return false;
    }
    if form.dsn.is_empty() {
        return false;
    }
    if adding {
        fill_defaults(&mut form);
    }
    let driver_name = if driver.is_empty() { "Kova" } else { driver };
    if !store_dsn(&api, driver_name, &form) {
        return false;
    }
    if !adding && original != form.dsn {
        let previous = wide(&original);
        unsafe { (api.remove_dsn)(previous.as_ptr()) };
    }
    true
}

fn form_from(map: &BTreeMap<String, String>, adding: bool) -> Form {
    let mut form = Form {
        dsn: mapped(map, "DSN"),
        server: mapped(map, "SERVER"),
        port: mapped(map, "PORT"),
        database: mapped(map, "DATABASE"),
        user: mapped(map, "UID"),
        password: mapped(map, "PWD"),
        edits: [std::ptr::null_mut(); 6],
        accepted: false,
    };
    if form.server.is_empty() {
        form.server = mapped(map, "HOST");
    }
    if form.database.is_empty() {
        form.database = mapped(map, "DB");
    }
    if form.user.is_empty() {
        form.user = mapped(map, "USER");
    }
    if form.password.is_empty() {
        form.password = mapped(map, "PASSWORD");
    }
    if adding && form.dsn.is_empty() {
        form.dsn = "Kova".to_string();
    }
    form
}

fn fill_defaults(form: &mut Form) {
    if form.server.is_empty() {
        form.server = "127.0.0.1".to_string();
    }
    if form.port.is_empty() {
        form.port = "27018".to_string();
    }
    if form.database.is_empty() {
        form.database = "db".to_string();
    }
}

fn fill_from_profile(form: &mut Form) {
    if form.dsn.is_empty() {
        return;
    }
    let Some(read) = super::profile_fn() else {
        return;
    };
    let dsn = form.dsn.clone();
    let mut values = [
        form.server.clone(),
        form.port.clone(),
        form.database.clone(),
        form.user.clone(),
    ];
    let keys = ["SERVER", "PORT", "DATABASE", "UID"];
    for (value, key) in values.iter_mut().zip(keys) {
        if value.is_empty() {
            if let Some(found) = read_profile(read, &dsn, key) {
                *value = found;
            }
        }
    }
    form.server = values[0].clone();
    form.port = values[1].clone();
    form.database = values[2].clone();
    form.user = values[3].clone();
}

fn read_profile(read: ProfileFn, dsn: &str, key: &str) -> Option<String> {
    let section = c_str(dsn)?;
    let key = c_str(key)?;
    let default = c_str("")?;
    let file = c_str("odbc.ini")?;
    let mut buf = [0i8; 512];
    let copied = unsafe {
        read(
            section.as_ptr(),
            key.as_ptr(),
            default.as_ptr(),
            buf.as_mut_ptr(),
            buf.len() as i32,
            file.as_ptr(),
        )
    };
    if copied <= 0 {
        return None;
    }
    let end = (copied as usize).min(buf.len() - 1);
    let bytes: Vec<u8> = buf[..end].iter().map(|byte| *byte as u8).collect();
    let text = String::from_utf8_lossy(&bytes).trim().to_string();
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

fn store_dsn(api: &Installer, driver: &str, form: &Form) -> bool {
    let dsn = wide(&form.dsn);
    let driver = wide(driver);
    if !write_ok(unsafe { (api.write_dsn)(dsn.as_ptr(), driver.as_ptr()) }) {
        return false;
    }
    for (key, value) in [
        ("SERVER", form.server.as_str()),
        ("PORT", form.port.as_str()),
        ("DATABASE", form.database.as_str()),
        ("UID", form.user.as_str()),
        ("PWD", form.password.as_str()),
    ] {
        if key == "PWD" && value.is_empty() {
            continue;
        }
        if !write_profile(api, &form.dsn, key, value) {
            return false;
        }
    }
    true
}

fn write_profile(api: &Installer, dsn: &str, key: &str, value: &str) -> bool {
    let section = wide(dsn);
    let key = wide(key);
    let file = wide("odbc.ini");
    let stored;
    let value_ptr = if value.is_empty() {
        std::ptr::null()
    } else {
        stored = wide(value);
        stored.as_ptr()
    };
    write_ok(unsafe {
        (api.write_string)(section.as_ptr(), key.as_ptr(), value_ptr, file.as_ptr())
    })
}

fn write_ok(code: i32) -> bool {
    code != 0
}

struct ModeRestore {
    set_mode: SetModeFn,
}

impl Drop for ModeRestore {
    fn drop(&mut self) {
        unsafe { (self.set_mode)(0) };
    }
}

fn installer_api() -> Option<Installer> {
    unsafe {
        let module = installer_module();
        if module.is_null() {
            return None;
        }
        Some(Installer {
            set_mode: proc_fn(module, "SQLSetConfigMode")?,
            write_dsn: proc_fn(module, "SQLWriteDSNToIniW")?,
            remove_dsn: proc_fn(module, "SQLRemoveDSNFromIniW")?,
            write_string: proc_fn(module, "SQLWritePrivateProfileStringW")?,
        })
    }
}

fn installer_module() -> *mut c_void {
    static SLOT: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    let handle = *SLOT.get_or_init(|| unsafe {
        let name = c_str("odbccp32.dll").unwrap_or_else(|| CString::new("odbccp32.dll").unwrap());
        LoadLibraryA(name.as_ptr()) as usize
    });
    handle as *mut c_void
}

unsafe fn proc_fn<T>(module: *mut c_void, name: &str) -> Option<T> {
    let symbol = c_str(name)?;
    let found = GetProcAddress(module, symbol.as_ptr());
    if found.is_null() {
        None
    } else {
        Some(std::mem::transmute_copy(&found))
    }
}

fn pair_map(pairs: &[(String, String)]) -> BTreeMap<String, String> {
    pairs.iter().cloned().collect()
}

fn mapped(map: &BTreeMap<String, String>, key: &str) -> String {
    map.get(key).cloned().unwrap_or_default()
}

fn prompt(parent: *mut c_void, form: &mut Form) -> bool {
    unsafe {
        let instance = module_handle();
        let class_name = wide("KovaOdbcSetup");
        let class = WndClassW {
            style: 0,
            wnd_proc: Some(wnd_proc),
            class_extra: 0,
            window_extra: 0,
            instance,
            icon: std::ptr::null_mut(),
            cursor: LoadCursorW(std::ptr::null_mut(), 32512u16 as *const u16),
            background: (5isize + 1) as *mut c_void,
            menu_name: std::ptr::null(),
            class_name: class_name.as_ptr(),
        };
        RegisterClassW(&class);
        let title = wide("Kova");
        let hwnd = CreateWindowExW(
            0,
            class_name.as_ptr(),
            title.as_ptr(),
            WS_OVERLAPPED | WS_CAPTION | WS_SYSMENU | WS_VISIBLE,
            CW_USEDEFAULT,
            CW_USEDEFAULT,
            460,
            340,
            parent,
            std::ptr::null_mut(),
            instance,
            form as *mut Form as *mut c_void,
        );
        if hwnd.is_null() {
            return false;
        }
        if !parent.is_null() {
            EnableWindow(parent, 0);
        }
        let mut message = std::mem::zeroed();
        while GetMessageW(&mut message, std::ptr::null_mut(), 0, 0) > 0 {
            TranslateMessage(&message);
            DispatchMessageW(&message);
        }
        if !parent.is_null() {
            EnableWindow(parent, 1);
            SetForegroundWindow(parent);
        }
        form.accepted
    }
}

unsafe extern "system" fn wnd_proc(
    hwnd: *mut c_void,
    msg: u32,
    wparam: usize,
    lparam: isize,
) -> isize {
    if msg == WM_NCCREATE {
        let created = &*(lparam as *const CreateStructW);
        SetWindowLongPtrW(hwnd, GWLP_USERDATA, created.create_params as isize);
        return DefWindowProcW(hwnd, msg, wparam, lparam);
    }
    let form = GetWindowLongPtrW(hwnd, GWLP_USERDATA) as *mut Form;
    if msg == 0x0001 {
        if !form.is_null() {
            build_controls(hwnd, &mut *form);
        }
        return 0;
    }
    if msg == WM_COMMAND && !form.is_null() {
        let id = (wparam & 0xffff) as u16;
        if id == IDOK {
            read_controls(&mut *form);
            if (&(*form).dsn).is_empty() {
                return 0;
            }
            (*form).accepted = true;
            DestroyWindow(hwnd);
            PostQuitMessage(0);
            return 0;
        }
        if id == IDCANCEL {
            (*form).accepted = false;
            DestroyWindow(hwnd);
            PostQuitMessage(0);
            return 0;
        }
    }
    if msg == WM_CLOSE {
        if !form.is_null() {
            (*form).accepted = false;
        }
        DestroyWindow(hwnd);
        PostQuitMessage(0);
        return 0;
    }
    DefWindowProcW(hwnd, msg, wparam, lparam)
}

unsafe fn build_controls(parent: *mut c_void, form: &mut Form) {
    let instance = module_handle();
    let font = GetStockObject(17);
    let labels = [
        "Data source",
        "Server",
        "Port",
        "Database",
        "User",
        "Password",
    ];
    let values = [
        form.dsn.as_str(),
        form.server.as_str(),
        form.port.as_str(),
        form.database.as_str(),
        form.user.as_str(),
        form.password.as_str(),
    ];
    for (index, label) in labels.iter().enumerate() {
        let y = 16 + (index as i32) * 36;
        let caption = wide(label);
        let static_hwnd = CreateWindowExW(
            0,
            wide("STATIC").as_ptr(),
            caption.as_ptr(),
            WS_CHILD | WS_VISIBLE,
            16,
            y + 4,
            110,
            20,
            parent,
            std::ptr::null_mut(),
            instance,
            std::ptr::null_mut(),
        );
        SendMessageW(static_hwnd, WM_SETFONT, font as usize, 1);
        let mut style = WS_CHILD | WS_VISIBLE | WS_TABSTOP | WS_BORDER | ES_AUTOHSCROLL;
        if index == 5 {
            style |= ES_PASSWORD;
        }
        let text = wide(values[index]);
        let edit = CreateWindowExW(
            0,
            wide("EDIT").as_ptr(),
            text.as_ptr(),
            style,
            136,
            y,
            280,
            24,
            parent,
            (index as isize + 100) as *mut c_void,
            instance,
            std::ptr::null_mut(),
        );
        SendMessageW(edit, WM_SETFONT, font as usize, 1);
        form.edits[index] = edit;
    }
    let ok = CreateWindowExW(
        0,
        wide("BUTTON").as_ptr(),
        wide("OK").as_ptr(),
        WS_CHILD | WS_VISIBLE | WS_TABSTOP | BS_DEFPUSHBUTTON,
        230,
        250,
        88,
        28,
        parent,
        IDOK as isize as *mut c_void,
        instance,
        std::ptr::null_mut(),
    );
    let cancel = CreateWindowExW(
        0,
        wide("BUTTON").as_ptr(),
        wide("Cancel").as_ptr(),
        WS_CHILD | WS_VISIBLE | WS_TABSTOP,
        328,
        250,
        88,
        28,
        parent,
        IDCANCEL as isize as *mut c_void,
        instance,
        std::ptr::null_mut(),
    );
    SendMessageW(ok, WM_SETFONT, font as usize, 1);
    SendMessageW(cancel, WM_SETFONT, font as usize, 1);
}

unsafe fn read_controls(form: &mut Form) {
    let values = [
        &mut form.dsn,
        &mut form.server,
        &mut form.port,
        &mut form.database,
        &mut form.user,
        &mut form.password,
    ];
    for (index, value) in values.into_iter().enumerate() {
        *value = window_text(form.edits[index]);
    }
}

unsafe fn window_text(hwnd: *mut c_void) -> String {
    if hwnd.is_null() {
        return String::new();
    }
    let len = GetWindowTextLengthW(hwnd);
    if len <= 0 {
        return String::new();
    }
    let mut buf = vec![0u16; len as usize + 1];
    let copied = GetWindowTextW(hwnd, buf.as_mut_ptr(), buf.len() as i32);
    if copied <= 0 {
        String::new()
    } else {
        String::from_utf16_lossy(&buf[..copied as usize])
            .trim()
            .to_string()
    }
}

fn module_handle() -> *mut c_void {
    unsafe {
        let mut handle = std::ptr::null_mut();
        if GetModuleHandleExW(6, wnd_proc as usize as *const u16, &mut handle) != 0 {
            return handle;
        }
        GetModuleHandleW(std::ptr::null())
    }
}

fn ansi_blob(ptr: *const i8) -> Vec<u8> {
    if ptr.is_null() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut previous_zero = false;
    let mut index = 0usize;
    while index < 65_536 {
        let byte = unsafe { *ptr.add(index) } as u8;
        out.push(byte);
        if byte == 0 {
            if previous_zero {
                break;
            }
            previous_zero = true;
        } else {
            previous_zero = false;
        }
        index += 1;
    }
    out
}

fn wide_pairs(ptr: *const u16) -> Vec<(String, String)> {
    if ptr.is_null() {
        return Vec::new();
    }
    let mut pairs = Vec::new();
    let mut index = 0usize;
    while index < 65_536 {
        if unsafe { *ptr.add(index) } == 0 {
            break;
        }
        let start = index;
        while index < 65_536 && unsafe { *ptr.add(index) } != 0 {
            index += 1;
        }
        let text = String::from_utf16_lossy(unsafe {
            std::slice::from_raw_parts(ptr.add(start), index - start)
        });
        if let Some((key, value)) = text.split_once('=') {
            pairs.push((key.trim().to_ascii_uppercase(), value.trim().to_string()));
        }
        index += 1;
    }
    pairs
}

fn ansi_text(ptr: *const i8) -> String {
    if ptr.is_null() {
        return String::new();
    }
    unsafe {
        std::ffi::CStr::from_ptr(ptr)
            .to_string_lossy()
            .trim()
            .to_string()
    }
}

fn wide_text(ptr: *const u16) -> String {
    if ptr.is_null() {
        return String::new();
    }
    let mut len = 0usize;
    while len < 4096 && unsafe { *ptr.add(len) } != 0 {
        len += 1;
    }
    String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(ptr, len) })
        .trim()
        .to_string()
}

fn c_str(text: &str) -> Option<CString> {
    CString::new(text).ok()
}

fn wide(text: &str) -> Vec<u16> {
    text.encode_utf16().chain(std::iter::once(0)).collect()
}

#[link(name = "kernel32")]
extern "system" {
    fn LoadLibraryA(name: *const i8) -> *mut c_void;
    fn GetProcAddress(module: *mut c_void, name: *const i8) -> *mut c_void;
    fn GetModuleHandleW(name: *const u16) -> *mut c_void;
    fn GetModuleHandleExW(flags: u32, address: *const u16, out: *mut *mut c_void) -> i32;
}

#[link(name = "user32")]
extern "system" {
    fn RegisterClassW(class: *const WndClassW) -> u16;
    fn CreateWindowExW(
        ex_style: u32,
        class: *const u16,
        window: *const u16,
        style: u32,
        x: i32,
        y: i32,
        width: i32,
        height: i32,
        parent: *mut c_void,
        menu: *mut c_void,
        instance: *mut c_void,
        param: *mut c_void,
    ) -> *mut c_void;
    fn DestroyWindow(hwnd: *mut c_void) -> i32;
    fn DefWindowProcW(hwnd: *mut c_void, msg: u32, wparam: usize, lparam: isize) -> isize;
    fn GetMessageW(message: *mut Msg, hwnd: *mut c_void, min: u32, max: u32) -> i32;
    fn TranslateMessage(message: *const Msg) -> i32;
    fn DispatchMessageW(message: *const Msg) -> isize;
    fn PostQuitMessage(code: i32);
    fn SendMessageW(hwnd: *mut c_void, msg: u32, wparam: usize, lparam: isize) -> isize;
    fn EnableWindow(hwnd: *mut c_void, enable: i32) -> i32;
    fn SetForegroundWindow(hwnd: *mut c_void) -> i32;
    fn LoadCursorW(instance: *mut c_void, cursor: *const u16) -> *mut c_void;
    fn SetWindowLongPtrW(hwnd: *mut c_void, index: i32, value: isize) -> isize;
    fn GetWindowLongPtrW(hwnd: *mut c_void, index: i32) -> isize;
    fn GetWindowTextW(hwnd: *mut c_void, buffer: *mut u16, max: i32) -> i32;
    fn GetWindowTextLengthW(hwnd: *mut c_void) -> i32;
}

#[link(name = "gdi32")]
extern "system" {
    fn GetStockObject(object: i32) -> *mut c_void;
}

#[repr(C)]
struct Msg {
    hwnd: *mut c_void,
    message: u32,
    wparam: usize,
    lparam: isize,
    time: u32,
    x: i32,
    y: i32,
}
