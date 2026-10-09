//! This machine's name as its owner knows it: shown in conflict-copy names and in the owner's
//! list of authorized devices.

/// The computer's name: the name set in the system settings on macOS ("Anna's MacBook"), the
/// computer name on Windows, the host name on Linux. Never empty.
pub fn device_name() -> String {
    os_name()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "this computer".into())
}

#[cfg(target_os = "macos")]
fn os_name() -> Option<String> {
    // The friendly name lives in the System Configuration store, not in the host name.
    std::process::Command::new("/usr/sbin/scutil")
        .args(["--get", "ComputerName"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .filter(|s| !s.trim().is_empty())
        .or_else(hostname)
}

#[cfg(all(unix, not(target_os = "macos")))]
fn os_name() -> Option<String> {
    hostname()
}

#[cfg(unix)]
fn hostname() -> Option<String> {
    let mut buf = [0u8; 256];
    // SAFETY: the buffer is valid for writes of its length.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    if rc != 0 {
        return None;
    }
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8(buf[..end].to_vec()).ok()
}

#[cfg(windows)]
fn os_name() -> Option<String> {
    use windows_sys::Win32::System::SystemInformation::{
        ComputerNameDnsHostname, GetComputerNameExW,
    };
    let mut len = 0u32;
    // SAFETY: a null buffer of length 0 only asks for the length (it fails with
    // ERROR_MORE_DATA and sets `len`, terminator included).
    unsafe { GetComputerNameExW(ComputerNameDnsHostname, std::ptr::null_mut(), &mut len) };
    let mut buf = vec![0u16; len as usize];
    // SAFETY: the buffer holds `len` characters; on success `len` is the name's length.
    let ok = unsafe { GetComputerNameExW(ComputerNameDnsHostname, buf.as_mut_ptr(), &mut len) };
    if ok == 0 {
        return None;
    }
    String::from_utf16(&buf[..len as usize]).ok()
}

#[cfg(test)]
mod tests {
    #[test]
    fn this_machine_has_a_name() {
        assert!(!super::device_name().trim().is_empty());
        assert!(super::os_name().is_some());
    }
}
