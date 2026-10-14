//! The synced folder as a Cloud Files sync root (Windows), which puts it at the top of
//! Explorer's navigation pane, as OneDrive's and Dropbox's are. Only the registration: no
//! placeholders, so the files stay ordinary files and nothing here touches them. It works
//! unpackaged too (tried on Windows 11, 2026-10-13).

use std::path::Path;

use windows::Storage::Provider::{
    StorageProviderHardlinkPolicy, StorageProviderHydrationPolicy,
    StorageProviderHydrationPolicyModifier, StorageProviderInSyncPolicy,
    StorageProviderPopulationPolicy, StorageProviderSyncRootInfo, StorageProviderSyncRootManager,
};
use windows::Storage::Streams::DataWriter;
use windows::Storage::{IStorageItem, StorageFolder};
use windows::core::{HSTRING, Interface};
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, LocalFree};
use windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW;
use windows_sys::Win32::Security::{GetTokenInformation, TOKEN_QUERY, TOKEN_USER, TokenUser};
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

/// The provider part of our sync roots' ids, `HomebaseFilesync!<user SID>!<account>` (the form
/// Windows asks for); the account is the identity and a hash of the folder, so each folder of
/// each identity (and each private instance's) has its own.
const PROVIDER: &str = "HomebaseFilesync";
/// The name in the navigation pane.
const NAME: &str = "Homebase";

fn io(e: windows::core::Error) -> std::io::Error {
    std::io::Error::other(e)
}

/// Makes `folder` (a plain path: no `\\?\`) the one sync root of `identity` there, shown with
/// `icon` (an ICO file, written by `write_icon` only when it registers). Ours that are stale
/// go: this one's id at another folder, or another of ours at this folder (an earlier
/// identity). True when it registered, false when it already was.
pub fn register(
    folder: &Path,
    identity: &str,
    icon: &Path,
    write_icon: impl FnOnce() -> std::io::Result<()>,
) -> std::io::Result<bool> {
    if !StorageProviderSyncRootManager::IsSupported().map_err(io)? {
        return Err(std::io::Error::other(
            "Windows doesn't support Cloud Files here",
        ));
    }
    let id = id(folder, identity)?;
    let mut present = false;
    for (other, path) in ours()? {
        let here = same_path(&path, folder);
        if other == id && here {
            present = true;
        } else if other == id || here {
            unregister_id(&other)?;
        }
    }
    if present {
        return Ok(false);
    }
    // An icon Explorer holds open can't be written again; the one there does.
    if let Err(e) = write_icon()
        && !icon.exists()
    {
        return Err(e);
    }
    let info = StorageProviderSyncRootInfo::new().map_err(io)?;
    info.SetId(&HSTRING::from(id)).map_err(io)?;
    let path = StorageFolder::GetFolderFromPathAsync(&HSTRING::from(folder.as_os_str()))
        .and_then(|f| f.join())
        .map_err(io)?;
    info.SetPath(&path).map_err(io)?;
    info.SetDisplayNameResource(&HSTRING::from(NAME))
        .map_err(io)?;
    info.SetIconResource(&HSTRING::from(format!("{},0", icon.display())))
        .map_err(io)?;
    // The files are always all there: Windows never asks us for content.
    info.SetHydrationPolicy(StorageProviderHydrationPolicy::Full)
        .map_err(io)?;
    info.SetHydrationPolicyModifier(StorageProviderHydrationPolicyModifier::None)
        .map_err(io)?;
    info.SetPopulationPolicy(StorageProviderPopulationPolicy::AlwaysFull)
        .map_err(io)?;
    info.SetInSyncPolicy(StorageProviderInSyncPolicy::Default)
        .map_err(io)?;
    info.SetHardlinkPolicy(StorageProviderHardlinkPolicy::None)
        .map_err(io)?;
    info.SetShowSiblingsAsGroup(false).map_err(io)?;
    info.SetVersion(&HSTRING::from("1")).map_err(io)?;
    let context = DataWriter::new().map_err(io)?;
    context.WriteBytes(PROVIDER.as_bytes()).map_err(io)?;
    info.SetContext(&context.DetachBuffer().map_err(io)?)
        .map_err(io)?;
    StorageProviderSyncRootManager::Register(&info).map_err(io)?;
    Ok(true)
}

/// Unregisters the sync root of `identity` at `folder`, found by its id: it goes even when the
/// folder was moved or deleted.
pub fn unregister(folder: &Path, identity: &str) -> std::io::Result<()> {
    let id = id(folder, identity)?;
    if ours()?.iter().any(|(other, _)| *other == id) {
        unregister_id(&id)?;
    }
    Ok(())
}

fn unregister_id(id: &str) -> std::io::Result<()> {
    StorageProviderSyncRootManager::Unregister(&HSTRING::from(id)).map_err(io)
}

/// Our sync roots of this user, as (id, folder); a root whose folder is gone has an empty one.
fn ours() -> std::io::Result<Vec<(String, String)>> {
    let prefix = format!("{PROVIDER}!{}!", user_sid()?);
    let mut found = Vec::new();
    for root in StorageProviderSyncRootManager::GetCurrentSyncRoots().map_err(io)? {
        let id = root.Id().map_err(io)?.to_string();
        if id.starts_with(&prefix) {
            let path = root
                .Path()
                .and_then(|p| p.cast::<IStorageItem>())
                .and_then(|p| p.Path())
                .map(|p| p.to_string())
                .unwrap_or_default();
            found.push((id, path));
        }
    }
    Ok(found)
}

/// Windows paths compare without regard to case.
fn same_path(a: &str, b: &Path) -> bool {
    !a.is_empty() && a.to_lowercase() == b.display().to_string().to_lowercase()
}

fn id(folder: &Path, identity: &str) -> std::io::Result<String> {
    Ok(format!(
        "{PROVIDER}!{}!{identity}~{:016x}",
        user_sid()?,
        fnv1a(folder.display().to_string().to_lowercase().as_bytes())
    ))
}

/// FNV-1a, 64 bits: stable across Rust versions, unlike `std`'s hasher (an id must not change
/// with the compiler).
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// This user's SID, `S-1-5-21-…`.
fn user_sid() -> std::io::Result<String> {
    let mut token: HANDLE = std::ptr::null_mut();
    // SAFETY: the current process's pseudo handle; `token` is written on success.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(std::io::Error::last_os_error());
    }
    let mut len = 0u32;
    // SAFETY: asks for the size only (no buffer).
    unsafe { GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &mut len) };
    // u64s: TOKEN_USER holds a pointer, so the buffer must be aligned for one.
    let mut buf = vec![0u64; (len as usize).div_ceil(8)];
    // SAFETY: `buf` holds `len` bytes; the token is closed once, here.
    let ok = unsafe {
        let ok = GetTokenInformation(token, TokenUser, buf.as_mut_ptr().cast(), len, &mut len);
        CloseHandle(token);
        ok
    };
    if ok == 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: filled in by GetTokenInformation as a TOKEN_USER, aligned (above); its SID
    // points into `buf`, which outlives this use. The string Windows allocates is
    // NUL-terminated and freed once, after it was copied.
    unsafe {
        let user = &*(buf.as_ptr() as *const TOKEN_USER);
        let mut text = std::ptr::null_mut();
        if ConvertSidToStringSidW(user.User.Sid, &mut text) == 0 {
            return Err(std::io::Error::last_os_error());
        }
        let sid = widestring::U16CStr::from_ptr_str(text).to_string_lossy();
        LocalFree(text.cast());
        Ok(sid)
    }
}
