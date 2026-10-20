//! The synced folder as a Cloud Files sync root (Windows), which puts it at the top of
//! Explorer's navigation pane, as OneDrive's and Dropbox's are. Only the registration: no
//! placeholders, so the files stay ordinary files and nothing here touches them. It works
//! unpackaged too (tried on Windows 11, 2026-10-13); the MSIX declares cloud files for it
//! (`packaging/conveyor.conf`), without which Windows won't list a packaged app's sync roots.

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

const PROVIDER: &str = "HomebaseFilesync";
/// The name in the navigation pane.
const NAME: &str = "Homebase";

fn io(e: windows::core::Error) -> std::io::Error {
    std::io::Error::other(e.message())
}

/// Makes `folder` (a plain path: no `\\?\`) the one sync root of `identity` there, shown with
/// `icon` (an ICO file). Ours that are stale go: this one's id at another folder or with another
/// icon, or another of ours at this folder (an earlier identity). True when it registered, false
/// when it already was.
pub(crate) fn register(folder: &Path, identity: &str, icon: &Path) -> std::io::Result<bool> {
    if !StorageProviderSyncRootManager::IsSupported().map_err(io)? {
        return Err(std::io::Error::other(
            "Windows doesn't support Cloud Files here",
        ));
    }
    let prefix = prefix()?;
    let id = id(&prefix, folder, identity);
    let key = key(folder);
    let shown = icon_resource(icon).to_lowercase();
    let mut present = false;
    for (other, path, other_icon) in ours(&prefix)? {
        let here = path.is_some_and(|p| p.to_lowercase() == key);
        if other == id && here && other_icon.is_some_and(|i| i.to_lowercase() == shown) {
            present = true;
        } else if other == id || here {
            unregister_id(&other)?;
        }
    }
    if !present {
        root_info(folder, &id, icon)
            .and_then(|info| StorageProviderSyncRootManager::Register(&info))
            .map_err(io)?;
    }
    Ok(!present)
}

fn root_info(
    folder: &Path,
    id: &str,
    icon: &Path,
) -> windows::core::Result<StorageProviderSyncRootInfo> {
    let info = StorageProviderSyncRootInfo::new()?;
    info.SetId(&HSTRING::from(id))?;
    info.SetPath(
        &StorageFolder::GetFolderFromPathAsync(&HSTRING::from(folder.as_os_str()))?.join()?,
    )?;
    info.SetDisplayNameResource(&HSTRING::from(NAME))?;
    info.SetIconResource(&HSTRING::from(icon_resource(icon)))?;
    // The files are always all there: Windows never asks us for content.
    info.SetHydrationPolicy(StorageProviderHydrationPolicy::Full)?;
    info.SetHydrationPolicyModifier(StorageProviderHydrationPolicyModifier::None)?;
    info.SetPopulationPolicy(StorageProviderPopulationPolicy::AlwaysFull)?;
    info.SetInSyncPolicy(StorageProviderInSyncPolicy::Default)?;
    info.SetHardlinkPolicy(StorageProviderHardlinkPolicy::None)?;
    info.SetShowSiblingsAsGroup(false)?;
    info.SetVersion(&HSTRING::from("1"))?;
    let context = DataWriter::new()?;
    context.WriteBytes(PROVIDER.as_bytes())?;
    info.SetContext(&context.DetachBuffer()?)?;
    Ok(info)
}

/// Unregisters the sync root of `identity` at `folder`, found by its id: it goes even when the
/// folder was moved or deleted.
pub(crate) fn unregister(folder: &Path, identity: &str) -> std::io::Result<()> {
    let prefix = prefix()?;
    let id = id(&prefix, folder, identity);
    if ours(&prefix)?.iter().any(|(other, ..)| *other == id) {
        unregister_id(&id)?;
    }
    Ok(())
}

fn unregister_id(id: &str) -> std::io::Result<()> {
    StorageProviderSyncRootManager::Unregister(&HSTRING::from(id)).map_err(io)
}

/// The icon as Windows names it: the file and the icon's index in it.
fn icon_resource(icon: &Path) -> String {
    format!("{},0", icon.display())
}

/// Our sync roots of this user, as (id, folder, icon); `None` for a folder that is gone.
#[allow(clippy::type_complexity)]
fn ours(prefix: &str) -> std::io::Result<Vec<(String, Option<String>, Option<String>)>> {
    let mut found = Vec::new();
    for root in StorageProviderSyncRootManager::GetCurrentSyncRoots().map_err(io)? {
        let id = root.Id().map_err(io)?.to_string();
        if id.starts_with(prefix) {
            let path = root
                .Path()
                .and_then(|p| p.cast::<IStorageItem>())
                .and_then(|p| p.Path())
                .ok()
                .map(|p| p.to_string());
            let icon = root.IconResource().ok().map(|i| i.to_string());
            found.push((id, path, icon));
        }
    }
    Ok(found)
}

/// `HomebaseFilesync!<user SID>!`: an id is this and an account, the form Windows asks for.
fn prefix() -> std::io::Result<String> {
    Ok(format!("{PROVIDER}!{}!", user_sid()?))
}

/// The account is the identity and a hash of the folder, so each folder of each identity (and
/// each private instance's) has its own root.
fn id(prefix: &str, folder: &Path, identity: &str) -> String {
    let hash = hbsync_fileio::fnv1a64(key(folder).as_bytes());
    format!("{prefix}{identity}~{hash:016x}")
}

/// Windows paths compare without regard to case.
fn key(folder: &Path) -> String {
    folder.display().to_string().to_lowercase()
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
