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
use windows::Storage::StorageFolder;
use windows::Storage::Streams::DataWriter;
use windows::core::HSTRING;
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, LocalFree};
use windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW;
use windows_sys::Win32::Security::{GetTokenInformation, TOKEN_QUERY, TOKEN_USER, TokenUser};
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

/// The provider part of our sync roots' ids: `HomebaseFilesync!<user SID>!<identity>`, the
/// form Windows asks for.
const PROVIDER: &str = "HomebaseFilesync";
/// The name in the navigation pane.
const NAME: &str = "Homebase";

fn io(e: windows::core::Error) -> std::io::Error {
    std::io::Error::other(e)
}

/// Registers `folder` as the sync root of `identity`, shown with `icon` (an ICO file). Again
/// is fine: it updates the registration. True when it wasn't registered before.
pub fn register(folder: &Path, identity: &str, icon: &Path) -> std::io::Result<bool> {
    let id = HSTRING::from(format!("{PROVIDER}!{}!{identity}", user_sid()?));
    let new = StorageProviderSyncRootManager::GetSyncRootInformationForId(&id).is_err();
    if new {
        // Ours for another identity (logged in again as someone else) holds the folder.
        unregister(folder)?;
    }
    let info = StorageProviderSyncRootInfo::new().map_err(io)?;
    info.SetId(&id).map_err(io)?;
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
    Ok(new)
}

/// Unregisters our sync root at `folder`, if there is one (another provider's stays).
pub fn unregister(folder: &Path) -> std::io::Result<()> {
    let Ok(path) = StorageFolder::GetFolderFromPathAsync(&HSTRING::from(folder.as_os_str()))
        .and_then(|f| f.join())
    else {
        return Ok(());
    };
    let Ok(info) = StorageProviderSyncRootManager::GetSyncRootInformationForFolder(&path) else {
        return Ok(());
    };
    let id = info.Id().map_err(io)?;
    if id.to_string().starts_with(&format!("{PROVIDER}!")) {
        StorageProviderSyncRootManager::Unregister(&id).map_err(io)?;
    }
    Ok(())
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
        let mut n = 0;
        while *text.add(n) != 0 {
            n += 1;
        }
        let sid = String::from_utf16_lossy(std::slice::from_raw_parts(text, n));
        LocalFree(text.cast());
        Ok(sid)
    }
}
