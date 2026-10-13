//! An experiment, not part of Filesync: does registering a folder as a Cloud Files sync root
//! (`StorageProviderSyncRootManager`) put it at the top of Explorer's navigation pane, as
//! OneDrive and Dropbox are, and do files turned into in-sync placeholders show the status
//! ticks? It works on a test folder of its own, `%USERPROFILE%\Homebase Test`, never on the
//! real one, and runs unpackaged (no MSIX identity). Started without arguments (a double click)
//! it shows a menu; else `register`, `in-sync`, `syncing`, `status`, `unregister`.

#[cfg(not(windows))]
fn main() {
    eprintln!("This experiment runs on Windows only.");
    std::process::exit(2);
}

#[cfg(windows)]
fn main() {
    win::main();
}

#[cfg(windows)]
mod win {
    use std::fs;
    use std::io::{self, BufRead, Write};
    use std::os::windows::ffi::OsStrExt;
    use std::path::{Path, PathBuf};

    use windows::Storage::Provider::{
        StorageProviderHardlinkPolicy, StorageProviderHydrationPolicy,
        StorageProviderHydrationPolicyModifier, StorageProviderInSyncPolicy,
        StorageProviderPopulationPolicy, StorageProviderSyncRootInfo,
        StorageProviderSyncRootManager,
    };
    use windows::Storage::Streams::DataWriter;
    use windows::Storage::{IStorageItem, StorageFolder};
    use windows::core::{HRESULT, HSTRING, Interface};
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, LocalFree};
    use windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW;
    use windows_sys::Win32::Security::{GetTokenInformation, TOKEN_QUERY, TOKEN_USER, TokenUser};
    use windows_sys::Win32::Storage::CloudFilters::{
        CF_CONVERT_FLAG_MARK_IN_SYNC, CF_IN_SYNC_STATE, CF_IN_SYNC_STATE_IN_SYNC,
        CF_IN_SYNC_STATE_NOT_IN_SYNC, CF_OPEN_FILE_FLAG_WRITE_ACCESS, CF_REVERT_FLAG_NONE,
        CF_SET_IN_SYNC_FLAG_NONE, CfCloseHandle, CfConvertToPlaceholder,
        CfGetWin32HandleFromProtectedHandle, CfOpenFileWithOplock, CfRevertPlaceholder,
        CfSetInSyncState,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    type Error = Box<dyn std::error::Error>;

    const NAME: &str = "Homebase Test";
    const PROVIDER: &str = "HomebaseFilesyncTest";
    const ICON: &[u8] = include_bytes!("../../../crates/hbsync-tray/assets/homebase.ico");
    /// The file `syncing` marks as not in sync.
    const SYNCING: &str = "Notes.txt";
    /// Not a cloud file (`ERROR_NOT_A_CLOUD_FILE`): reverting one that never was a placeholder.
    const NOT_A_CLOUD_FILE: HRESULT = HRESULT(0x8007_0178_u32 as i32);

    pub fn main() {
        let args: Vec<String> = std::env::args().skip(1).collect();
        if let Some(command) = args.first() {
            if let Err(e) = run(command) {
                eprintln!("Failed: {e}");
                std::process::exit(1);
            }
            return;
        }
        println!(
            "Homebase sync root experiment. Test folder: {}",
            show(folder())
        );
        loop {
            println!();
            println!("  1  Register the test folder as a sync root (step 1)");
            println!("  2  Mark everything in it as synced (step 2: green ticks?)");
            println!("  3  Mark {SYNCING} as syncing (step 3: sync arrows?)");
            println!("  4  Show the sync roots Windows knows");
            println!("  5  Unregister and undo everything (keeps the test folder)");
            println!("  q  Quit");
            print!("> ");
            let _ = io::stdout().flush();
            let mut line = String::new();
            if io::stdin().lock().read_line(&mut line).unwrap_or(0) == 0 {
                return;
            }
            let command = match line.trim() {
                "1" => "register",
                "2" => "in-sync",
                "3" => "syncing",
                "4" => "status",
                "5" => "unregister",
                "q" | "Q" => return,
                _ => continue,
            };
            if let Err(e) = run(command) {
                println!("Failed: {e}");
            }
        }
    }

    fn show(path: Result<PathBuf, Error>) -> String {
        path.map(|p| p.display().to_string())
            .unwrap_or_else(|e| e.to_string())
    }

    fn run(command: &str) -> Result<(), Error> {
        match command {
            "register" => register(),
            "in-sync" => mark_all(),
            "syncing" => syncing(),
            "status" => status(),
            "unregister" => unregister(),
            other => Err(format!("unknown command {other:?}").into()),
        }
    }

    fn folder() -> Result<PathBuf, Error> {
        let home = std::env::var_os("USERPROFILE").ok_or("USERPROFILE isn't set")?;
        Ok(PathBuf::from(home).join(NAME))
    }

    fn icon_path() -> Result<PathBuf, Error> {
        let local = std::env::var_os("LOCALAPPDATA").ok_or("LOCALAPPDATA isn't set")?;
        Ok(PathBuf::from(local)
            .join("Homebase Filesync Test")
            .join("homebase.ico"))
    }

    /// `<provider>!<user SID>!<account>`, the form Windows asks for.
    fn id() -> Result<HSTRING, Error> {
        Ok(HSTRING::from(format!("{PROVIDER}!{}!test", user_sid()?)))
    }

    fn register() -> Result<(), Error> {
        println!(
            "Cloud Files supported here: {}",
            StorageProviderSyncRootManager::IsSupported()?
        );
        let dir = folder()?;
        if !dir.exists() {
            fs::create_dir_all(dir.join("Photos"))?;
            fs::write(dir.join(SYNCING), "Some notes.\r\n")?;
            fs::write(dir.join("Report.txt"), "A report.\r\n")?;
            fs::write(dir.join("Photos").join("Holiday.txt"), "Not a photo.\r\n")?;
        }
        let icon = icon_path()?;
        fs::create_dir_all(icon.parent().ok_or("no folder for the icon")?)?;
        fs::write(&icon, ICON)?;

        let info = StorageProviderSyncRootInfo::new()?;
        info.SetId(&id()?)?;
        let folder =
            StorageFolder::GetFolderFromPathAsync(&HSTRING::from(dir.as_os_str()))?.join()?;
        info.SetPath(&folder)?;
        info.SetDisplayNameResource(&HSTRING::from(NAME))?;
        info.SetIconResource(&HSTRING::from(format!("{},0", icon.display())))?;
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
        StorageProviderSyncRootManager::Register(&info)?;

        println!("Registered {}.", dir.display());
        println!("Open a NEW Explorer window: is \"{NAME}\" in the left pane, at the top level?");
        println!("(If not, sign out and in again, or restart Explorer, and look once more.)");
        Ok(())
    }

    /// Turns every file and folder under the test folder into a placeholder marked in sync.
    fn mark_all() -> Result<(), Error> {
        let dir = folder()?;
        let mut done = 0;
        for path in walk(&dir)? {
            let file = CloudFile::open(&path)?;
            let identity = path.strip_prefix(&dir)?.to_string_lossy().into_owned();
            let hr = unsafe {
                CfConvertToPlaceholder(
                    file.raw(),
                    identity.as_ptr().cast(),
                    identity.len() as u32,
                    CF_CONVERT_FLAG_MARK_IN_SYNC,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                )
            };
            match HRESULT(hr).ok() {
                Ok(()) => {}
                // Already a placeholder (a second run): mark it in sync instead.
                Err(_) => set_in_sync(&path, CF_IN_SYNC_STATE_IN_SYNC)?,
            }
            done += 1;
        }
        println!("Marked {done} items as synced.");
        println!("In Explorer: do the items show a green tick in the Status column?");
        Ok(())
    }

    fn syncing() -> Result<(), Error> {
        set_in_sync(&folder()?.join(SYNCING), CF_IN_SYNC_STATE_NOT_IN_SYNC)?;
        println!("Marked {SYNCING} as not in sync.");
        println!("In Explorer: does it show the sync arrows, and the folder in the left pane?");
        println!("Choose 2 to mark it synced again.");
        Ok(())
    }

    fn status() -> Result<(), Error> {
        let ours = id()?;
        for root in StorageProviderSyncRootManager::GetCurrentSyncRoots()? {
            let path = root
                .Path()
                .and_then(|p| p.cast::<IStorageItem>())
                .and_then(|p| p.Path())
                .map(|p| p.to_string())
                .unwrap_or_else(|e| format!("({e})"));
            let mark = if root.Id()? == ours {
                "  <- this test"
            } else {
                ""
            };
            println!(
                "{}  \"{}\"  {path}{mark}",
                root.Id()?,
                root.DisplayNameResource()?
            );
        }
        Ok(())
    }

    fn unregister() -> Result<(), Error> {
        let dir = folder()?;
        if dir.exists() {
            for path in walk(&dir)? {
                let file = CloudFile::open(&path)?;
                let hr = unsafe {
                    CfRevertPlaceholder(file.raw(), CF_REVERT_FLAG_NONE, std::ptr::null_mut())
                };
                if hr != NOT_A_CLOUD_FILE.0 {
                    HRESULT(hr).ok()?;
                }
            }
        }
        StorageProviderSyncRootManager::Unregister(&id()?)?;
        let _ = fs::remove_file(icon_path()?);
        println!(
            "Unregistered. {} stays; delete it when you like.",
            dir.display()
        );
        Ok(())
    }

    fn set_in_sync(path: &Path, state: CF_IN_SYNC_STATE) -> Result<(), Error> {
        let file = CloudFile::open(path)?;
        let hr = unsafe {
            CfSetInSyncState(
                file.raw(),
                state,
                CF_SET_IN_SYNC_FLAG_NONE,
                std::ptr::null_mut(),
            )
        };
        Ok(HRESULT(hr).ok()?)
    }

    /// Everything under `dir`, not `dir` itself.
    fn walk(dir: &Path) -> Result<Vec<PathBuf>, Error> {
        let mut found = Vec::new();
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                found.extend(walk(&path)?);
            }
            found.push(path);
        }
        Ok(found)
    }

    /// A file or folder opened through the Cloud Files API.
    struct CloudFile(HANDLE);

    impl CloudFile {
        fn open(path: &Path) -> Result<Self, Error> {
            let wide: Vec<u16> = path.as_os_str().encode_wide().chain([0]).collect();
            let mut handle: HANDLE = std::ptr::null_mut();
            let hr = unsafe {
                CfOpenFileWithOplock(wide.as_ptr(), CF_OPEN_FILE_FLAG_WRITE_ACCESS, &mut handle)
            };
            HRESULT(hr)
                .ok()
                .map_err(|e| format!("{}: {e}", path.display()))?;
            Ok(CloudFile(handle))
        }

        fn raw(&self) -> HANDLE {
            unsafe { CfGetWin32HandleFromProtectedHandle(self.0) }
        }
    }

    impl Drop for CloudFile {
        fn drop(&mut self) {
            unsafe { CfCloseHandle(self.0) };
        }
    }

    fn user_sid() -> Result<String, Error> {
        unsafe {
            let mut token: HANDLE = std::ptr::null_mut();
            if OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) == 0 {
                return Err(io::Error::last_os_error().into());
            }
            let mut len = 0u32;
            GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &mut len);
            // u64s: TOKEN_USER holds a pointer, so the buffer must be aligned for one.
            let mut buf = vec![0u64; (len as usize).div_ceil(8)];
            let ok = GetTokenInformation(token, TokenUser, buf.as_mut_ptr().cast(), len, &mut len);
            CloseHandle(token);
            if ok == 0 {
                return Err(io::Error::last_os_error().into());
            }
            let user = &*(buf.as_ptr() as *const TOKEN_USER);
            let mut text = std::ptr::null_mut();
            if ConvertSidToStringSidW(user.User.Sid, &mut text) == 0 {
                return Err(io::Error::last_os_error().into());
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
}
