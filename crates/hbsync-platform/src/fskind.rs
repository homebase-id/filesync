//! What kind of file system a sync root lives on, and what that implies.

use std::io;
use std::path::Path;

/// The file system families the engine treats differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FsFamily {
    Ntfs,
    ReFs,
    Apfs,
    HfsPlus,
    Ext4,
    Btrfs,
    Xfs,
    Zfs,
    Tmpfs,
    /// FAT12/16/32 (vfat, msdos).
    Fat,
    ExFat,
    /// SMB/CIFS, NFS and other network file systems.
    Network,
    Fuse,
    Other,
}

impl FsFamily {
    /// `true` if file ids are stable across renames and reboots, so they can be used for local
    /// move detection.
    pub fn has_stable_file_ids(self) -> bool {
        !matches!(
            self,
            FsFamily::Fat | FsFamily::ExFat | FsFamily::Network | FsFamily::Fuse | FsFamily::Other
        )
    }

    /// `true` if a deleted file's id can be handed to a new, unrelated file soon after
    /// (Unix inode reuse). NTFS ids carry a sequence number and APFS ids are never reused, so
    /// there an id match alone proves identity; elsewhere a move also needs content evidence.
    pub fn reuses_file_ids(self) -> bool {
        !matches!(self, FsFamily::Ntfs | FsFamily::ReFs | FsFamily::Apfs)
    }

    /// `true` if an OS change journal (USN / FSEvents) can be trusted on this file system.
    pub fn supports_journal(self) -> bool {
        matches!(
            self,
            FsFamily::Ntfs | FsFamily::ReFs | FsFamily::Apfs | FsFamily::HfsPlus
        )
    }
}

/// Detects the file system family of the volume holding `path`.
pub fn detect(path: &Path) -> io::Result<FsFamily> {
    imp::detect(path)
}

/// Probes whether names in `dir` are case-sensitive, by creating a temporary file.
///
/// Per-directory on Windows (case sensitivity can be enabled per folder), per-volume elsewhere.
pub fn probe_case_sensitive(dir: &Path) -> io::Result<bool> {
    let probe = tempfile::Builder::new()
        .prefix(".hbsync-case-probe-")
        .tempfile_in(dir)?;
    let name = probe
        .path()
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| io::Error::other("probe name is not UTF-8"))?;
    let flipped = dir.join(name.to_uppercase());
    match std::fs::symlink_metadata(&flipped) {
        Ok(_) => Ok(false),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(true),
        Err(e) => Err(e),
    }
}

#[cfg(target_os = "linux")]
mod imp {
    use super::*;
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    pub fn detect(path: &Path) -> io::Result<FsFamily> {
        let c_path = CString::new(path.as_os_str().as_bytes())?;
        // SAFETY: zeroed POD out struct, valid path.
        let mut st: libc::statfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statfs(c_path.as_ptr(), &mut st) } != 0 {
            return Err(io::Error::last_os_error());
        }
        #[allow(clippy::unnecessary_cast)] // f_type is i64 on x86_64, u32 on some targets
        Ok(from_magic(st.f_type as u64))
    }

    pub fn from_magic(magic: u64) -> FsFamily {
        match magic {
            0xEF53 => FsFamily::Ext4,
            0x9123_683E => FsFamily::Btrfs,
            0x5846_5342 => FsFamily::Xfs,
            0x2FC1_2FC1 => FsFamily::Zfs,
            0x0102_1994 => FsFamily::Tmpfs,
            0x4D44 => FsFamily::Fat,
            0x2011_BAB0 => FsFamily::ExFat,
            0x6969 | 0xFF53_4D42 | 0xFE53_4D42 | 0x517B => FsFamily::Network,
            0x6573_5546 => FsFamily::Fuse,
            0x7366_746E | 0x5346_544E => FsFamily::Ntfs,
            _ => FsFamily::Other,
        }
    }
}

#[cfg(target_os = "macos")]
mod imp {
    use super::*;
    use std::ffi::{CStr, CString};
    use std::os::unix::ffi::OsStrExt;

    pub fn detect(path: &Path) -> io::Result<FsFamily> {
        let c_path = CString::new(path.as_os_str().as_bytes())?;
        // SAFETY: zeroed POD out struct, valid path; f_fstypename is NUL-terminated.
        let mut st: libc::statfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statfs(c_path.as_ptr(), &mut st) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let name = unsafe { CStr::from_ptr(st.f_fstypename.as_ptr()) };
        Ok(from_name(&name.to_string_lossy()))
    }

    pub fn from_name(name: &str) -> FsFamily {
        match name {
            "apfs" => FsFamily::Apfs,
            "hfs" => FsFamily::HfsPlus,
            "msdos" => FsFamily::Fat,
            "exfat" => FsFamily::ExFat,
            "smbfs" | "nfs" | "afpfs" | "webdav" => FsFamily::Network,
            "macfuse" | "osxfuse" | "fuse" => FsFamily::Fuse,
            "ntfs" => FsFamily::Ntfs,
            _ => FsFamily::Other,
        }
    }
}

#[cfg(windows)]
mod imp {
    use super::*;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::GetVolumeInformationByHandleW;

    pub fn detect(path: &Path) -> io::Result<FsFamily> {
        let file = crate::meta::open_attributes(path)?;
        let mut name = [0u16; 64];
        // SAFETY: the buffer length is passed; other out pointers are optional (null).
        let ok = unsafe {
            GetVolumeInformationByHandleW(
                file.as_raw_handle(),
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                name.as_mut_ptr(),
                name.len() as u32,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        let len = name.iter().position(|&c| c == 0).unwrap_or(name.len());
        let fs_name = String::from_utf16_lossy(&name[..len]);
        let family = from_name(&fs_name);
        // A mapped network drive reports the server's file system name (often "NTFS").
        if is_remote(path) {
            Ok(FsFamily::Network)
        } else {
            Ok(family)
        }
    }

    pub fn from_name(name: &str) -> FsFamily {
        match name.to_ascii_uppercase().as_str() {
            "NTFS" => FsFamily::Ntfs,
            "REFS" => FsFamily::ReFs,
            "FAT" | "FAT32" => FsFamily::Fat,
            "EXFAT" => FsFamily::ExFat,
            _ => FsFamily::Other,
        }
    }

    fn is_remote(path: &Path) -> bool {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{GetDriveTypeW, GetVolumePathNameW};
        use windows_sys::Win32::System::WindowsProgramming::DRIVE_REMOTE;
        let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        let mut root = [0u16; 1024];
        // SAFETY: NUL-terminated input, output buffer with its length.
        let ok = unsafe { GetVolumePathNameW(wide.as_ptr(), root.as_mut_ptr(), root.len() as u32) };
        if ok == 0 {
            return false;
        }
        // SAFETY: GetVolumePathNameW NUL-terminates `root`.
        unsafe { GetDriveTypeW(root.as_ptr()) == DRIVE_REMOTE }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn family_rules() {
        assert!(!FsFamily::ExFat.has_stable_file_ids());
        assert!(FsFamily::Ntfs.supports_journal());
        assert!(!FsFamily::Ext4.supports_journal());
    }

    #[test]
    fn detects_temp_dir() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let family = detect(dir.path())?;
        // Every CI runner's temp dir is on a local file system.
        assert_ne!(family, FsFamily::Network);
        Ok(())
    }

    #[test]
    fn case_probe_matches_os_default() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let sensitive = probe_case_sensitive(dir.path())?;
        if cfg!(any(windows, target_os = "macos")) {
            assert!(
                !sensitive,
                "default Windows/macOS volumes are case-insensitive"
            );
        }
        // Linux: tmpfs/ext4 are case-sensitive, but some CI setups differ; only check it runs.
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_magic_numbers() {
        assert_eq!(imp::from_magic(0xEF53), FsFamily::Ext4);
        assert_eq!(imp::from_magic(0x4D44), FsFamily::Fat);
        assert_eq!(imp::from_magic(0xFF53_4D42), FsFamily::Network);
        assert_eq!(imp::from_magic(1), FsFamily::Other);
    }
}
