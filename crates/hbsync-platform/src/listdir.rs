//! Bulk directory listing: names plus the metadata the walker needs, in as few system calls as
//! the OS allows.
//!
//! | OS | Mechanism |
//! |---|---|
//! | Windows | `GetFileInformationByHandleEx(FileIdExtdDirectoryInfo)`: ids and change times in bulk |
//! | macOS | `getattrlistbulk` |
//! | Linux / other | `getdents64` (std `read_dir`) + `fstatat` per entry (std `DirEntry::metadata`) |
//!
//! Every bulk path falls back to [`list_dir_portable`] when the volume doesn't support it, and
//! tests check that both agree.

use std::ffi::OsString;
use std::fs;
use std::io;
use std::path::Path;

use crate::LocalMeta;

/// One directory entry. `.` and `..` are never returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirItem {
    pub name: OsString,
    pub meta: LocalMeta,
}

/// Lists `dir` with the fastest mechanism available.
pub fn list_dir(dir: &Path) -> io::Result<Vec<DirItem>> {
    imp::list_dir(dir)
}

/// Lists `dir` with `read_dir` plus one metadata call per entry. Works everywhere.
pub fn list_dir_portable(dir: &Path) -> io::Result<Vec<DirItem>> {
    let mut items = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let meta = match portable_meta(&entry) {
            Ok(m) => m,
            // Deleted between listing and stat: it's simply gone.
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        items.push(DirItem {
            name: entry.file_name(),
            meta,
        });
    }
    Ok(items)
}

#[cfg(unix)]
fn portable_meta(entry: &fs::DirEntry) -> io::Result<LocalMeta> {
    // DirEntry::metadata is fstatat(dirfd, name, AT_SYMLINK_NOFOLLOW): no path re-resolution.
    Ok(crate::meta::from_metadata(&entry.metadata()?))
}

#[cfg(not(unix))]
fn portable_meta(entry: &fs::DirEntry) -> io::Result<LocalMeta> {
    crate::stat(&entry.path())
}

#[cfg(not(any(windows, target_os = "macos")))]
mod imp {
    pub use super::list_dir_portable as list_dir;
}

#[cfg(windows)]
mod imp {
    use super::*;
    use crate::meta::{filetime_to_unix_ns, open_attributes};
    use crate::{EntryKind, FileId};
    use std::os::windows::ffi::OsStringExt;
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::{ERROR_INVALID_PARAMETER, ERROR_NO_MORE_FILES};
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS,
        FILE_ID_EXTD_DIR_INFO, FILE_ID_INFO, FILE_LIST_DIRECTORY, FILE_READ_ATTRIBUTES,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FileIdExtdDirectoryInfo,
        FileIdExtdDirectoryRestartInfo, FileIdInfo, GetFileInformationByHandleEx,
    };

    const IO_REPARSE_TAG_SYMLINK: u32 = 0xA000_000C;
    const IO_REPARSE_TAG_MOUNT_POINT: u32 = 0xA000_0003;

    pub fn list_dir(dir: &Path) -> io::Result<Vec<DirItem>> {
        match list_bulk(dir) {
            // FAT and some network redirectors don't implement the extended id class.
            Err(e) if e.raw_os_error() == Some(ERROR_INVALID_PARAMETER as i32) => {
                list_dir_portable(dir)
            }
            other => other,
        }
    }

    fn list_bulk(dir: &Path) -> io::Result<Vec<DirItem>> {
        let handle_owner = fs::OpenOptions::new()
            .access_mode(FILE_LIST_DIRECTORY | FILE_READ_ATTRIBUTES)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(crate::winpath::extended(dir))?;
        let handle = handle_owner.as_raw_handle();
        let volume = volume_serial(dir)?;

        // u64 storage keeps the buffer 8-byte aligned, as the structs require.
        let mut buf = vec![0u64; 64 * 1024 / 8];
        let buf_bytes = (buf.len() * 8) as u32;
        let mut items = Vec::new();
        let mut class = FileIdExtdDirectoryRestartInfo;
        loop {
            // SAFETY: buffer pointer and size are valid; the handle stays open.
            let ok = unsafe {
                GetFileInformationByHandleEx(handle, class, buf.as_mut_ptr().cast(), buf_bytes)
            };
            if ok == 0 {
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(ERROR_NO_MORE_FILES as i32) {
                    return Ok(items);
                }
                return Err(err);
            }
            class = FileIdExtdDirectoryInfo;
            let base = buf.as_ptr().cast::<u8>();
            let mut offset = 0usize;
            loop {
                // SAFETY: the kernel filled the buffer with a chain of FILE_ID_EXTD_DIR_INFO
                // records linked by NextEntryOffset, each inside the buffer.
                let info: FILE_ID_EXTD_DIR_INFO =
                    unsafe { std::ptr::read_unaligned(base.add(offset).cast()) };
                let name_ptr = unsafe {
                    base.add(offset + std::mem::offset_of!(FILE_ID_EXTD_DIR_INFO, FileName))
                        .cast::<u16>()
                };
                let name_units = info.FileNameLength as usize / 2;
                let name_slice: Vec<u16> = (0..name_units)
                    .map(|i| unsafe { std::ptr::read_unaligned(name_ptr.add(i)) })
                    .collect();
                if name_slice != [b'.' as u16] && name_slice != [b'.' as u16, b'.' as u16] {
                    items.push(DirItem {
                        name: OsString::from_wide(&name_slice),
                        meta: to_meta(&info, volume),
                    });
                }
                if info.NextEntryOffset == 0 {
                    break;
                }
                offset += info.NextEntryOffset as usize;
            }
        }
    }

    fn to_meta(info: &FILE_ID_EXTD_DIR_INFO, volume: u64) -> LocalMeta {
        let attrs = info.FileAttributes;
        let is_link = attrs & FILE_ATTRIBUTE_REPARSE_POINT != 0
            && matches!(
                info.ReparsePointTag,
                IO_REPARSE_TAG_SYMLINK | IO_REPARSE_TAG_MOUNT_POINT
            );
        let kind = if is_link {
            EntryKind::Symlink
        } else if attrs & FILE_ATTRIBUTE_DIRECTORY != 0 {
            EntryKind::Dir
        } else {
            EntryKind::File
        };
        LocalMeta {
            kind,
            size: if kind == EntryKind::File {
                info.EndOfFile as u64
            } else {
                0
            },
            mtime_ns: filetime_to_unix_ns(info.LastWriteTime),
            ctime_ns: filetime_to_unix_ns(info.ChangeTime),
            file_id: FileId {
                volume,
                file: u128::from_le_bytes(info.FileId.Identifier),
            },
            executable: None,
        }
    }

    fn volume_serial(dir: &Path) -> io::Result<u64> {
        let file = open_attributes(dir)?;
        let mut id: FILE_ID_INFO = unsafe { std::mem::zeroed() };
        // SAFETY: POD out struct with its size.
        let ok = unsafe {
            GetFileInformationByHandleEx(
                file.as_raw_handle(),
                FileIdInfo,
                (&raw mut id).cast(),
                size_of::<FILE_ID_INFO>() as u32,
            )
        };
        if ok == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(id.VolumeSerialNumber)
        }
    }
}

#[cfg(target_os = "macos")]
mod imp {
    use super::*;
    use crate::{EntryKind, FileId};
    use std::ffi::CString;
    use std::os::unix::ffi::{OsStrExt, OsStringExt};

    // <sys/attr.h> values (ABI-stable).
    const ATTR_BIT_MAP_COUNT: u16 = 5;
    const ATTR_CMN_NAME: u32 = 0x0000_0001;
    const ATTR_CMN_DEVID: u32 = 0x0000_0002;
    const ATTR_CMN_OBJTYPE: u32 = 0x0000_0008;
    const ATTR_CMN_MODTIME: u32 = 0x0000_0400;
    const ATTR_CMN_CHGTIME: u32 = 0x0000_0800;
    const ATTR_CMN_ACCESSMASK: u32 = 0x0002_0000;
    const ATTR_CMN_FILEID: u32 = 0x0200_0000;
    const ATTR_CMN_ERROR: u32 = 0x2000_0000;
    const ATTR_CMN_RETURNED_ATTRS: u32 = 0x8000_0000;
    const ATTR_FILE_DATALENGTH: u32 = 0x0000_0200;
    const VREG: u32 = 1;
    const VDIR: u32 = 2;
    const VLNK: u32 = 5;

    #[repr(C)]
    struct AttrList {
        bitmapcount: u16,
        reserved: u16,
        commonattr: u32,
        volattr: u32,
        dirattr: u32,
        fileattr: u32,
        forkattr: u32,
    }

    unsafe extern "C" {
        fn getattrlistbulk(
            dirfd: libc::c_int,
            attr_list: *mut AttrList,
            attr_buf: *mut libc::c_void,
            attr_buf_size: libc::size_t,
            options: u64,
        ) -> libc::c_int;
    }

    /// Reads values out of one packed entry, in attribute-bit order.
    struct Cursor<'a> {
        buf: &'a [u8],
        pos: usize,
    }

    impl Cursor<'_> {
        fn u32(&mut self) -> io::Result<u32> {
            let v = self.bytes::<4>()?;
            Ok(u32::from_ne_bytes(v))
        }
        fn i64(&mut self) -> io::Result<i64> {
            Ok(i64::from_ne_bytes(self.bytes::<8>()?))
        }
        fn bytes<const N: usize>(&mut self) -> io::Result<[u8; N]> {
            let out = self
                .buf
                .get(self.pos..self.pos + N)
                .and_then(|s| s.try_into().ok())
                .ok_or_else(|| io::Error::other("getattrlistbulk entry truncated"))?;
            self.pos += N;
            Ok(out)
        }
    }

    pub fn list_dir(dir: &Path) -> io::Result<Vec<DirItem>> {
        let c_dir = CString::new(dir.as_os_str().as_bytes())?;
        // SAFETY: valid path; the fd is closed below.
        let fd = unsafe {
            libc::open(
                c_dir.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let result = list_fd(fd);
        // SAFETY: fd is ours.
        unsafe { libc::close(fd) };
        match result {
            // Volumes without bulk support (some network file systems).
            Err(e) if matches!(e.raw_os_error(), Some(libc::ENOTSUP) | Some(libc::EINVAL)) => {
                list_dir_portable(dir)
            }
            other => other,
        }
    }

    fn list_fd(fd: libc::c_int) -> io::Result<Vec<DirItem>> {
        let mut attrs = AttrList {
            bitmapcount: ATTR_BIT_MAP_COUNT,
            reserved: 0,
            commonattr: ATTR_CMN_RETURNED_ATTRS
                | ATTR_CMN_NAME
                | ATTR_CMN_DEVID
                | ATTR_CMN_OBJTYPE
                | ATTR_CMN_MODTIME
                | ATTR_CMN_CHGTIME
                | ATTR_CMN_ACCESSMASK
                | ATTR_CMN_FILEID
                | ATTR_CMN_ERROR,
            volattr: 0,
            dirattr: 0,
            fileattr: ATTR_FILE_DATALENGTH,
            forkattr: 0,
        };
        let mut buf = vec![0u8; 128 * 1024];
        let mut items = Vec::new();
        loop {
            // SAFETY: valid fd, attr list and buffer with its size.
            let count =
                unsafe { getattrlistbulk(fd, &mut attrs, buf.as_mut_ptr().cast(), buf.len(), 0) };
            if count < 0 {
                return Err(io::Error::last_os_error());
            }
            if count == 0 {
                return Ok(items);
            }
            let mut offset = 0usize;
            for _ in 0..count {
                let len = u32::from_ne_bytes(
                    buf.get(offset..offset + 4)
                        .and_then(|s| s.try_into().ok())
                        .ok_or_else(|| io::Error::other("getattrlistbulk length truncated"))?,
                ) as usize;
                let entry = buf
                    .get(offset..offset + len)
                    .ok_or_else(|| io::Error::other("getattrlistbulk entry out of bounds"))?;
                if let Some(item) = parse_entry(entry)? {
                    items.push(item);
                }
                offset += len;
            }
        }
    }

    fn parse_entry(entry: &[u8]) -> io::Result<Option<DirItem>> {
        let mut c = Cursor { buf: entry, pos: 4 };
        let returned_common = c.u32()?;
        let _vol = c.u32()?;
        let _dir = c.u32()?;
        let returned_file = c.u32()?;
        let _fork = c.u32()?;

        let mut name = None;
        if returned_common & ATTR_CMN_NAME != 0 {
            let ref_pos = c.pos;
            let name_off = c.u32()? as i32;
            let name_len = c.u32()? as usize;
            let start = (ref_pos as isize + name_off as isize) as usize;
            let raw = entry
                .get(start..start + name_len)
                .ok_or_else(|| io::Error::other("getattrlistbulk name out of bounds"))?;
            // name_len includes the NUL terminator.
            let raw = raw.strip_suffix(&[0]).unwrap_or(raw);
            name = Some(OsString::from_vec(raw.to_vec()));
        }
        let dev = if returned_common & ATTR_CMN_DEVID != 0 {
            c.u32()? as i32
        } else {
            0
        };
        let objtype = if returned_common & ATTR_CMN_OBJTYPE != 0 {
            c.u32()?
        } else {
            0
        };
        let timespec = |c: &mut Cursor<'_>| -> io::Result<i64> {
            let secs = c.i64()?;
            let nanos = c.i64()?;
            Ok(secs.saturating_mul(1_000_000_000).saturating_add(nanos))
        };
        let mtime_ns = if returned_common & ATTR_CMN_MODTIME != 0 {
            timespec(&mut c)?
        } else {
            0
        };
        let ctime_ns = if returned_common & ATTR_CMN_CHGTIME != 0 {
            timespec(&mut c)?
        } else {
            0
        };
        let mode = if returned_common & ATTR_CMN_ACCESSMASK != 0 {
            c.u32()?
        } else {
            0
        };
        let file_id = if returned_common & ATTR_CMN_FILEID != 0 {
            c.i64()? as u64
        } else {
            0
        };
        let error = if returned_common & ATTR_CMN_ERROR != 0 {
            c.u32()?
        } else {
            0
        };
        let size = if returned_file & ATTR_FILE_DATALENGTH != 0 {
            c.i64()? as u64
        } else {
            0
        };

        let Some(name) = name else { return Ok(None) };
        if error != 0 {
            return Err(io::Error::from_raw_os_error(error as i32));
        }
        let kind = match objtype {
            VREG => EntryKind::File,
            VDIR => EntryKind::Dir,
            VLNK => EntryKind::Symlink,
            _ => EntryKind::Other,
        };
        Ok(Some(DirItem {
            name,
            meta: LocalMeta {
                kind,
                size: if kind == EntryKind::File { size } else { 0 },
                mtime_ns,
                ctime_ns,
                // Same widening as std's MetadataExt::dev (dev_t is i32 on macOS).
                file_id: FileId {
                    volume: dev as u64,
                    file: u128::from(file_id),
                },
                executable: Some(mode & 0o111 != 0),
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sorted(mut v: Vec<DirItem>) -> Vec<DirItem> {
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    #[test]
    fn bulk_matches_portable_and_stat() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        fs::write(dir.path().join("small.txt"), b"hello")?;
        fs::write(dir.path().join("empty"), b"")?;
        fs::write(dir.path().join("Ünïcödé name.bin"), vec![7u8; 70_000])?;
        fs::create_dir(dir.path().join("sub"))?;
        fs::write(dir.path().join("sub").join("inner"), b"x")?;
        #[cfg(unix)]
        std::os::unix::fs::symlink("small.txt", dir.path().join("link"))?;

        let bulk = sorted(list_dir(dir.path())?);
        let portable = sorted(list_dir_portable(dir.path())?);
        assert_eq!(bulk, portable);
        assert_eq!(bulk.len(), if cfg!(unix) { 5 } else { 4 });
        for item in &bulk {
            let stat = crate::stat(&dir.path().join(&item.name))?;
            assert_eq!(item.meta, stat, "{:?}", item.name);
        }
        Ok(())
    }

    #[test]
    fn large_directory_spans_several_buffers() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        // Long names make each record big, so the listing needs several calls.
        for i in 0..1500 {
            fs::write(dir.path().join(format!("{i:04}-{}", "n".repeat(100))), b"")?;
        }
        let bulk = list_dir(dir.path())?;
        assert_eq!(bulk.len(), 1500);
        Ok(())
    }
}
