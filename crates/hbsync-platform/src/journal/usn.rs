//! Windows NTFS/ReFS USN change journal.
//!
//! Position format: `usn:<journal id hex>:<next usn>`. Each record names the folder it happened
//! in (`ParentFileReferenceNumber`), which becomes a [`Touched::DirId`]. A rename produces
//! records in both the old and the new folder.
//!
//! The journal is read with `FSCTL_READ_UNPRIVILEGED_USN_JOURNAL`, which doesn't need
//! administrator rights (Windows 10 1709+). The volume is opened read-only first and with no
//! access rights as a fallback. Not yet verified as a non-admin: CI runners are administrators.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::AsRawHandle;
use std::path::Path;

use windows_sys::Win32::Foundation::GENERIC_READ;
use windows_sys::Win32::Storage::FileSystem::{
    FILE_ATTRIBUTE_DIRECTORY, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    GetVolumePathNameW,
};
use windows_sys::Win32::System::IO::DeviceIoControl;
use windows_sys::Win32::System::Ioctl::{
    FSCTL_QUERY_USN_JOURNAL, FSCTL_READ_UNPRIVILEGED_USN_JOURNAL, READ_USN_JOURNAL_DATA_V1,
    USN_JOURNAL_DATA_V0,
};

use super::{ChangeJournal, JournalDelta, JournalError, Touched};
use crate::FileId;

// winerror.h
const ERROR_INVALID_FUNCTION: i32 = 1;
const ERROR_JOURNAL_DELETE_IN_PROGRESS: i32 = 1178;
const ERROR_JOURNAL_NOT_ACTIVE: i32 = 1179;
const ERROR_JOURNAL_ENTRY_DELETED: i32 = 1181;

#[derive(Debug)]
pub struct UsnJournal {
    volume: File,
    /// `VolumeSerialNumber` as in [`FileId::volume`].
    volume_serial: u64,
}

impl UsnJournal {
    pub fn open(root: &Path) -> io::Result<Self> {
        let volume_serial = crate::stat(root)?.file_id.volume;
        let device = volume_device(root)?;
        let open = |access: u32| {
            OpenOptions::new()
                .access_mode(access)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
                .open(&device)
        };
        let volume = open(GENERIC_READ).or_else(|_| open(0))?;
        let journal = UsnJournal {
            volume,
            volume_serial,
        };
        journal.query().map_err(|e| match e {
            JournalError::Io(e) => e,
            JournalError::Unusable(msg) => io::Error::other(msg),
        })?;
        Ok(journal)
    }

    fn query(&self) -> Result<USN_JOURNAL_DATA_V0, JournalError> {
        let mut data = USN_JOURNAL_DATA_V0::default();
        let mut returned = 0u32;
        // SAFETY: out buffer is a POD struct with its size; no overlapped I/O.
        let ok = unsafe {
            DeviceIoControl(
                self.volume.as_raw_handle(),
                FSCTL_QUERY_USN_JOURNAL,
                std::ptr::null(),
                0,
                (&raw mut data).cast(),
                size_of::<USN_JOURNAL_DATA_V0>() as u32,
                &mut returned,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(classify(io::Error::last_os_error()));
        }
        Ok(data)
    }
}

impl ChangeJournal for UsnJournal {
    fn now(&mut self) -> Result<String, JournalError> {
        let data = self.query()?;
        Ok(format!("usn:{:x}:{}", data.UsnJournalID, data.NextUsn))
    }

    fn since(&mut self, cursor: &str) -> Result<JournalDelta, JournalError> {
        let (journal_id, mut start) = parse(cursor)?;
        let data = self.query()?;
        if data.UsnJournalID != journal_id {
            return Err(JournalError::Unusable("USN journal was recreated".into()));
        }
        if start < data.LowestValidUsn {
            return Err(JournalError::Unusable("USN journal wrapped".into()));
        }
        // Read up to the end as of now; later records belong to the next catch-up.
        let end = data.NextUsn;
        let mut touched = Vec::new();
        let mut buf = vec![0u64; 64 * 1024 / 8];
        while start < end {
            let request = READ_USN_JOURNAL_DATA_V1 {
                StartUsn: start,
                ReasonMask: u32::MAX,
                ReturnOnlyOnClose: 0,
                Timeout: 0,
                BytesToWaitFor: 0,
                UsnJournalID: journal_id,
                MinMajorVersion: 2,
                MaxMajorVersion: 3,
            };
            let mut returned = 0u32;
            // SAFETY: in/out buffers with their sizes; no overlapped I/O.
            let ok = unsafe {
                DeviceIoControl(
                    self.volume.as_raw_handle(),
                    FSCTL_READ_UNPRIVILEGED_USN_JOURNAL,
                    (&raw const request).cast(),
                    size_of::<READ_USN_JOURNAL_DATA_V1>() as u32,
                    buf.as_mut_ptr().cast(),
                    (buf.len() * 8) as u32,
                    &mut returned,
                    std::ptr::null_mut(),
                )
            };
            if ok == 0 {
                return Err(classify(io::Error::last_os_error()));
            }
            // SAFETY: the buffer holds `returned` initialized bytes.
            let bytes =
                unsafe { std::slice::from_raw_parts(buf.as_ptr().cast::<u8>(), returned as usize) };
            let next = i64::from_le_bytes(field(bytes, 0)?);
            parse_records(&bytes[8..], self.volume_serial, &mut touched)?;
            if next <= start {
                break;
            }
            start = next;
        }
        touched.dedup();
        Ok(JournalDelta {
            touched,
            rescan_all: false,
            cursor: format!("usn:{journal_id:x}:{start}"),
        })
    }
}

fn parse(cursor: &str) -> Result<(u64, i64), JournalError> {
    let bad = || JournalError::Unusable(format!("not a USN position: {cursor:?}"));
    let rest = cursor.strip_prefix("usn:").ok_or_else(bad)?;
    let (id, usn) = rest.split_once(':').ok_or_else(bad)?;
    Ok((
        u64::from_str_radix(id, 16).map_err(|_| bad())?,
        usn.parse().map_err(|_| bad())?,
    ))
}

fn field<const N: usize>(bytes: &[u8], at: usize) -> Result<[u8; N], JournalError> {
    bytes
        .get(at..at + N)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| JournalError::Unusable("truncated USN record".into()))
}

/// Parses `USN_RECORD_V2` / `V3` records into the folders they happened in.
fn parse_records(
    mut bytes: &[u8],
    volume: u64,
    out: &mut Vec<Touched>,
) -> Result<(), JournalError> {
    while bytes.len() >= 8 {
        let len = u32::from_le_bytes(field(bytes, 0)?) as usize;
        if len < 8 || len > bytes.len() {
            break;
        }
        let record = &bytes[..len];
        let major = u16::from_le_bytes(field(record, 4)?);
        let (parent, attrs, file) = match major {
            2 => (
                u128::from(u64::from_le_bytes(field(record, 16)?)),
                u32::from_le_bytes(field(record, 52)?),
                u128::from(u64::from_le_bytes(field(record, 8)?)),
            ),
            3 => (
                u128::from_le_bytes(field(record, 24)?),
                u32::from_le_bytes(field(record, 68)?),
                u128::from_le_bytes(field(record, 8)?),
            ),
            _ => {
                return Err(JournalError::Unusable(format!(
                    "USN record version {major}"
                )));
            }
        };
        out.push(Touched::DirId(FileId {
            volume,
            file: parent,
        }));
        if attrs & FILE_ATTRIBUTE_DIRECTORY != 0 {
            // A folder's own record (created, renamed): rescan it too, in case it's the root of
            // something we only know by id.
            out.push(Touched::DirId(FileId { volume, file }));
        }
        bytes = &bytes[len..];
    }
    Ok(())
}

fn classify(err: io::Error) -> JournalError {
    match err.raw_os_error() {
        Some(
            ERROR_JOURNAL_ENTRY_DELETED
            | ERROR_JOURNAL_NOT_ACTIVE
            | ERROR_JOURNAL_DELETE_IN_PROGRESS
            | ERROR_INVALID_FUNCTION,
        ) => JournalError::Unusable(err.to_string()),
        _ => JournalError::Io(err),
    }
}

/// `\\.\C:` for a path on drive C (or the volume GUID path for mounted folders).
fn volume_device(path: &Path) -> io::Result<String> {
    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut buf = [0u16; 1024];
    // SAFETY: NUL-terminated input, output buffer with its length.
    if unsafe { GetVolumePathNameW(wide.as_ptr(), buf.as_mut_ptr(), buf.len() as u32) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let len = buf.iter().position(|&c| c == 0).unwrap_or(buf.len());
    let mount = String::from_utf16_lossy(&buf[..len]);
    let mount = mount.trim_end_matches('\\');
    Ok(match mount.strip_prefix(r"\\?\") {
        Some(guid_path) => format!(r"\\.\{guid_path}"),
        None => format!(r"\\.\{mount}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn records_name_the_changed_folders() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        let mut journal = match UsnJournal::open(root) {
            Ok(j) => j,
            // A volume without a journal (FAT, some VMs): nothing to test.
            Err(e) => {
                eprintln!("no USN journal: {e}");
                return Ok(());
            }
        };
        fs::create_dir(root.join("sub"))?;
        let cursor = journal.now()?;
        fs::write(root.join("sub").join("new.txt"), b"x")?;
        let delta = journal.since(&cursor)?;
        let sub_id = crate::stat(&root.join("sub"))?.file_id;
        assert!(
            delta.touched.contains(&Touched::DirId(sub_id)),
            "{:?}",
            delta.touched
        );
        assert_ne!(delta.cursor, cursor);
        // Nothing new since the returned cursor touches "sub".
        let again = journal.since(&delta.cursor)?;
        assert!(!again.touched.contains(&Touched::DirId(sub_id)));
        Ok(())
    }

    #[test]
    fn foreign_journal_id_is_unusable() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let Ok(mut journal) = UsnJournal::open(dir.path()) else {
            return Ok(());
        };
        let err = journal.since("usn:1:0").expect_err("different journal id");
        assert!(matches!(err, JournalError::Unusable(_)));
        Ok(())
    }
}
