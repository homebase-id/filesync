//! The macOS keychain entry of a login, readable by all three of this app's programs: the tray,
//! which saves it at a login, the daemon (`hbsyncd`), which reads it at every start, and the
//! command line (`hbsync`). The login keychain lets the program that made an entry read it and
//! asks the user for every other one: the daemon, at every logon. So the entry is made with an
//! access list naming the three ([`save`]); one made before, by one program alone, is made again
//! so by the tray as it starts ([`share`]), without asking: one the tray can't read without
//! asking is left as it is.
//!
//! The access list is the older keychain's (`SecAccess`, deprecated but working): the newer
//! data protection keychain shares entries by an access group, which takes an entitlement and
//! a provisioning profile.

use std::ffi::{CString, c_char, c_void};
use std::path::PathBuf;

use core_foundation::array::CFArray;
use core_foundation::base::{CFType, CFTypeRef, OSStatus, TCFType};
use core_foundation::boolean::CFBoolean;
use core_foundation::data::CFData;
use core_foundation::dictionary::{CFDictionary, CFMutableDictionary};
use core_foundation::string::{CFString, CFStringRef};
use core_foundation_sys::array::CFArrayRef;
use security_framework::base::Error;
use security_framework::os::macos::keychain::SecKeychain;
use security_framework_sys::base::{SecAccessRef, errSecItemNotFound, errSecSuccess};
use security_framework_sys::item::{
    kSecAttrAccount, kSecAttrComment, kSecAttrService, kSecClass, kSecClassGenericPassword,
    kSecReturnAttributes, kSecReturnData, kSecValueData,
};
use security_framework_sys::keychain_item::{SecItemAdd, SecItemCopyMatching, SecItemDelete};

/// Written on an entry made with the access list: one without it was made before.
const SHARED: &str = "Homebase Filesync: the app, hbsyncd and hbsync";

/// The programs that may read the entry, as this install has them ([`crate::install`]).
const PROGRAMS: [&str; 3] = [crate::autostart::TRAY, crate::lock::DAEMON, "hbsync"];

#[allow(non_camel_case_types)]
type SecTrustedApplicationRef = *mut c_void;

#[link(name = "Security", kind = "framework")]
unsafe extern "C" {
    static kSecAttrAccess: CFStringRef;
    fn SecTrustedApplicationCreateFromPath(
        path: *const c_char,
        app: *mut SecTrustedApplicationRef,
    ) -> OSStatus;
    fn SecAccessCreate(
        descriptor: CFStringRef,
        trusted: CFArrayRef,
        access: *mut SecAccessRef,
    ) -> OSStatus;
}

fn check(status: OSStatus) -> Result<(), Error> {
    if status == errSecSuccess {
        Ok(())
    } else {
        Err(Error::from_code(status))
    }
}

/// The entry's query: the generic password of `service` and `account`.
fn query(service: &str, account: &str) -> CFMutableDictionary<CFType, CFType> {
    let mut query = CFMutableDictionary::new();
    query.add(
        &key(unsafe { kSecClass }),
        &key(unsafe { kSecClassGenericPassword }),
    );
    query.add(
        &key(unsafe { kSecAttrService }),
        &CFString::new(service).as_CFType(),
    );
    query.add(
        &key(unsafe { kSecAttrAccount }),
        &CFString::new(account).as_CFType(),
    );
    query
}

/// One of the framework's constant keys (or values), valid for the program's life.
fn key(k: CFStringRef) -> CFType {
    // SAFETY: a constant CFString of the framework's.
    unsafe { CFString::wrap_under_get_rule(k) }.as_CFType()
}

/// The programs of this install that exist, then this one.
fn programs() -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = PROGRAMS
        .iter()
        .filter_map(|name| crate::autostart::program_of(name).ok())
        .filter(|p| p.exists())
        .collect();
    if let Ok(me) = crate::install::current_exe() {
        found.push(me.to_path_buf());
    }
    found.sort();
    found.dedup();
    found
}

/// An access list letting `programs` read the entry without asking (each by its code signature,
/// so an update of the app keeps it); a path that can't be made one is left out.
fn access(label: &str, programs: &[PathBuf]) -> Result<CFType, Error> {
    let mut trusted = Vec::new();
    for path in programs {
        let Ok(c) = CString::new(path.as_os_str().as_encoded_bytes()) else {
            continue;
        };
        let mut app: SecTrustedApplicationRef = std::ptr::null_mut();
        // SAFETY: a NUL-terminated path; `app` receives a reference we own (create rule).
        match check(unsafe { SecTrustedApplicationCreateFromPath(c.as_ptr(), &mut app) }) {
            Ok(()) if !app.is_null() => {
                trusted.push(unsafe { CFType::wrap_under_create_rule(app as CFTypeRef) })
            }
            Ok(()) => {}
            Err(e) => tracing::info!("keychain: {} can't be trusted: {e}", path.display()),
        }
    }
    let trusted = CFArray::from_CFTypes(&trusted);
    let label = CFString::new(label);
    let mut access: SecAccessRef = std::ptr::null_mut();
    // SAFETY: valid CF objects; `access` receives a reference we own (create rule).
    check(unsafe {
        SecAccessCreate(
            label.as_concrete_TypeRef(),
            trusted.as_concrete_TypeRef(),
            &mut access,
        )
    })?;
    Ok(unsafe { CFType::wrap_under_create_rule(access as CFTypeRef) })
}

/// Saves `secret` as the entry of `service` and `account`, readable by this app's programs
/// without asking: the entry before, if any, is deleted first (an entry's access list is set
/// as it is made).
pub(crate) fn save(service: &str, account: &str, secret: &[u8]) -> Result<(), Error> {
    save_for(service, account, secret, &programs())
}

fn save_for(
    service: &str,
    account: &str,
    secret: &[u8],
    programs: &[PathBuf],
) -> Result<(), Error> {
    delete(service, account)?;
    let mut item = query(service, account);
    item.add(
        &key(unsafe { kSecValueData }),
        &CFData::from_buffer(secret).as_CFType(),
    );
    item.add(
        &key(unsafe { kSecAttrComment }),
        &CFString::new(SHARED).as_CFType(),
    );
    item.add(&key(unsafe { kSecAttrAccess }), &access(service, programs)?);
    // SAFETY: a valid dictionary; no result asked for.
    check(unsafe { SecItemAdd(item.as_concrete_TypeRef(), std::ptr::null_mut()) })
}

/// Deletes the entry; none there is fine.
fn delete(service: &str, account: &str) -> Result<(), Error> {
    let query = query(service, account);
    // SAFETY: a valid dictionary.
    match unsafe { SecItemDelete(query.as_concrete_TypeRef()) } {
        s if s == errSecItemNotFound => Ok(()),
        s => check(s),
    }
}

/// The entry's comment, `None` without the entry (attributes only: read without asking).
fn comment(service: &str, account: &str) -> Result<Option<String>, Error> {
    let mut query = query(service, account);
    query.add(
        &key(unsafe { kSecReturnAttributes }),
        &CFBoolean::true_value().as_CFType(),
    );
    let mut found: CFTypeRef = std::ptr::null();
    // SAFETY: a valid dictionary; `found` receives a reference we own (copy rule).
    match unsafe { SecItemCopyMatching(query.as_concrete_TypeRef(), &mut found) } {
        s if s == errSecItemNotFound => return Ok(None),
        s => check(s)?,
    }
    let attributes: CFDictionary<CFType, CFType> =
        unsafe { CFDictionary::wrap_under_create_rule(found as _) };
    Ok(Some(
        attributes
            .find(key(unsafe { kSecAttrComment }))
            .and_then(|c| c.downcast::<CFString>())
            .map(|c| c.to_string())
            .unwrap_or_default(),
    ))
}

/// The entry's secret.
fn read(service: &str, account: &str) -> Result<Vec<u8>, Error> {
    let mut query = query(service, account);
    query.add(
        &key(unsafe { kSecReturnData }),
        &CFBoolean::true_value().as_CFType(),
    );
    let mut found: CFTypeRef = std::ptr::null();
    // SAFETY: a valid dictionary; `found` receives a reference we own (copy rule).
    check(unsafe { SecItemCopyMatching(query.as_concrete_TypeRef(), &mut found) })?;
    let data: CFData = unsafe { CFData::wrap_under_create_rule(found as _) };
    Ok(data.bytes().to_vec())
}

/// Makes an entry saved before (by one program alone) again so that this app's programs read
/// it without asking. Never asks: an entry this program can't read and delete so is left as it
/// is (`errSecInteractionNotAllowed`). Returns whether it was made again.
pub(crate) fn share(service: &str, account: &str) -> Result<bool, Error> {
    match comment(service, account)? {
        Some(c) if c != SHARED => {}
        _ => return Ok(false),
    }
    let _quiet = SecKeychain::disable_user_interaction()?;
    let secret = read(service, account)?;
    save(service, account, &secret)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[link(name = "Security", kind = "framework")]
    unsafe extern "C" {
        static kSecReturnRef: CFStringRef;
        fn SecKeychainItemCopyAccess(item: CFTypeRef, access: *mut SecAccessRef) -> OSStatus;
        fn SecAccessCopyACLList(access: SecAccessRef, list: *mut CFArrayRef) -> OSStatus;
        fn SecACLCopyContents(
            acl: CFTypeRef,
            apps: *mut CFArrayRef,
            description: *mut CFStringRef,
            prompt: *mut u16,
        ) -> OSStatus;
    }

    /// How many programs each of the entry's access list's entries names (`None`: any program),
    /// read without asking.
    fn trusted_counts(service: &str, account: &str) -> Result<Vec<Option<usize>>, Error> {
        let mut query = query(service, account);
        query.add(
            &key(unsafe { kSecReturnRef }),
            &CFBoolean::true_value().as_CFType(),
        );
        let mut item: CFTypeRef = std::ptr::null();
        // SAFETY: valid CF objects; each out pointer receives a reference we own (copy rule),
        // wrapped to be released.
        check(unsafe { SecItemCopyMatching(query.as_concrete_TypeRef(), &mut item) })?;
        let item = unsafe { CFType::wrap_under_create_rule(item) };
        let mut access: SecAccessRef = std::ptr::null_mut();
        check(unsafe { SecKeychainItemCopyAccess(item.as_CFTypeRef(), &mut access) })?;
        let access = unsafe { CFType::wrap_under_create_rule(access as CFTypeRef) };
        let mut list: CFArrayRef = std::ptr::null();
        check(unsafe { SecAccessCopyACLList(access.as_CFTypeRef() as SecAccessRef, &mut list) })?;
        let list: CFArray<CFType> = unsafe { CFArray::wrap_under_create_rule(list) };
        let mut counts = Vec::new();
        for acl in list.iter() {
            let (mut apps, mut description, mut prompt) =
                (std::ptr::null(), std::ptr::null(), 0u16);
            check(unsafe {
                SecACLCopyContents(acl.as_CFTypeRef(), &mut apps, &mut description, &mut prompt)
            })?;
            if !description.is_null() {
                drop(unsafe { CFString::wrap_under_create_rule(description) });
            }
            counts.push((!apps.is_null()).then(|| {
                unsafe { CFArray::<CFType>::wrap_under_create_rule(apps) }.len() as usize
            }));
        }
        Ok(counts)
    }

    /// Run on CI's Mac (its login keychain): an entry is saved with an access list naming the
    /// programs given (each program is asked about by macOS only when it isn't on it; one
    /// signed by another team, Apple's own tools say, is asked about anyway: its partition
    /// differs); saved again it is replaced, still shared, and isn't shared again.
    #[test]
    fn the_entry_names_the_programs_that_read_it() -> Result<(), Box<dyn std::error::Error>> {
        if std::env::var_os("CI").is_none() {
            return Ok(());
        }
        let service = "Homebase Filesync test";
        let account = format!("acl-{}.example", std::process::id());
        let me = crate::install::current_exe()?.to_path_buf();
        let programs = [me, Path::new("/bin/ls").to_path_buf()];

        save_for(service, &account, b"secret one", &programs)?;
        let counts = trusted_counts(service, &account)?;
        assert!(counts.contains(&Some(2)), "{counts:?}");
        assert!(!counts.contains(&None), "never any program: {counts:?}");
        assert_eq!(comment(service, &account)?.as_deref(), Some(SHARED));
        assert_eq!(read(service, &account)?, b"secret one");
        // Saved again: replaced, still shared.
        save_for(service, &account, b"secret two", &programs)?;
        assert_eq!(read(service, &account)?, b"secret two");
        assert!(!share(service, &account)?, "shared already");

        delete(service, &account)?;
        assert_eq!(comment(service, &account)?, None);
        assert!(!share(service, &account)?, "nothing to share");
        Ok(())
    }

    /// An entry made the way earlier versions made it (the keyring store's, this program's
    /// alone) is made again shared, by the program that made it, without asking.
    #[test]
    fn an_earlier_entry_is_shared() -> Result<(), Box<dyn std::error::Error>> {
        if std::env::var_os("CI").is_none() {
            return Ok(());
        }
        let service = "Homebase Filesync test";
        let account = format!("earlier-{}.example", std::process::id());
        SecKeychain::default()?.set_generic_password(service, &account, b"from before")?;
        assert_eq!(comment(service, &account)?.as_deref(), Some(""));
        assert!(share(service, &account)?);
        assert_eq!(comment(service, &account)?.as_deref(), Some(SHARED));
        assert_eq!(read(service, &account)?, b"from before");
        delete(service, &account)?;
        Ok(())
    }
}
