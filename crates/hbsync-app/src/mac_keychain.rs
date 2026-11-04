//! The macOS keychain entry of a login, readable by all three of this app's programs: the tray,
//! which saves it at a login, the daemon (`hbsyncd`), which reads it at every start, and the
//! command line (`hbsync`). The login keychain lets the program that made an entry read it and
//! asks the user for every other one: the daemon, at every logon. So the entry's access list is
//! set to name the three ([`share`]): after each save, and by the tray as it starts for one an
//! earlier version saved. The list is changed in place: the secret is never read, rewritten or
//! deleted for it, and a program that may not change it (one that didn't make the entry) leaves
//! it as it is, without asking.
//!
//! The access list is the older keychain's (`SecAccess`, deprecated but working): the newer
//! data protection keychain shares entries by an access group, which takes an entitlement and
//! a provisioning profile.

use std::ffi::{CString, c_char, c_void};
use std::path::PathBuf;

use core_foundation::array::{CFArray, CFArrayRef};
use core_foundation::base::{CFType, CFTypeRef, OSStatus, TCFType};
use core_foundation::string::{CFString, CFStringRef};
use security_framework::base::Error;
use security_framework::item::{ItemClass, ItemSearchOptions, Reference, SearchResult};
use security_framework::os::macos::access::SecAccess;
use security_framework::os::macos::keychain::{SecKeychain, SecPreferencesDomain};
use security_framework::os::macos::keychain_item::SecKeychainItem;
use security_framework_sys::base::{
    SecAccessRef, SecKeychainItemRef, errSecItemNotFound, errSecSuccess,
};

/// The description of an access list naming all three programs: one without it was set before
/// (by the keychain, for the program that made the entry), or without one of them (not found).
const SHARED: &str = "Homebase Filesync: the app, hbsyncd and hbsync";

/// The programs that may read the entry, as this install has them ([`crate::install`]).
const PROGRAMS: [&str; 3] = [crate::autostart::TRAY, crate::lock::DAEMON, "hbsync"];

#[allow(non_camel_case_types)]
type SecTrustedApplicationRef = *mut c_void;

#[link(name = "Security", kind = "framework")]
unsafe extern "C" {
    static kSecACLAuthorizationDecrypt: CFStringRef;
    fn SecTrustedApplicationCreateFromPath(
        path: *const c_char,
        app: *mut SecTrustedApplicationRef,
    ) -> OSStatus;
    fn SecAccessCreate(
        descriptor: CFStringRef,
        trusted: CFArrayRef,
        access: *mut SecAccessRef,
    ) -> OSStatus;
    fn SecKeychainItemCopyAccess(item: SecKeychainItemRef, access: *mut SecAccessRef) -> OSStatus;
    fn SecKeychainItemSetAccess(item: SecKeychainItemRef, access: SecAccessRef) -> OSStatus;
    fn SecAccessCopyACLList(access: SecAccessRef, list: *mut CFArrayRef) -> OSStatus;
    fn SecACLCopyAuthorizations(acl: CFTypeRef) -> CFArrayRef;
    fn SecACLCopyContents(
        acl: CFTypeRef,
        apps: *mut CFArrayRef,
        description: *mut CFStringRef,
        prompt: *mut u16,
    ) -> OSStatus;
}

fn check(status: OSStatus) -> Result<(), Error> {
    if status == errSecSuccess {
        Ok(())
    } else {
        Err(Error::from_code(status))
    }
}

/// The entry of `service` and `account` in the keychain the keyring store keeps it in (the
/// user's default), `None` without one. Its attributes only: the secret isn't read.
fn entry(service: &str, account: &str) -> Result<Option<SecKeychainItem>, Error> {
    let keychain = SecKeychain::default_for_domain(SecPreferencesDomain::User)?;
    let found = ItemSearchOptions::new()
        .keychains(&[keychain])
        .class(ItemClass::generic_password())
        .service(service)
        .account(account)
        .load_refs(true)
        .search();
    match found {
        Err(e) if e.code() == errSecItemNotFound => Ok(None),
        Err(e) => Err(e),
        Ok(found) => Ok(found.into_iter().find_map(|r| match r {
            SearchResult::Ref(Reference::KeychainItem(item)) => Some(item),
            _ => None,
        })),
    }
}

/// The entries of `item`'s access list that let its secret be read: each one's description and
/// how many programs it names (`None`: any program). Read without asking.
fn readers(item: &SecKeychainItem) -> Result<Vec<(String, Option<usize>)>, Error> {
    let mut access: SecAccessRef = std::ptr::null_mut();
    // SAFETY: a valid item; each out pointer receives a reference we own (copy or create rule),
    // wrapped to be released.
    check(unsafe { SecKeychainItemCopyAccess(item.as_concrete_TypeRef(), &mut access) })?;
    let access = unsafe { SecAccess::wrap_under_create_rule(access) };
    let mut list: CFArrayRef = std::ptr::null();
    check(unsafe { SecAccessCopyACLList(access.as_concrete_TypeRef(), &mut list) })?;
    let list: CFArray<CFType> = unsafe { CFArray::wrap_under_create_rule(list) };
    // SAFETY: a constant CFString of the framework's.
    let decrypt = unsafe { CFString::wrap_under_get_rule(kSecACLAuthorizationDecrypt) }.as_CFType();
    let mut readers = Vec::new();
    for acl in list.iter() {
        let rights: CFArray<CFType> = unsafe {
            CFArray::wrap_under_create_rule(SecACLCopyAuthorizations(acl.as_CFTypeRef()))
        };
        if !rights.iter().any(|r| *r == decrypt) {
            continue;
        }
        let (mut apps, mut description, mut prompt) = (std::ptr::null(), std::ptr::null(), 0u16);
        check(unsafe {
            SecACLCopyContents(acl.as_CFTypeRef(), &mut apps, &mut description, &mut prompt)
        })?;
        let description = (!description.is_null())
            .then(|| unsafe { CFString::wrap_under_create_rule(description) }.to_string());
        let apps = (!apps.is_null())
            .then(|| unsafe { CFArray::<CFType>::wrap_under_create_rule(apps) }.len() as usize);
        readers.push((description.unwrap_or_default(), apps));
    }
    Ok(readers)
}

/// The programs of this install, and this one; whether all three were found.
fn programs() -> (Vec<PathBuf>, bool) {
    let mut found: Vec<PathBuf> = PROGRAMS
        .iter()
        .filter_map(|name| crate::autostart::program_of(name).ok())
        .filter(|p| p.exists())
        .collect();
    let all = found.len() == PROGRAMS.len();
    if let Ok(me) = crate::install::current_exe() {
        found.push(me.to_path_buf());
    }
    found.sort();
    found.dedup();
    (found, all)
}

/// Sets `item`'s access list to let `programs` read it without asking (each by its code
/// signature, so an update of the app keeps it; one that can't be named is left out),
/// described as [`SHARED`] if they are `all` and each is on it. Returns whether it is.
fn share_with(item: &SecKeychainItem, programs: &[PathBuf], all: bool) -> Result<bool, Error> {
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
    let shared = all && trusted.len() == programs.len();
    let trusted = CFArray::from_CFTypes(&trusted);
    let description = CFString::new(if shared { SHARED } else { "Homebase Filesync" });
    let mut access: SecAccessRef = std::ptr::null_mut();
    // SAFETY: valid CF objects; `access` receives a reference we own (create rule).
    check(unsafe {
        SecAccessCreate(
            description.as_concrete_TypeRef(),
            trusted.as_concrete_TypeRef(),
            &mut access,
        )
    })?;
    let access = unsafe { SecAccess::wrap_under_create_rule(access) };
    // SAFETY: a valid item and access list.
    check(unsafe {
        SecKeychainItemSetAccess(item.as_concrete_TypeRef(), access.as_concrete_TypeRef())
    })?;
    Ok(shared)
}

/// Lets this app's programs read the entry of `service` and `account` without asking, if its
/// access list doesn't already: the list is set in place. Never asks: the keychain's prompts
/// are off while it runs (for the whole process: called where no other thread uses the
/// keychain), and a program that may not change the list without asking leaves it
/// (`errSecInteractionNotAllowed`). Returns whether the list was set naming all three.
/// Costs one keychain query when the entry is shared already.
pub(crate) fn share(service: &str, account: &str) -> Result<bool, Error> {
    let _quiet = SecKeychain::disable_user_interaction()?;
    let Some(item) = entry(service, account)? else {
        return Ok(false);
    };
    if readers(&item)?.iter().any(|(d, _)| d == SHARED) {
        return Ok(false);
    }
    let (programs, all) = programs();
    share_with(&item, &programs, all)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use security_framework::passwords;

    use super::*;

    /// Run on CI's Mac (its login keychain). An entry made as the keyring store makes it (this
    /// program's alone) gets an access list letting the programs given read its secret, and no
    /// other: macOS asks about a program only when it isn't on it (one signed by another team,
    /// Apple's own tools say, is asked about anyway: its partition differs). The secret is
    /// untouched, and an entry shared already isn't changed again.
    #[test]
    fn the_entry_is_shared_with_the_programs_in_place() -> Result<(), Box<dyn std::error::Error>> {
        if std::env::var_os("CI").is_none() {
            return Ok(());
        }
        let service = "Homebase Filesync test";
        let account = format!("acl-{}.example", std::process::id());
        let keychain = SecKeychain::default_for_domain(SecPreferencesDomain::User)?;
        keychain.set_generic_password(service, &account, b"from before")?;
        let item = entry(service, &account)?.ok_or("no entry")?;
        assert!(readers(&item)?.iter().all(|(d, _)| d != SHARED));

        let me = crate::install::current_exe()?.to_path_buf();
        let programs = [me, Path::new("/bin/ls").to_path_buf()];
        assert!(share_with(&item, &programs, true)?);
        let item = entry(service, &account)?.ok_or("no entry")?;
        assert_eq!(readers(&item)?, [(SHARED.to_owned(), Some(2))]);
        assert_eq!(
            passwords::get_generic_password(service, &account)?,
            b"from before"
        );
        assert!(!share(service, &account)?, "shared already");

        passwords::delete_generic_password(service, &account)?;
        assert!(entry(service, &account)?.is_none());
        assert!(!share(service, &account)?, "nothing to share");
        Ok(())
    }
}
