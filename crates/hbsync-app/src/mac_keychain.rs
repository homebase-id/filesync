//! The macOS keychain entry of a login, readable by all three of this app's programs: the tray,
//! which saves it at a login, the daemon (`hbsyncd`), which reads it at every start, and the
//! command line (`hbsync`). The login keychain lets the program that made an entry read it and
//! asks the user for every other one: the daemon, at every logon. So the entry is made again
//! with an access list naming the three ([`share`]): after each save, and by the tray as it
//! starts for one an earlier version saved. An access list is set as an entry is made: changing
//! one in place asks for the keychain's password, even of the program that made the entry.
//! Made again, the entry is gone for a moment: a daemon reading it just then finds none (once
//! per entry, at the first start after an update to this version).
//!
//! The access list is the older keychain's (`SecAccess`, deprecated but working): the newer
//! data protection keychain shares entries by an access group, which takes an entitlement and
//! a provisioning profile.

use std::ffi::{CString, c_char, c_void};
use std::path::PathBuf;

use core_foundation::array::{CFArray, CFArrayRef};
use core_foundation::base::{CFType, CFTypeRef, OSStatus, TCFType};
use core_foundation::data::CFData;
use core_foundation::dictionary::CFMutableDictionary;
use core_foundation::string::{CFString, CFStringRef};
use security_framework::base::Error;
use security_framework::item::{ItemClass, ItemSearchOptions, Reference, SearchResult};
use security_framework::os::macos::access::SecAccess;
use security_framework::os::macos::keychain::{SecKeychain, SecPreferencesDomain};
use security_framework::os::macos::keychain_item::SecKeychainItem;
use security_framework_sys::base::{
    SecAccessRef, SecKeychainItemRef, errSecItemNotFound, errSecSuccess,
};
use security_framework_sys::item::{
    kSecAttrAccount, kSecAttrService, kSecClass, kSecClassGenericPassword, kSecUseKeychain,
    kSecValueData,
};
use security_framework_sys::keychain_item::{SecItemAdd, SecKeychainItemDelete};

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
    static kSecAttrAccess: CFStringRef;
    fn SecKeychainItemCopyAccess(item: SecKeychainItemRef, access: *mut SecAccessRef) -> OSStatus;
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

/// The keychain the keyring store keeps the entries in: the user's default.
fn keychain() -> Result<SecKeychain, Error> {
    SecKeychain::default_for_domain(SecPreferencesDomain::User)
}

/// The entry of `service` and `account` in `keychain`, `None` without one. Its attributes
/// only: the secret isn't read.
fn entry(
    keychain: &SecKeychain,
    service: &str,
    account: &str,
) -> Result<Option<SecKeychainItem>, Error> {
    let found = ItemSearchOptions::new()
        .keychains(std::slice::from_ref(keychain))
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

/// An access list letting `programs` read an entry without asking (each by its code signature,
/// so an update of the app keeps it; one that can't be named is left out), described as
/// [`SHARED`] if they are `all` and each is on it; and whether it is.
fn access(programs: &[PathBuf], all: bool) -> Result<(SecAccess, bool), Error> {
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
    Ok((unsafe { SecAccess::wrap_under_create_rule(access) }, shared))
}

/// Adds the entry of `service` and `account` to `keychain`, holding `secret`, with `access`.
fn add(
    keychain: &SecKeychain,
    service: &str,
    account: &str,
    secret: &[u8],
    access: &SecAccess,
) -> Result<(), Error> {
    // SAFETY: the framework's constant keys and values, valid for the program's life.
    let key = |k: CFStringRef| unsafe { CFString::wrap_under_get_rule(k) }.as_CFType();
    let mut item = CFMutableDictionary::<CFType, CFType>::new();
    item.add(
        &key(unsafe { kSecClass }),
        &key(unsafe { kSecClassGenericPassword }),
    );
    item.add(
        &key(unsafe { kSecAttrService }),
        &CFString::new(service).as_CFType(),
    );
    item.add(
        &key(unsafe { kSecAttrAccount }),
        &CFString::new(account).as_CFType(),
    );
    item.add(
        &key(unsafe { kSecValueData }),
        &CFData::from_buffer(secret).as_CFType(),
    );
    item.add(&key(unsafe { kSecUseKeychain }), &keychain.as_CFType());
    item.add(&key(unsafe { kSecAttrAccess }), &access.as_CFType());
    // SAFETY: a valid dictionary; no result asked for.
    check(unsafe { SecItemAdd(item.as_concrete_TypeRef(), std::ptr::null_mut()) })
}

/// Why an entry wasn't shared, and its secret if it is gone from the keychain: made again, it
/// couldn't be added, nor put back as it was (to be kept elsewhere, or the login is lost).
#[derive(Debug)]
pub(crate) struct Failed {
    pub(crate) error: Error,
    pub(crate) lost: Option<Vec<u8>>,
}

impl From<Error> for Failed {
    fn from(error: Error) -> Self {
        Failed { error, lost: None }
    }
}

/// Lets this app's programs read the entry of `service` and `account` without asking, if its
/// access list doesn't already: the entry is made again with one ([`access`]). Never asks: the
/// keychain's prompts are off while it runs (for the whole process: called where no other
/// thread uses the keychain), and a program that may not read and delete the entry without
/// asking leaves it (`errSecInteractionNotAllowed`). An entry that can't be added again is put
/// back as it was. Returns whether it was made again naming all three. Costs one keychain query
/// when the entry is shared already.
pub(crate) fn share(service: &str, account: &str) -> Result<bool, Failed> {
    let _quiet = SecKeychain::disable_user_interaction()?;
    let keychain = keychain()?;
    let Some(item) = entry(&keychain, service, account)? else {
        return Ok(false);
    };
    if readers(&item)?.iter().any(|(d, _)| d == SHARED) {
        return Ok(false);
    }
    let (programs, all) = programs();
    let (access, shared) = access(&programs, all)?;
    let (secret, item) = keychain.find_generic_password(service, account)?;
    let secret = secret.to_vec();
    // SAFETY: a valid item, deleted from its own keychain only.
    check(unsafe { SecKeychainItemDelete(item.as_concrete_TypeRef()) })?;
    match add(&keychain, service, account, &secret, &access) {
        Ok(()) => Ok(shared),
        Err(error) => {
            let lost = keychain
                .set_generic_password(service, account, &secret)
                .is_err()
                .then_some(secret);
            Err(Failed { error, lost })
        }
    }
}

#[cfg(test)]
mod tests {
    use security_framework::passwords;

    use super::*;

    /// Run on CI's Mac (its login keychain). An entry made as the keyring store makes it (this
    /// program's alone) is made again, without asking, with an access list letting the programs
    /// of this install read its secret (this test's program, in CI), and no other: macOS asks
    /// about a program only when it isn't on it (one signed by another team, Apple's own tools
    /// say, is asked about anyway: its partition differs). The secret stays; an entry shared
    /// already isn't made again.
    #[test]
    fn an_entry_is_made_again_shared() -> Result<(), Box<dyn std::error::Error>> {
        if std::env::var_os("CI").is_none() {
            return Ok(());
        }
        let service = "Homebase Filesync test";
        let account = format!("acl-{}.example", std::process::id());
        let keychain = keychain()?;
        keychain.set_generic_password(service, &account, b"from before")?;
        let item = entry(&keychain, service, &account)?.ok_or("no entry")?;
        assert!(readers(&item)?.iter().all(|(d, _)| d != SHARED));

        let (programs, all) = programs();
        let shared = share(service, &account).map_err(|f| f.error)?;
        assert_eq!(shared, all);
        let item = entry(&keychain, service, &account)?.ok_or("no entry")?;
        let description = if all { SHARED } else { "Homebase Filesync" };
        assert_eq!(
            readers(&item)?,
            [(description.to_owned(), Some(programs.len()))]
        );
        assert_eq!(
            passwords::get_generic_password(service, &account)?,
            b"from before"
        );
        if all {
            assert!(!share(service, &account).map_err(|f| f.error)?, "shared");
        }

        passwords::delete_generic_password(service, &account)?;
        assert!(entry(&keychain, service, &account)?.is_none());
        assert!(!share(service, &account).map_err(|f| f.error)?, "none");
        Ok(())
    }
}
