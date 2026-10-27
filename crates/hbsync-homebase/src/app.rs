//! What the Filesync app is on an identity: its registration, its drive and its circle.
//!
//! Homebase requires an app slug at registration and a drive slug and drive type slug on every
//! drive an app requests (odin-core #1787, verified in `AppRegistrationService.AssignSlugAsync`
//! and the owner app's `RegisterApp.tsx`). Slugs are lowercase letters, digits and inner
//! hyphens, at most 14 characters (`OdinSlug.cs`). A circle has a slug too, unique within the app
//! that owns it (odin-core #1939, PR #1943, v0.1.960).
//!
//! TODO(odin-core): a sync drive should require payload hashes (`RequirePayloadHashes`, the
//! drive flag from #1899) and say it is a file-sync drive. Neither can be asked for at
//! registration today: the drive request (`d`) has no field for either, the owner app creates
//! requested drives without their attributes, and the owner UI can't set the flag or a drive
//! kind afterwards (verified in odin-js `useApp.ts` / `DriveProvider.ts`). Until then the
//! client sends hashes on every upload anyway, and the drive is only recognised by its type.

use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde_json::{Value, json};
use uuid::{Uuid, uuid};

use crate::HomebaseError;
use crate::drive::{HomebaseRemote, TargetDrive};

/// The app's id on every identity. Never change it: it is how an identity recognises the app.
pub(crate) const APP_ID: Uuid = uuid!("03ae1a49-cdae-483f-9da7-fedc0e8f20b9");
pub const APP_NAME: &str = "Homebase Filesync";
pub(crate) const APP_SLUG: &str = "filesync";

/// The identity's files in the browser: chat's web app on the identity, opened on its Filesync
/// mini-app.
pub fn files_online(identity: &str) -> String {
    format!("https://{identity}/apps/chat/#filesync")
}

/// A drive the app asks for at registration.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DriveSpec {
    pub alias: Uuid,
    pub drive_type: Uuid,
    pub name: &'static str,
    pub description: &'static str,
    pub slug: &'static str,
    pub type_slug: &'static str,
    /// Read = 1, Write = 2.
    pub permission: u32,
}

/// The drive the sync folder mirrors.
pub(crate) const SYNC_DRIVE: DriveSpec = DriveSpec {
    alias: uuid!("cfceb937-b868-496b-91cb-3402f18320f8"),
    drive_type: uuid!("5da48122-e946-487a-8b9d-589642f3c91d"),
    name: "Filesync",
    description: "Files synced with your computers by Homebase Filesync",
    slug: "filesync",
    type_slug: "filesync",
    permission: 3,
};

/// A circle the app owns.
#[derive(Debug, Clone, Copy)]
pub struct CircleSpec {
    /// Its address within the app: how a device finds it again.
    pub slug: &'static str,
    name: &'static str,
    description: &'static str,
    emoji: &'static str,
    /// What members get on the sync drive (Read = 1).
    drive_permission: u32,
}

/// The app's circle: people the files can be shared with, Read on the sync drive, empty at first.
/// Declared at registration ([`circle_declaration`]), so installing the app creates it; found again
/// by its slug ([`circle_request`]).
pub const CIRCLE: CircleSpec = CircleSpec {
    slug: "filesync",
    name: "Filesync",
    description: "People who can see the files you share through Homebase Filesync",
    emoji: "📁",
    drive_permission: 1,
};

/// The drive request entry (`d`) for `drive`, in the keys the owner app reads.
pub(crate) fn drive_request(drive: &DriveSpec) -> serde_json::Value {
    json!({
        "a": drive.alias.to_string(),
        "t": drive.drive_type.to_string(),
        "n": drive.name,
        "d": drive.description,
        "p": drive.permission,
        "ds": drive.slug,
        "ts": drive.type_slug,
    })
}

/// [`CIRCLE`] as the app declares it at registration (`ac`), in the keys the owner app reads
/// (odin-js `AppCircleAuthorizationParams`, verified in `util.ts`): `s` slug, `n` name, `d`
/// description, `e` emoji, `g` drive grants (`a` alias, `t` type, `p` permission). It may grant
/// only what the registration grants the app (`AssertDeclaredCirclesAsync`): Read on its drive.
pub(crate) fn circle_declaration() -> serde_json::Value {
    json!({
        "s": CIRCLE.slug,
        "n": CIRCLE.name,
        "d": CIRCLE.description,
        "e": CIRCLE.emoji,
        "g": [{
            "a": SYNC_DRIVE.alias.to_string(),
            "t": SYNC_DRIVE.drive_type.to_string(),
            "p": CIRCLE.drive_permission,
        }],
    })
}

/// [`CIRCLE`] as `circles/create` asks for it, by its slug, on `drive`: the sync drive this
/// device found (at install, [`circle_declaration`] can only name the one it asks for).
pub(crate) fn circle_request(drive: TargetDrive) -> serde_json::Value {
    json!({
        "name": CIRCLE.name,
        "slug": CIRCLE.slug,
        "description": CIRCLE.description,
        "emoji": CIRCLE.emoji,
        "driveGrants": [{
            "permissionedDrive": {
                "drive": {
                    "alias": drive.alias.simple().to_string(),
                    "type": drive.drive_type.simple().to_string(),
                },
                "permission": CIRCLE.drive_permission,
            }
        }],
        "permissions": { "keys": [] },
    })
}

/// The YouAuth `permission_request` for registering the app (or authorizing a new device).
/// `friendly_name` names this device in the owner's list of the app's clients.
pub(crate) fn permission_request(friendly_name: &str, return_url: &str) -> serde_json::Value {
    json!({
        "n": APP_NAME,
        "appId": APP_ID.simple().to_string(),
        "as": APP_SLUG,
        "fn": friendly_name,
        // JSON lists sent as strings, as the owner app expects; `cd` must be present.
        "d": serde_json::to_string(&[drive_request(&SYNC_DRIVE)]).unwrap_or_default(),
        "cd": "[]",
        // Created with the app when it is installed; only the install reads it.
        "ac": serde_json::to_string(&[circle_declaration()]).unwrap_or_default(),
        "return": return_url,
    })
}

/// The owner app's page that gives the installed app more (`/owner/appupdate`, odin-js
/// `ExtendAppPermissions.tsx`), asked for [`SYNC_DRIVE`] again: a drive the app asks for as its
/// own is made there when it is missing, owned by the app; one that exists is only granted
/// (`ensureAppOwnedDrives` and `ensureDrive`, verified). The browser comes back to `return_url`,
/// with `status=canceled` when the owner says no. For [`HomebaseError::NoDrive`].
pub fn restore_drive_url(identity: &str, return_url: &str) -> String {
    let params = [
        ("appId", APP_ID.simple().to_string()),
        ("d", json!([drive_request(&SYNC_DRIVE)]).to_string()),
        // Required, as at registration: no circle grants asked for.
        ("cd", "[]".to_owned()),
        ("return", return_url.to_owned()),
    ];
    let query: Vec<String> = params
        .iter()
        .map(|(k, v)| format!("{k}={}", utf8_percent_encode(v, NON_ALPHANUMERIC)))
        .collect();
    format!("https://{identity}/owner/appupdate?{}", query.join("&"))
}

/// The app's circle on the identity, by its slug: the one installing the app created, the one an
/// earlier version created (circles got slugs from their names: "Filesync" is `filesync`), or a
/// new one if there is none. `circles/create` with a slug the app holds returns that circle and
/// changes nothing (verified: `CircleMembershipService.CreateAppCircleAsync`), so every device
/// finds the same circle.
pub fn app_circle(remote: &HomebaseRemote) -> Result<Uuid, HomebaseError> {
    let id: Value = remote.client().post(
        "/api/v2/connections/circles/create",
        &circle_request(remote.drive()),
    )?;
    Ok(serde_json::from_value(id)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slugs_are_valid_odin_slugs() {
        // OdinSlug: ^[a-z0-9]([a-z0-9-]*[a-z0-9])?$, at most 14 characters.
        for slug in [APP_SLUG, SYNC_DRIVE.slug, SYNC_DRIVE.type_slug, CIRCLE.slug] {
            assert!(!slug.is_empty() && slug.len() <= 14, "{slug}");
            assert!(
                slug.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            );
            assert!(!slug.starts_with('-') && !slug.ends_with('-'));
        }
    }

    #[test]
    fn drives_param_is_a_json_list_in_a_string() -> Result<(), serde_json::Error> {
        let request = permission_request("laptop", "http://localhost:1/cb");
        let drives: serde_json::Value =
            serde_json::from_str(request["d"].as_str().unwrap_or_default())?;
        assert_eq!(drives[0]["ds"], "filesync");
        assert_eq!(drives[0]["p"], 3);
        assert_eq!(request["appId"], "03ae1a49cdae483f9da7fedc0e8f20b9");
        Ok(())
    }

    /// Asked for again, the drive is the one registration asks for, as the app's own (`n`, `ds`):
    /// what makes the owner app create it.
    #[test]
    fn restoring_asks_for_the_registered_drive() -> Result<(), Box<dyn std::error::Error>> {
        let url = restore_drive_url("anna.example.com", "http://localhost:1/back?x=1");
        let (page, query) = url.split_once('?').ok_or("no query")?;
        assert_eq!(page, "https://anna.example.com/owner/appupdate");
        let params: std::collections::HashMap<&str, String> = query
            .split('&')
            .filter_map(|kv| kv.split_once('='))
            .map(|(k, v)| {
                (
                    k,
                    percent_encoding::percent_decode_str(v)
                        .decode_utf8_lossy()
                        .into(),
                )
            })
            .collect();
        assert_eq!(params["appId"], "03ae1a49cdae483f9da7fedc0e8f20b9");
        assert_eq!(params["return"], "http://localhost:1/back?x=1");
        assert_eq!(params["cd"], "[]");
        let registered = permission_request("laptop", "http://localhost:1/cb");
        assert_eq!(params["d"], registered["d"].as_str().ok_or("no d")?);
        Ok(())
    }

    /// Registering declares the circle in `ac`, a JSON list in a string.
    #[test]
    fn the_circle_is_declared_at_registration() -> Result<(), serde_json::Error> {
        let request = permission_request("laptop", "http://localhost:1/cb");
        let declared: Value = serde_json::from_str(request["ac"].as_str().unwrap_or_default())?;
        assert_eq!(declared, json!([circle_declaration()]));
        assert_eq!(declared[0]["s"], "filesync");
        Ok(())
    }
}
