//! What the Filesync app is on an identity: its registration, its drive and its circle.
//!
//! Homebase requires an app slug at registration and a drive slug and drive type slug on every
//! drive an app requests (odin-core #1787, verified in `AppRegistrationService.AssignSlugAsync`
//! and the owner app's `RegisterApp.tsx`). Slugs are lowercase letters, digits and inner
//! hyphens, at most 14 characters (`OdinSlug.cs`). Circles have no slug.
//!
//! TODO(odin-core): a sync drive should require payload hashes (`RequirePayloadHashes`, the
//! drive flag from #1899) and say it is a file-sync drive. Neither can be asked for at
//! registration today: the drive request (`d`) has no field for either, the owner app creates
//! requested drives without their attributes, and the owner UI can't set the flag or a drive
//! kind afterwards (verified in odin-js `useApp.ts` / `DriveProvider.ts`). Until then the
//! client sends hashes on every upload anyway, and the drive is only recognised by its type.

use serde_json::{Value, json};
use uuid::{Uuid, uuid};

use crate::drive::HomebaseRemote;
use crate::{Credentials, HomebaseError};

/// The app's id on every identity. Never change it: it is how an identity recognises the app.
pub const APP_ID: Uuid = uuid!("03ae1a49-cdae-483f-9da7-fedc0e8f20b9");
pub const APP_NAME: &str = "Homebase Filesync";
pub const APP_SLUG: &str = "filesync";

/// A drive the app asks for at registration.
#[derive(Debug, Clone, Copy)]
pub struct DriveSpec {
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
pub const SYNC_DRIVE: DriveSpec = DriveSpec {
    alias: uuid!("cfceb937-b868-496b-91cb-3402f18320f8"),
    drive_type: uuid!("5da48122-e946-487a-8b9d-589642f3c91d"),
    name: "Filesync",
    description: "Files synced with your computers by Homebase Filesync",
    slug: "filesync",
    type_slug: "filesync",
    permission: 3,
};

/// The circle the app creates for itself once registered (circles can't be requested at
/// registration; an app creates its own with `POST /api/v2/connections/circles/create`).
pub const CIRCLE_NAME: &str = "Filesync";
pub const CIRCLE_DESCRIPTION: &str =
    "People who can see the files you share through Homebase Filesync";
pub const CIRCLE_EMOJI: &str = "📁";
/// What members of the circle get on the sync drive: Read. The circle starts empty.
pub const CIRCLE_DRIVE_PERMISSION: u32 = 1;

/// The drive request entry (`d`) for `drive`, in the keys the owner app reads.
pub fn drive_request(drive: &DriveSpec) -> serde_json::Value {
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

/// The YouAuth `permission_request` for registering the app (or authorizing a new device).
/// `friendly_name` names this device in the owner's list of the app's clients.
pub fn permission_request(friendly_name: &str, return_url: &str) -> serde_json::Value {
    json!({
        "n": APP_NAME,
        "appId": APP_ID.simple().to_string(),
        "as": APP_SLUG,
        "fn": friendly_name,
        // JSON lists sent as strings, as the owner app expects; `cd` must be present.
        "d": serde_json::to_string(&[drive_request(&SYNC_DRIVE)]).unwrap_or_default(),
        "cd": "[]",
        "return": return_url,
    })
}

/// Creates the app's circle on the identity unless `creds` already names one, with Read on the
/// sync drive. Returns the new circle's id (the caller saves `creds`), or `None` when there
/// was one already.
pub fn ensure_circle(
    remote: &HomebaseRemote,
    creds: &mut Credentials,
) -> Result<Option<Uuid>, HomebaseError> {
    if creds.circle_id.is_some() {
        return Ok(None);
    }
    let drive = remote.drive();
    let id: Value = remote.client().post(
        "/api/v2/connections/circles/create",
        &json!({
            "name": CIRCLE_NAME,
            "description": CIRCLE_DESCRIPTION,
            "emoji": CIRCLE_EMOJI,
            "driveGrants": [{
                "permissionedDrive": {
                    "drive": {
                        "alias": drive.alias.simple().to_string(),
                        "type": drive.drive_type.simple().to_string(),
                    },
                    "permission": CIRCLE_DRIVE_PERMISSION,
                }
            }],
            "permissions": { "keys": [] },
        }),
    )?;
    let id: Uuid = serde_json::from_value(id)?;
    creds.circle_id = Some(id);
    Ok(Some(id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slugs_are_valid_odin_slugs() {
        // OdinSlug: ^[a-z0-9]([a-z0-9-]*[a-z0-9])?$, at most 14 characters.
        for slug in [APP_SLUG, SYNC_DRIVE.slug, SYNC_DRIVE.type_slug] {
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
}
