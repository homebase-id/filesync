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

use serde::Serialize;
use serde_json::{Value, json};
use uuid::{Uuid, uuid};

use crate::HomebaseError;
use crate::drive::{HomebaseRemote, TargetDrive};

/// The app's id on every identity. Never change it: it is how an identity recognises the app.
pub(crate) const APP_ID: Uuid = uuid!("03ae1a49-cdae-483f-9da7-fedc0e8f20b9");
/// The product's name where people read it (`docs/spec/packaging.md`, "The name shown"): the
/// packages' `site.display-name`.
pub const SHOWN_NAME: &str = "Homebase Drive";
/// The Mac app's name, which it keeps: its bundle is named after it (the packages'
/// `display-name`).
pub const MAC_NAME: &str = "Homebase Filesync";
/// The name this program shows: [`SHOWN_NAME`], or [`MAC_NAME`] on a Mac.
pub const APP_NAME: &str = if cfg!(target_os = "macos") {
    MAC_NAME
} else {
    SHOWN_NAME
};
/// The app's name on an identity, as registering it says: the Homebase app registers it so too
/// (chat-kmp `AppConfig.kt`, `filesyncAppRegistration`), and whichever registers first names it.
const REGISTERED_NAME: &str = "Homebase Filesync";
pub(crate) const APP_SLUG: &str = "filesync";

/// The identity's files in the browser: chat's web app on the identity, opened on its Filesync
/// mini-app.
pub fn files_online(identity: &str) -> String {
    format!("https://{identity}/apps/chat/#filesync")
}

/// What a grant on a drive gives, as odin-core's `DrivePermission` flags: added up in a
/// grant's `p`.
pub(crate) const READ: u32 = 1;
pub(crate) const WRITE: u32 = 2;
pub(crate) const REACT: u32 = 4;
pub(crate) const COMMENT: u32 = 8;

/// A drive the app asks for at registration.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DriveSpec {
    pub alias: Uuid,
    pub drive_type: Uuid,
    pub name: &'static str,
    pub description: &'static str,
    pub slug: &'static str,
    pub type_slug: &'static str,
    /// What the app may do on it ([`READ`] and the others, added up).
    pub permission: u32,
    /// How the drive is made, when it is (`sm`); a drive that exists stays as it is.
    pub sharing_model: SharingModel,
}

/// How a drive is made (odin-core `DriveSharingModel`, camelCase in JSON **[verified]**): the
/// drive request's `sm`. Private and selective share are zero knowledge (`encryption.md`, "A
/// private drive"); which a drive is, is read from the drive, never from the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum SharingModel {
    /// Only the owner's devices and apps hold the key (shares later): Filesync's.
    SelectiveShare,
}

/// The drive the sync folder mirrors.
pub(crate) const SYNC_DRIVE: DriveSpec = DriveSpec {
    alias: uuid!("cfceb937-b868-496b-91cb-3402f18320f8"),
    drive_type: uuid!("5da48122-e946-487a-8b9d-589642f3c91d"),
    name: "Filesync",
    description: "Files synced with your computers by Homebase Filesync",
    slug: "filesync",
    type_slug: "filesync",
    // Read and Write for syncing; React and Comment only to pass on to [`CIRCLE`], which
    // says why. As the chat app's Drive asks for the drive (chat-kmp
    // `filesyncAppRegistration`): whichever installs the app first, its drive and its
    // circle are the same.
    permission: READ | WRITE | REACT | COMMENT,
    // Private from the start: only the owner's devices hold its key (encryption.md, "A
    // private drive"). An older console ignores it and makes a shared one, which syncs too.
    sharing_model: SharingModel::SelectiveShare,
};

/// A circle the app owns.
#[derive(Debug, Clone, Copy)]
pub struct CircleSpec {
    /// Its address within the app: how a device finds it again.
    pub slug: &'static str,
    name: &'static str,
    description: &'static str,
    emoji: &'static str,
    /// What members get on the sync drive (as [`DriveSpec::permission`]): no more than
    /// the app holds there.
    drive_permission: u32,
}

/// The app's circle: people the files can be shared with, empty at first. Its members read,
/// react to and comment on what is shared with them; never Write, which would let a member
/// add files anywhere on the drive, whatever is shared with them. The chat app's Drive,
/// which does the sharing, offers no access a member's circle doesn't grant: with Read
/// alone it shares nothing (chat-kmp `FilesyncSharing.offeredAccess`). An app gives a
/// circle no more than it holds on the drive itself (odin-core
/// `AssertRegistrationMayGrantDrivesAsync`, verified), which is why [`SYNC_DRIVE`] asks for
/// React and Comment too.
/// Declared at registration ([`circle_declaration`]), so installing the app creates it; found again
/// by its slug ([`circle_request`]).
pub const CIRCLE: CircleSpec = CircleSpec {
    slug: "filesync",
    name: "Filesync",
    description: "People who can see the files you share through Homebase Filesync",
    emoji: "📁",
    drive_permission: READ | REACT | COMMENT,
};

// (The server's rule, held to where the two are set.)
const _: () = assert!(CIRCLE.drive_permission & SYNC_DRIVE.permission == CIRCLE.drive_permission);

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
        "sm": drive.sharing_model,
    })
}

/// [`CIRCLE`] as the app declares it at registration (`ac`), in the keys the owner app reads
/// (odin-js `AppCircleAuthorizationParams`, verified in `util.ts`): `s` slug, `n` name, `d`
/// description, `e` emoji, `g` drive grants (`a` alias, `t` type, `p` permission). It may grant
/// only what the registration grants the app on a drive (`AssertDeclaredCirclesAsync`: see
/// [`CIRCLE`]).
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

/// The one permission key the app asks for besides its drive: odin-core's `UseTransitRead`
/// (305, **[verified]**), for reading what others share with this identity through its own server
/// (`docs/spec/homebase-api-notes.md`, "Permission keys"; what it also gives the app:
/// `encryption.md`, "What it doesn't do").
pub(crate) const USE_TRANSIT_READ: u32 = 305;

/// odin-core's `UseTransitWrite` (210, **[verified]** `PermissionKeys`; live: without it a send
/// answers 403 "Sending, updating or deleting files on a peer requires UseTransitWrite"): to
/// send to other identities' drives through this one's server, which a change to what they
/// share with this identity is: a share is written to only where the app holds it. It is the
/// server's one key for sending to others, so it lets the app send more than that (what the
/// owner's circles let it write on other identities' drives; live relays): the owner's
/// consent page says it as the server names it.
pub(crate) const USE_TRANSIT_WRITE: u32 = 210;

/// Which of the permission keys the app asks for ([`USE_TRANSIT_READ`],
/// [`USE_TRANSIT_WRITE`]) it doesn't hold: an install from before it asked for one lacks
/// that one until the owner approves it ([`update_url`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Lacks {
    /// [`USE_TRANSIT_READ`]: what others share with this identity isn't read.
    pub reads: bool,
    /// [`USE_TRANSIT_WRITE`]: what they share for editing isn't changed.
    pub sends: bool,
}

impl Lacks {
    /// Both: what a new registration asks for.
    pub const ALL: Lacks = Lacks {
        reads: true,
        sends: true,
    };

    /// The app holds every key it asks for.
    pub fn none(self) -> bool {
        self == Lacks::default()
    }

    /// The keys lacking as the owner app reads them (`p`, odin-js
    /// `permissionParamToPermissionSet`: numbers joined by commas).
    fn keys(self) -> String {
        let keys = [
            self.reads.then_some(USE_TRANSIT_READ),
            self.sends.then_some(USE_TRANSIT_WRITE),
        ];
        let keys: Vec<String> = keys.iter().flatten().map(u32::to_string).collect();
        keys.join(",")
    }
}

/// The YouAuth `permission_request` for registering the app (or authorizing a new device).
/// `friendly_name` names this device in the owner's list of the app's clients.
pub(crate) fn permission_request(friendly_name: &str, return_url: &str) -> serde_json::Value {
    json!({
        "n": REGISTERED_NAME,
        "appId": APP_ID.simple().to_string(),
        "as": APP_SLUG,
        "fn": friendly_name,
        // JSON lists sent as strings, as the owner app expects; `cd` must be present.
        "d": serde_json::to_string(&[drive_request(&SYNC_DRIVE)]).unwrap_or_default(),
        "cd": "[]",
        // Read when the app registers; an installed app gets it on `/owner/appupdate`
        // ([`update_url`]), as a sign-in never adds permissions (odin-core
        // `AppNeedsRegistration`, verified).
        "p": Lacks::ALL.keys(),
        // Created with the app when it is installed; only the install reads it.
        "ac": serde_json::to_string(&[circle_declaration()]).unwrap_or_default(),
        "return": return_url,
    })
}

/// What the installed app asks the owner for again ([`update_url`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Update {
    /// [`SYNC_DRIVE`], made when it is missing, owned by the app (one that exists is only
    /// granted: `ensureAppOwnedDrives`, `ensureDrive`, verified): for [`HomebaseError::NoDrive`].
    Drive,
    /// The permission keys an install from before the app asked for them lacks. Only those:
    /// the page adds the keys it is asked for to the ones the app has, a second copy too,
    /// and tells the owner of it as more access (odin-js, verified).
    Permission(Lacks),
}

/// The owner app's page that gives the installed app more (`/owner/appupdate`, odin-js
/// `ExtendAppPermissions.tsx`): `what`. The browser comes back to `return_url`, with
/// `status=canceled` when the owner says no.
pub fn update_url(identity: &str, return_url: &str, what: Update) -> String {
    let app_id = APP_ID.simple().to_string();
    let drives = json!([drive_request(&SYNC_DRIVE)]).to_string();
    let keys;
    let asked = match what {
        Update::Drive => ("d", drives.as_str()),
        Update::Permission(lacks) => {
            keys = lacks.keys();
            ("p", keys.as_str())
        }
    };
    let query = crate::query(&[
        ("appId", &app_id),
        asked,
        // No circle grants asked for (a missing `cd`, or `d`, reads the same there, verified).
        ("cd", "[]"),
        ("return", return_url),
    ]);
    format!("https://{identity}/owner/appupdate?{query}")
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
    use std::collections::HashMap;

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
        assert_eq!(drives[0]["sm"], "selectiveShare");
        assert_eq!(drives[0]["p"], 15);
        assert_eq!(request["appId"], "03ae1a49cdae483f9da7fedc0e8f20b9");
        Ok(())
    }

    /// `url`'s page and query, decoded.
    fn query_of(url: &str) -> Result<(&str, HashMap<String, String>), &'static str> {
        let (page, query) = url.split_once('?').ok_or("no query")?;
        let decode = |s: &str| {
            percent_encoding::percent_decode_str(s)
                .decode_utf8_lossy()
                .into()
        };
        let params = query
            .split('&')
            .filter_map(|kv| kv.split_once('='))
            .map(|(k, v)| (decode(k), decode(v)))
            .collect();
        Ok((page, params))
    }

    /// Asked for again, the drive is the one registration asks for, as the app's own (`n`, `ds`):
    /// what makes the owner app create it; the permission key, only when the app lacks it.
    #[test]
    fn updating_asks_for_what_registration_asks() -> Result<(), Box<dyn std::error::Error>> {
        let registered = permission_request("laptop", "http://localhost:1/cb");
        let url = update_url(
            "anna.example.com",
            "http://localhost:1/back?x=1",
            Update::Drive,
        );
        let (page, params) = query_of(&url)?;
        assert_eq!(page, "https://anna.example.com/owner/appupdate");
        assert_eq!(params["appId"], "03ae1a49cdae483f9da7fedc0e8f20b9");
        assert_eq!(params["return"], "http://localhost:1/back?x=1");
        assert_eq!(params["cd"], "[]");
        assert_eq!(params["d"], registered["d"].as_str().ok_or("no d")?);
        // The drive alone: a key the app holds would be stored twice.
        assert!(!params.contains_key("p"));
        // The keys, only those it lacks: all of them are what registration asks for.
        let asked = |lacks| {
            let url = update_url("anna.example.com", "http://x/b", Update::Permission(lacks));
            query_of(&url).map(|(_, params)| params)
        };
        let params = asked(Lacks::ALL)?;
        assert_eq!(params["p"], registered["p"].as_str().ok_or("no p")?);
        assert_eq!(params["p"], "305,210");
        assert!(!params.contains_key("d"));
        let (reads, sends) = (true, true);
        let only_reads = Lacks {
            reads,
            ..Lacks::default()
        };
        let only_sends = Lacks {
            sends,
            ..Lacks::default()
        };
        assert_eq!(asked(only_reads)?["p"], "305");
        assert_eq!(asked(only_sends)?["p"], "210");
        assert!(Lacks::default().none() && !only_sends.none());
        Ok(())
    }

    /// Registering declares the circle in `ac`, a JSON list in a string.
    #[test]
    fn the_circle_is_declared_at_registration() -> Result<(), serde_json::Error> {
        let request = permission_request("laptop", "http://localhost:1/cb");
        let declared: Value = serde_json::from_str(request["ac"].as_str().unwrap_or_default())?;
        assert_eq!(declared, json!([circle_declaration()]));
        assert_eq!(declared[0]["s"], "filesync");
        // What a member gets, as the request says it: Read, React and Comment (13), never
        // Write, on the drive the request asks for, where the app is given each of them
        // (the server matches the two by the drive's alias, and refuses a circle given more).
        let asked: Value = serde_json::from_str(request["d"].as_str().unwrap_or_default())?;
        let (grant, drive) = (&declared[0]["g"][0], &asked[0]);
        assert_eq!(grant["p"], 13);
        assert_eq!((&grant["a"], &grant["t"]), (&drive["a"], &drive["t"]));
        let (given, held) = (grant["p"].as_u64(), drive["p"].as_u64());
        assert_eq!(held, Some(15));
        assert_eq!(given.zip(held).map(|(g, h)| g & h), given);
        Ok(())
    }
}
