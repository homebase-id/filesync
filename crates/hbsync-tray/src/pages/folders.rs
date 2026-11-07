//! Choosing which of the user's own folders sync (Documents, Pictures, …, the Desktop): each
//! where this computer keeps it, how much it holds, and why it can't be added if it can't. The
//! Desktop gets a folder of its own in Desktops on the drive, named as the user says, or joins
//! one there (this computer's from before).

use std::path::Path;
use std::time::Instant;

use hbsync_app::folders::{self, Own, Taken};
use hbsync_app::{Identity, Root};
use hbsync_types::{NodeId, Role};

use super::*;

/// Counting a folder's files stops after this many entries; the page's counting, after this long.
const COUNT_AT_MOST: usize = 200_000;
const COUNT_FOR: Duration = Duration::from_secs(2);

/// Whether macOS asks before an app reads the folder: not read before the user chose it.
fn asks_first(own: Own) -> bool {
    cfg!(target_os = "macos")
        && matches!(
            own,
            Own::Role(Role::Documents | Role::Downloads) | Own::Desktop
        )
}

impl Server {
    pub(super) fn folders_page(&self, said: Option<&str>) -> String {
        let config = match Config::load(&self.paths.config_file()) {
            Ok(c) => c.unwrap_or_default(),
            Err(e) => {
                return format!(
                    "<h1>Set up this computer</h1><p class=error>{}</p>",
                    escape(&e.to_string())
                );
            }
        };
        let Some(main) = config.root().ok().flatten() else {
            return "<h1>Set up this computer</h1><p>Log in to Homebase first.</p>".into();
        };
        let identity = &main.identity;
        let home = folders::home();
        let count_until = Instant::now() + COUNT_FOR;
        let rows: String = Role::ALL
            .into_iter()
            .map(|role| {
                let synced = config.folder(identity, Some(role));
                let (path, action) = match synced {
                    Some(root) => (
                        Some(root.folder.clone()),
                        self.button("folders/remove", Own::Role(role), "", "Stop syncing", true),
                    ),
                    None => match setup::role_folder(role) {
                        Err(e) => (None, small(&e.to_string())),
                        Ok(path) => {
                            let root = Root::role(identity, &path, role);
                            let refused = folders::check(&config, &root, home.as_deref()).err();
                            let action = match refused {
                                Some(why) => small(&why),
                                None => {
                                    self.button("folders/add", Own::Role(role), "", "Sync", false)
                                }
                            };
                            (Some(path), action)
                        }
                    },
                };
                let place = path.as_deref().map_or(String::new(), |p| {
                    place(p, synced.is_some(), Own::Role(role), count_until)
                });
                let note = match role {
                    Role::Downloads => {
                        "<p class=small>Not recommended: Downloads fills up with installers and \
                         half-finished files, which then take room on every device.</p>"
                    }
                    _ => "",
                };
                format!(
                    "<li><strong>{}</strong>{}<br>{place}{note}{action}</li>",
                    role.name(),
                    state(synced.is_some())
                )
            })
            .collect();
        // Counted after the role folders, as the last row.
        let desktop = self.desktop_row(&config, identity, home.as_deref(), count_until);
        let said = said.map_or(String::new(), |s| format!("<p>{}</p>", escape(s)));
        format!(
            "<h1>Set up this computer</h1>{said}\
             <p>Besides your Homebase folder, {APP_NAME} can sync your own folders where they are, \
             with {identity}. A folder another device syncs already is joined: identical files \
             are kept once, a file that differs is kept twice.</p>\
             <ul class=deleted>{rows}{desktop}</ul>",
            identity = escape(identity),
        )
    }

    /// The Desktop's row: synced, its folder's name on the drive to change; else how it would
    /// sync ([`Server::desktop_to_add`]).
    fn desktop_row(
        &self,
        config: &Config,
        identity: &Identity,
        home: Option<&Path>,
        count_until: Instant,
    ) -> String {
        let synced = config.desktop(identity);
        let (path, action) = match synced {
            Some(root) => {
                // Its folder's name as the drive has it (a rename made elsewhere shown).
                let seen = folders::desktops_seen(&self.paths, identity, &root.folder);
                let name = seen
                    .ok()
                    .and_then(|s| s.before.flatten())
                    .unwrap_or_default();
                let rename = format!(
                    "<form method=post action=\"{}\"><label class=check>Its folder in Desktops \
                     on your Homebase:</label>{}<button class=secondary>Rename</button></form>",
                    self.pages.url("folders/rename"),
                    name_field(&name, true)
                );
                let stop = self.button("folders/remove", Own::Desktop, "", "Stop syncing", true);
                (Some(root.folder.clone()), format!("{rename}{stop}"))
            }
            None => match setup::desktop_folder() {
                Err(e) => (None, small(&e.to_string())),
                Ok(path) => {
                    let action = self.desktop_to_add(config, identity, &path, home);
                    (Some(path), action)
                }
            },
        };
        let place = path.as_deref().map_or(String::new(), |p| {
            place(p, synced.is_some(), Own::Desktop, count_until)
        });
        format!(
            "<li><strong>Desktop</strong>{}<br>{place}<p class=small>Each computer's Desktop has \
             a folder of its own in Desktops on your Homebase, which your other computers show \
             in their Homebase folder.</p>{action}</li>",
            state(synced.is_some())
        )
    }

    /// How this computer's Desktop at `path`, not synced, would sync, or why it can't: with its
    /// folder from before, as the account's DB knows it by its path (nothing in the Desktop is
    /// read before the user chose it); else in a new folder in Desktops, named as the computer
    /// to start with, or joining one there.
    fn desktop_to_add(
        &self,
        config: &Config,
        identity: &Identity,
        path: &Path,
        home: Option<&Path>,
    ) -> String {
        // Checked as a Desktop: which folder in Desktops it gets doesn't count here.
        let root = Root::desktop(identity, path, NodeId::new());
        if let Err(why) = folders::check(config, &root, home) {
            return small(&why);
        }
        // A DB that can't be read now: nothing known (adding it says what is wrong).
        let seen = folders::desktops_seen(&self.paths, identity, path).unwrap_or_default();
        let form = match seen.before {
            Some(name) => again_form(name.as_deref()),
            None => new_form(&seen.listed, &config.device_name()),
        };
        self.button("folders/add", Own::Desktop, &form, "Sync", false)
    }

    /// A form posting `own`'s key to `page`, holding `inner` above its button.
    fn button(&self, page: &str, own: Own, inner: &str, text: &str, secondary: bool) -> String {
        format!(
            "<form method=post action=\"{url}\"><input type=hidden name=role value={key}>\
             {inner}<button{class}>{text}</button></form>",
            url = self.pages.url(page),
            key = own.key(),
            class = if secondary { " class=secondary" } else { "" },
        )
    }

    /// Makes a change with the config and the identity set up (logged in), then has the menu
    /// look again; what to say.
    fn changing(
        &self,
        change: impl FnOnce(&Config, &Identity) -> hbsync_app::Result<String>,
    ) -> String {
        let result = (|| {
            let config = Config::load(&self.paths.config_file())?.unwrap_or_default();
            let identity = config.root()?.ok_or("not logged in")?.identity.clone();
            change(&config, &identity)
        })();
        (self.send)(Event::Wake);
        result.unwrap_or_else(|e| e.to_string())
    }

    /// Adds or removes the folder the form names; what to say.
    pub(super) fn change_folder(&self, form: &HashMap<String, String>, add: bool) -> String {
        let Some(own) = form.get("role").and_then(|r| r.parse::<Own>().ok()) else {
            return "Nothing changed.".into();
        };
        self.changing(|config, identity| match (own, add) {
            (_, false) => {
                let Some(root) = own.synced(config, identity) else {
                    return Ok(format!("{} wasn't synced.", own.name()));
                };
                folders::remove_synced(&self.paths, &root.folder)?;
                Ok(format!(
                    "{} is no longer synced. Its files stay.",
                    own.name()
                ))
            }
            (Own::Desktop, true) => self.add_desktop(config, identity, form),
            (Own::Role(role), true) => {
                let folder = setup::role_folder(role)?;
                // Read here, now: macOS asks for access to a protected folder (Documents,
                // Downloads) while the user is at this page, not later out of the blue.
                std::fs::read_dir(&folder)?;
                let root = Root::role(identity, &folder, role);
                Ok(added(
                    folders::add_synced(&self.paths, &root, None)?,
                    role.name(),
                ))
            }
        })
    }

    /// Adds this computer's Desktop as the form says: in a folder of its own, named as typed
    /// (checked as `hbsync add desktop --name` checks it), or joining the folder in Desktops
    /// picked (`join`, its node), which keeps its name; what to say.
    fn add_desktop(
        &self,
        config: &Config,
        identity: &Identity,
        form: &HashMap<String, String>,
    ) -> hbsync_app::Result<String> {
        let folder = setup::desktop_folder()?;
        // Read here, now: macOS asks for access to the Desktop while the user is at this page.
        std::fs::read_dir(&folder)?;
        let join = form.get("join").filter(|j| *j != "new");
        let (root, name) = match join {
            Some(node) => {
                let node = hbsync_types::Uuid::parse_str(node).map(NodeId);
                let node = node.map_err(|_| "no such folder in Desktops")?;
                let seen = folders::desktops_seen(&self.paths, identity, &folder)?;
                if !seen.listed.iter().any(|(listed, _)| *listed == node) {
                    return Err("that folder is no longer in Desktops".into());
                }
                (Root::desktop(identity, &folder, node), None)
            }
            None => {
                let root = folders::desktop(config, &self.paths, identity, &folder)?;
                // None typed (or none asked, synced here before): as it is, or the computer's.
                let name = form.get("name").filter(|n| !n.trim().is_empty());
                let name = name.map(|n| folders::checked_desktop_name(n)).transpose()?;
                (root, name)
            }
        };
        Ok(added(
            folders::add_synced(&self.paths, &root, name.as_deref())?,
            "Desktop",
        ))
    }

    /// Names the Desktop's folder on the drive as typed (as `hbsync add desktop --name` does),
    /// unless it is so named already; what to say.
    pub(super) fn rename_desktop(&self, form: &HashMap<String, String>) -> String {
        self.changing(|config, identity| {
            let Some(root) = config.desktop(identity) else {
                return Ok("Desktop isn't synced.".into());
            };
            let name = folders::checked_desktop_name(form.get("name").map_or("", String::as_str))?;
            let seen = folders::desktops_seen(&self.paths, identity, &root.folder)?;
            if seen.before.flatten().as_deref() == Some(name.as_str()) {
                return Ok(format!("Your Desktop's folder is named {name} already."));
            }
            Ok(
                match folders::add_synced(&self.paths, root, Some(&name))?.0 {
                    Taken::Now => format!("Your Desktop's folder is named {name} now."),
                    Taken::AtStart => {
                        format!("Your Desktop's folder is named {name} once {APP_NAME} runs.")
                    }
                },
            )
        })
    }
}

/// What to say once `what` is added (`taken`), and what readying the account's DB set aside.
fn added((taken, set_aside): (Taken, Option<folders::Unusable>), what: &str) -> String {
    let said = match taken {
        Taken::Now => format!("{what} syncs now."),
        Taken::AtStart => format!("{what} is set up: it syncs once {APP_NAME} runs."),
    };
    match set_aside {
        Some(why) => format!("{} {said}", hbsync_app::words::set_aside(why)),
        None => said,
    }
}

/// How the Desktop synced here before syncs again: with its folder from before, `name`d.
fn again_form(name: Option<&str>) -> String {
    let name = name.map_or(" ".into(), |n| format!(", {}, ", escape(n)));
    format!("<p class=small>It syncs with its folder from before{name}again.</p>")
}

/// How the Desktop new here would sync: in a new folder in Desktops named as typed (the
/// computer's name to start with; none typed, the computer's as the drive takes it), or, with
/// folders `listed` there, joining one of those.
fn new_form(listed: &[(NodeId, String)], computer: &str) -> String {
    let computer = folders::valid_desktop_name(computer).unwrap_or_default();
    let name = name_field(&computer, false);
    if listed.is_empty() {
        return format!(
            "<label class=check>Its folder in Desktops on your Homebase:</label>{name}"
        );
    }
    let joins: String = listed
        .iter()
        .map(|(node, name)| {
            format!(
                "<label class=check><input type=radio name=join value={node}> {}</label>",
                escape(name)
            )
        })
        .collect();
    format!(
        "<label class=check><input type=radio name=join value=new checked> A new folder in \
         Desktops, named:</label>{name}\
         <p class=small>Or this computer's from before (another computer's would mix the two \
         Desktops):</p>{joins}"
    )
}

/// The text field of a Desktop's folder's name, holding `value`.
fn name_field(value: &str, required: bool) -> String {
    let required = if required { " required" } else { "" };
    format!(
        "<input type=text name=name value=\"{}\"{required}>",
        escape(value)
    )
}

/// The folder at `path` and, once it may be read (synced, or not one macOS asks about), how
/// much it holds.
fn place(path: &Path, synced: bool, own: Own, until: Instant) -> String {
    let size = if !synced && asks_first(own) {
        String::new()
    } else {
        size(path, until)
    };
    format!(
        "<code>{}</code> {size}",
        escape(&path.display().to_string())
    )
}

/// Said beside a folder that syncs.
fn state(synced: bool) -> &'static str {
    if synced {
        " <span class=small>syncing</span>"
    } else {
        ""
    }
}

fn small(text: &str) -> String {
    format!("<p class=small>{}</p>", escape(text))
}

/// How much `path` holds, in words, counted until `until` at most; "more than" when counting
/// stopped early.
fn size(path: &Path, until: Instant) -> String {
    let (mut files, mut bytes, mut seen) = (0u64, 0u64, 0usize);
    let mut todo = vec![path.to_path_buf()];
    while let Some(dir) = todo.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            seen += 1;
            if seen > COUNT_AT_MOST || Instant::now() > until {
                return format!(
                    "<span class=small>more than {} files, {}</span>",
                    words::count(files),
                    words::bytes(bytes)
                );
            }
            match entry.file_type() {
                Ok(t) if t.is_dir() => todo.push(entry.path()),
                Ok(t) if t.is_file() => {
                    files += 1;
                    bytes += entry.metadata().map_or(0, |m| m.len());
                }
                _ => {}
            }
        }
    }
    format!(
        "<span class=small>{} files, {}</span>",
        words::count(files),
        words::bytes(bytes)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Desktop new here: a new folder named as the computer (made one the drive takes, or
    /// left for the drive's default), or, with folders in Desktops, one of those to join; synced
    /// here before: it syncs with its folder again, nothing to choose.
    #[test]
    fn the_desktop_form_offers_a_new_folder_or_one_to_join() {
        let other = (NodeId::new(), "Old <laptop>".to_owned());
        let alone = new_form(&[], "Anna's PC");
        assert!(alone.contains("value=\"Anna's PC\""), "{alone}");
        assert!(
            !alone.contains("radio") && !alone.contains("required"),
            "{alone}"
        );

        let joins = new_form(std::slice::from_ref(&other), "Anna's PC");
        assert!(joins.contains("name=join value=new checked"), "{joins}");
        assert!(joins.contains(&format!("value={}", other.0)), "{joins}");
        assert!(joins.contains("Old &lt;laptop&gt;"), "escaped: {joins}");

        let again = again_form(Some("Laptop"));
        assert!(again.contains("from before, Laptop, again"), "{again}");
        assert!(!again.contains("input"), "{again}");

        // A computer's name the drive won't take: none typed for the user.
        assert!(new_form(&[], "a/b").contains("value=\"\""));
    }
}
