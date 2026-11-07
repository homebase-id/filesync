//! Choosing which of the user's own folders sync (Documents, Pictures, …, the Desktop): each
//! where this computer keeps it, how much it holds, and why it can't be added if it can't. The
//! Desktop gets a folder of its own in Desktops on the drive, named as the user says, or joins
//! one there (this computer's from before).

use std::path::Path;
use std::time::Instant;

use hbsync_app::folders::{self, Own, Taken};
use hbsync_app::protocol::DesktopFolder;
use hbsync_app::{Identity, Root, RootKind};
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
                        self.button("folders/remove", Own::Role(role), "Stop syncing", true),
                    ),
                    None => match setup::role_folder(role) {
                        Err(e) => (
                            None,
                            format!("<p class=small>{}</p>", escape(&e.to_string())),
                        ),
                        Ok(path) => {
                            let root = Root::role(identity, &path, role);
                            let refused = folders::check(&config, &root, home.as_deref()).err();
                            let action = match refused {
                                Some(why) => format!("<p class=small>{}</p>", escape(&why)),
                                None => self.button("folders/add", Own::Role(role), "Sync", false),
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
            .chain([self.desktop_row(&config, identity, home.as_deref(), count_until)])
            .collect();
        let said = said.map_or(String::new(), |s| format!("<p>{}</p>", escape(s)));
        format!(
            "<h1>Set up this computer</h1>{said}\
             <p>Besides your Homebase folder, {APP_NAME} can sync your own folders where they are, \
             with {identity}. A folder another device syncs already is joined: identical files \
             are kept once, a file that differs is kept twice.</p>\
             <ul class=deleted>{rows}</ul>",
            identity = escape(identity),
        )
    }

    /// The Desktop's row: synced, its folder's name on the drive to change; else how it would
    /// sync (a folder of its own, named as the computer, or one in Desktops to join).
    fn desktop_row(
        &self,
        config: &Config,
        identity: &Identity,
        home: Option<&Path>,
        count_until: Instant,
    ) -> String {
        // From the daemon (its mirror of the drive); none while it doesn't run.
        let listed = control::desktops(&self.paths, identity)
            .ok()
            .flatten()
            .unwrap_or_default();
        let named = |top: NodeId| listed.iter().find(|d| d.node == top).map(|d| &d.name);
        let url = |page| self.pages.url(page);
        let synced = config.desktop(identity);
        let (path, action) = match synced {
            Some(root) => {
                let name = match root.kind {
                    RootKind::Desktop(top) => named(top).map_or("", String::as_str),
                    _ => "",
                };
                let rename = format!(
                    "<form method=post action=\"{}\"><label class=check>Its folder in Desktops \
                     on your Homebase:</label><input type=text name=name value=\"{}\" \
                     required><button class=secondary>Rename</button></form>",
                    url("folders/rename"),
                    escape(name)
                );
                let stop = self.button("folders/remove", Own::Desktop, "Stop syncing", true);
                (Some(root.folder.clone()), format!("{rename}{stop}"))
            }
            None => match setup::desktop_folder().and_then(|path| {
                Ok((
                    folders::desktop(config, &self.paths, identity, &path)?,
                    path,
                ))
            }) {
                Err(e) => (None, small(&e.to_string())),
                Ok((root, path)) => match folders::check(config, &root, home) {
                    Err(why) => (Some(path), small(&why)),
                    Ok(_) => {
                        let form = desktop_form(&root, &listed, &config.device_name());
                        let action = format!(
                            "<form method=post action=\"{}\">\
                             <input type=hidden name=role value=desktop>{form}\
                             <button>Sync</button></form>",
                            url("folders/add")
                        );
                        (Some(path), action)
                    }
                },
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

    fn button(&self, page: &str, own: Own, text: &str, secondary: bool) -> String {
        format!(
            "<form method=post action=\"{url}\"><input type=hidden name=role value={key}>\
             <button{class}>{text}</button></form>",
            url = self.pages.url(page),
            key = own.key(),
            class = if secondary { " class=secondary" } else { "" },
        )
    }

    /// Adds or removes the folder the form names; what to say.
    pub(super) fn change_folder(&self, form: &HashMap<String, String>, add: bool) -> String {
        let Some(own) = form.get("role").and_then(|r| r.parse::<Own>().ok()) else {
            return "Nothing changed.".into();
        };
        let result = (|| -> hbsync_app::Result<String> {
            let config = Config::load(&self.paths.config_file())?.unwrap_or_default();
            let identity = config.root()?.ok_or("not logged in")?.identity.clone();
            let role = match (own, add) {
                (Own::Desktop, true) => return self.add_desktop(&config, &identity, form),
                (Own::Role(role), true) => role,
                (_, false) => {
                    let Some(root) = own.synced(&config, &identity) else {
                        return Ok(format!("{} wasn't synced.", own.name()));
                    };
                    folders::remove_synced(&self.paths, &root.folder)?;
                    return Ok(format!(
                        "{} is no longer synced. Its files stay.",
                        own.name()
                    ));
                }
            };
            let folder = setup::role_folder(role)?;
            // Read here, now: macOS asks for access to a protected folder (Documents,
            // Downloads) while the user is at this page, not later out of the blue.
            std::fs::read_dir(&folder)?;
            let root = Root::role(&identity, &folder, role);
            let taken = folders::add_synced(&self.paths, &root, None)?;
            Ok(added(taken, role.name()))
        })();
        (self.send)(Event::Wake);
        result.unwrap_or_else(|e| e.to_string())
    }

    /// Adds this computer's Desktop as the form says: in a folder of its own, named as typed
    /// (checked as `hbsync add desktop --name` checks it), or joining the folder in Desktops
    /// picked (`join`, its node); what to say.
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
                let listed = control::desktops(&self.paths, identity)?.unwrap_or_default();
                let joined = listed.into_iter().find(|d| d.node == node);
                let joined = joined.ok_or("that folder is no longer in Desktops")?;
                // Its name kept: the folder joined isn't renamed.
                (Root::desktop(identity, &folder, node), Some(joined.name))
            }
            None => {
                let root = folders::desktop(config, &self.paths, identity, &folder)?;
                (root, form.get("name").map(|n| valid_name(n)).transpose()?)
            }
        };
        let taken = folders::add_synced(&self.paths, &root, name.as_deref())?;
        Ok(added(taken, "Desktop"))
    }

    /// Names the Desktop's folder on the drive anew, as typed (as `hbsync add desktop --name`
    /// does); what to say.
    pub(super) fn rename_desktop(&self, form: &HashMap<String, String>) -> String {
        let result = (|| -> hbsync_app::Result<String> {
            let config = Config::load(&self.paths.config_file())?.unwrap_or_default();
            let identity = config.root()?.ok_or("not logged in")?.identity.clone();
            let Some(root) = config.desktop(&identity) else {
                return Ok("Desktop isn't synced.".into());
            };
            let name = valid_name(form.get("name").map_or("", String::as_str))?;
            match folders::add_synced(&self.paths, root, Some(&name))?.0 {
                Taken::Now => Ok(format!("Your Desktop's folder is named {name} now.")),
                Taken::AtStart => Ok(format!(
                    "Your Desktop's folder is named {name} once {APP_NAME} runs."
                )),
            }
        })();
        (self.send)(Event::Wake);
        result.unwrap_or_else(|e| e.to_string())
    }
}

/// `name` as a Desktop's folder can have it, or why not.
fn valid_name(name: &str) -> hbsync_app::Result<String> {
    folders::valid_desktop_name(name)
        .ok_or_else(|| format!("{name:?} can't name a folder on your Homebase").into())
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

/// How the Desktop not synced yet would sync: with its folder in Desktops from before (it
/// syncs here again), or in a new one named as typed (the computer's name to start with), or
/// joining one listed there.
fn desktop_form(root: &Root, listed: &[DesktopFolder], computer: &str) -> String {
    let top = match root.kind {
        RootKind::Desktop(top) => top,
        _ => NodeId::ROOT,
    };
    if let Some(own) = listed.iter().find(|d| d.node == top) {
        return format!(
            "<p class=small>It syncs with its folder from before, {}, again.</p>",
            escape(&own.name)
        );
    }
    let computer = folders::valid_desktop_name(computer).unwrap_or_default();
    let name = format!(
        "<input type=text name=name value=\"{}\" required>",
        escape(&computer)
    );
    if listed.is_empty() {
        return format!(
            "<label class=check>Its folder in Desktops on your Homebase:</label>{name}"
        );
    }
    let joins: String = listed
        .iter()
        .map(|d| {
            format!(
                "<label class=check><input type=radio name=join value={}> {}</label>",
                d.node,
                escape(&d.name)
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

    /// The Desktop not synced yet: with its folder from before listed, it syncs there again;
    /// else a new folder named as the computer (made one the drive takes), or, with folders in
    /// Desktops, one of those to join.
    #[test]
    fn the_desktop_form_offers_a_new_folder_or_one_to_join() {
        let a: Identity = "a.example".into();
        let mine = NodeId::new();
        let root = Root::desktop(&a, Path::new("/home/a/Desktop"), mine);
        let other = DesktopFolder {
            node: NodeId::new(),
            name: "Old <laptop>".into(),
        };

        let alone = desktop_form(&root, &[], "Anna's PC");
        assert!(alone.contains("value=\"Anna's PC\""), "{alone}");
        assert!(!alone.contains("radio"), "nothing to join: {alone}");

        let joins = desktop_form(&root, std::slice::from_ref(&other), "Anna's PC");
        assert!(joins.contains("name=join value=new checked"), "{joins}");
        assert!(joins.contains(&format!("value={}", other.node)), "{joins}");
        assert!(joins.contains("Old &lt;laptop&gt;"), "escaped: {joins}");

        let own = DesktopFolder {
            node: mine,
            name: "Laptop".into(),
        };
        let again = desktop_form(&root, &[other, own], "Anna's PC");
        assert!(again.contains("from before, Laptop, again"), "{again}");
        assert!(!again.contains("radio"), "{again}");

        // A computer's name the drive won't take: none typed for the user.
        assert!(desktop_form(&root, &[], "a/b").contains("value=\"\""));
    }
}
