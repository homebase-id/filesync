//! Choosing which of the user's own folders sync (Documents, Pictures, …): each where this
//! computer keeps it, how much it holds, and why it can't be added if it can't.

use std::path::Path;
use std::time::Instant;

use hbsync_types::Role;

use super::*;

/// Counting a folder's files stops after this many entries, or this long.
const COUNT_AT_MOST: usize = 200_000;
const COUNT_FOR: Duration = Duration::from_secs(2);

impl Server {
    pub(super) fn folders_page(&self, said: Option<&str>) -> String {
        let config = match Config::load(&self.paths.config_file()) {
            Ok(c) => c.unwrap_or_default(),
            Err(e) => {
                return format!(
                    "<h1>Folders</h1><p class=error>{}</p>",
                    escape(&e.to_string())
                );
            }
        };
        let Some(main) = config.roots.iter().find(|r| r.role.is_none()) else {
            return "<h1>Folders</h1><p>Log in to Homebase first.</p>".into();
        };
        let identity = &main.identity;
        let home = hbsync_app::paths::home();
        let rows: String = Role::ALL
            .into_iter()
            .map(|role| {
                let synced = config
                    .roots
                    .iter()
                    .find(|r| r.identity == *identity && r.role == Some(role));
                let (path, action) = match synced {
                    Some(root) => (
                        Some(root.folder.clone()),
                        self.button("folders/remove", role, "Stop syncing", true),
                    ),
                    None => match setup::role_folder(role) {
                        Err(e) => (
                            None,
                            format!("<p class=small>{}</p>", escape(&e.to_string())),
                        ),
                        Ok(path) => {
                            let refused = setup::refuse_folder(&path, home.as_deref());
                            let action = match refused {
                                Some(why) => format!("<p class=small>{}</p>", escape(&why)),
                                None => self.button("folders/add", role, "Sync", false),
                            };
                            (Some(path), action)
                        }
                    },
                };
                let place = path.as_deref().map_or(String::new(), |p| {
                    format!(
                        "<code>{}</code> {}",
                        escape(&p.display().to_string()),
                        size(p)
                    )
                });
                let note = match role {
                    Role::Downloads => {
                        "<p class=small>Not recommended: Downloads fills up with installers and \
                         half-finished files, which then take room on every device.</p>"
                    }
                    _ => "",
                };
                let state = if synced.is_some() {
                    " <span class=small>syncing</span>"
                } else {
                    ""
                };
                format!(
                    "<li><strong>{}</strong>{state}<br>{place}{note}{action}</li>",
                    role.name()
                )
            })
            .collect();
        let said = said.map_or(String::new(), |s| format!("<p>{}</p>", escape(s)));
        format!(
            "<h1>Folders</h1>{said}\
             <p>Besides your Homebase folder, Filesync can sync your own folders where they are, \
             with {identity}. A folder another device syncs already is joined: identical files \
             are kept once, a file that differs is kept twice.</p>\
             <ul class=deleted>{rows}</ul>",
            identity = escape(identity),
        )
    }

    fn button(&self, page: &str, role: Role, text: &str, secondary: bool) -> String {
        format!(
            "<form method=post action=\"{url}\"><input type=hidden name=role value={role}>\
             <button{class}>{text}</button></form>",
            url = self.pages.url(page),
            role = role.key(),
            class = if secondary { " class=secondary" } else { "" },
        )
    }

    /// Adds or removes the role folder the form names; what to say.
    pub(super) fn change_folder(&self, form: &HashMap<String, String>, add: bool) -> String {
        let Some(role) = form.get("role").and_then(|r| r.parse::<Role>().ok()) else {
            return "Nothing changed.".into();
        };
        let result = (|| -> control::Result<String> {
            let config = Config::load(&self.paths.config_file())?.unwrap_or_default();
            let identity = config
                .roots
                .iter()
                .find(|r| r.role.is_none())
                .map(|r| r.identity.clone())
                .ok_or("not logged in")?;
            let running = control::running(&self.paths)?;
            if add {
                let folder = setup::role_folder(role)?;
                // Read here, now: macOS asks for access to a protected folder (Documents,
                // Downloads) while the user is at this page, not later out of the blue.
                std::fs::read_dir(&folder)?;
                if running {
                    control::add_folder(&self.paths, &identity, &folder, Some(role))?;
                } else {
                    hbsync_app::folders::add(&self.paths, &identity, &folder, Some(role))?;
                }
                return Ok(format!("{} syncs now.", role.name()));
            }
            let Some(root) = config
                .roots
                .iter()
                .find(|r| r.identity == identity && r.role == Some(role))
            else {
                return Ok(format!("{} wasn't synced.", role.name()));
            };
            if running {
                control::remove_folder(&self.paths, &root.folder)?;
            } else {
                hbsync_app::folders::remove(&self.paths, &root.folder)?;
            }
            Ok(format!(
                "{} is no longer synced. Its files stay.",
                role.name()
            ))
        })();
        (self.send)(Event::Wake);
        result.unwrap_or_else(|e| e.to_string())
    }
}

/// How much `path` holds, in words; "more than" when counting stopped early.
fn size(path: &Path) -> String {
    let started = Instant::now();
    let (mut files, mut bytes, mut seen) = (0u64, 0u64, 0usize);
    let mut todo = vec![path.to_path_buf()];
    while let Some(dir) = todo.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            seen += 1;
            if seen > COUNT_AT_MOST || started.elapsed() > COUNT_FOR {
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
