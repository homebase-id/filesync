//! Reviewing held deletes, and Recently deleted: its items, restoring and emptying them.

use super::*;

/// At most this many items of Recently deleted on its page.
const MAX_DELETED: usize = 1000;

impl Server {
    pub(super) fn deletions_page(&self) -> String {
        let s = lock(&self.pages.shared);
        let Some(status) = &s.status else {
            return "<h1>Deletions</h1><p>Filesync isn't running.</p>".into();
        };
        let Some(held) = &status.held_deletes else {
            return "<h1>Deletions</h1><p>No deletions are waiting.</p>".into();
        };
        let what: String = words::held_deletes(held)
            .iter()
            .map(|line| format!("<p>{}</p>", escape(line)))
            .collect();
        let examples: String = held
            .examples
            .iter()
            .map(|p| format!("<li><code>{}</code></li>", escape(p)))
            .collect();
        let more = if held.more_than_shown() {
            "<li>…</li>"
        } else {
            ""
        };
        format!(
            "<h1>{} deletions are waiting</h1>\
             <p>Filesync holds back deletions this large until you confirm them.</p>{what}\
             <ul>{examples}{more}</ul>\
             <form method=post action=\"{url}\">\
             <input type=hidden name=token value=\"{token}\">\
             <button>Delete them</button></form>\
             <p class=small>Not sure? Leave this page: nothing is deleted, and the files stay \
             as they are on every device until you confirm.</p>",
            held.total(),
            url = self.pages.url("deletions"),
            token = escape(&held.token),
        )
    }

    /// Recently deleted, as the user deleted it: each item (a folder with what was in it),
    /// newest first.
    pub(super) fn deleted_page(&self) -> String {
        let list: Vec<_> = match control::deleted(&self.paths, usize::MAX) {
            Ok(Some(list)) => list.into_iter().map(|item| item.deleted).collect(),
            Ok(None) => return "<h1>Recently deleted</h1><p>Filesync isn't running.</p>".into(),
            Err(e) => {
                return format!(
                    "<h1>Recently deleted</h1><p class=error>{}</p>",
                    escape(&e.to_string())
                );
            }
        };
        if list.is_empty() {
            return "<h1>Recently deleted</h1><p>Nothing is in Recently deleted.</p>".into();
        }
        let now = words::now();
        let main = control::configured(&self.paths)
            .ok()
            .flatten()
            .map(|r| r.folder);
        // Shown: the items whose folder isn't in Recently deleted too; each with how many items
        // below it its restore brings back (those deleted with it).
        let by_node: HashMap<_, _> = list.iter().map(|d| (d.node, d)).collect();
        let mut along: HashMap<hbsync_types::NodeId, usize> = HashMap::new();
        for d in &list {
            let mut up = d.parent;
            for _ in 0..256 {
                let Some(folder) = by_node.get(&up) else {
                    break;
                };
                if !by_node.contains_key(&folder.parent) {
                    if folder.root == d.root {
                        *along.entry(folder.node).or_default() += 1;
                    }
                    break;
                }
                up = folder.parent;
            }
        }
        let rows: String = list
            .iter()
            .filter(|d| !by_node.contains_key(&d.parent))
            .take(MAX_DELETED)
            .map(|d| {
                let along = match along.get(&d.node).copied().unwrap_or(0) {
                    0 => String::new(),
                    n => format!(" <span class=small>with {n} items in it</span>"),
                };
                format!(
                    "<li><form method=post action=\"{url}\">\
                     <input type=hidden name=node value=\"{node}\">{what}{along} \
                     <button class=secondary>Restore</button></form></li>",
                    url = self.pages.url("deleted/restore"),
                    node = d.node.0,
                    what = escape(&words::deleted(d, main.as_deref(), now)),
                )
            })
            .collect();
        format!(
            "<h1>Recently deleted</h1>\
             <p>Deleted items are kept on your Homebase for {days} days, then deleted for good. \
             A restored item comes back on every device.</p><ul class=deleted>{rows}</ul>\
             <form method=post action=\"{empty}\"><input type=hidden name=count value={count}>\
             <input type=hidden name=until value={until}>\
             <button class=secondary>Empty now…</button></form>",
            days = self.keep_days(),
            empty = self.pages.url("deleted/empty"),
            count = list.len(),
            // Newest first: what the user sees, nothing deleted after this look.
            until = list[0].removed_at_ms,
        )
    }

    pub(super) fn empty_page(&self, form: &HashMap<String, String>) -> String {
        let field = |name| escape(form.get(name).map_or("", String::as_str));
        format!(
            "<h1>Empty Recently deleted?</h1>\
             <p>The {count} items in it are deleted for good, from your Homebase and every \
             device. This can't be undone.</p>\
             <form method=post action=\"{url}\"><input type=hidden name=until value=\"{until}\">\
             <button>Delete them for good</button></form>\
             <p class=small>Changed your mind? Leave this page: nothing is deleted.</p>",
            count = field("count"),
            until = field("until"),
            url = self.pages.url("deleted/empty/yes"),
        )
    }

    /// Restores the item the form names.
    pub(super) fn restore(&self, form: &HashMap<String, String>) -> String {
        let Some(node) = form
            .get("node")
            .and_then(|n| n.parse().ok())
            .map(hbsync_types::NodeId)
        else {
            return "Nothing was restored.".into();
        };
        self.ask_account(
            AccountControl::Restore { nodes: vec![node] },
            "Restoring: it comes back with the next sync, on every device.",
        )
    }

    /// Empties Recently deleted of what the page showed (deleted until then).
    pub(super) fn empty(&self, form: &HashMap<String, String>) -> String {
        let Some(at_ms) = form.get("until").and_then(|t| t.parse().ok()) else {
            return "Nothing was deleted.".into();
        };
        self.ask_account(
            AccountControl::EmptyDeletedUntil { at_ms },
            "Emptying Recently deleted.",
        )
    }

    /// Sends `control` to the account set up here; `done` if the daemon took it, else why not.
    pub(super) fn ask_account(&self, control: AccountControl, done: &str) -> String {
        let Ok(Some(root)) = control::configured(&self.paths) else {
            return "That didn't work: Filesync isn't set up.".into();
        };
        let control = Control::Account {
            identity: root.identity,
            control,
        };
        match control::control(&self.paths, control) {
            Ok(()) => {
                (self.send)(Event::Wake);
                done.into()
            }
            Err(e) => format!("That didn't work: {e}"),
        }
    }

    pub(super) fn deleted_said(&self, said: &str) -> String {
        format!(
            "<h1>Recently deleted</h1><p>{}</p><p><a href=\"{}\">Back to Recently deleted</a></p>",
            escape(said),
            self.pages.url("deleted"),
        )
    }

    /// How long Recently deleted keeps items, from the config.
    pub(super) fn keep_days(&self) -> u32 {
        Config::load(&self.paths.config_file())
            .ok()
            .flatten()
            .unwrap_or_default()
            .keep_deleted_days
            .0
    }

    pub(super) fn confirm_deletes(&self, form: &HashMap<String, String>) -> String {
        let path = lock(&self.pages.shared)
            .status
            .as_ref()
            .map(|s| s.path.clone());
        let (Some(token), Some(path)) = (form.get("token"), path) else {
            return "Nothing was confirmed.".into();
        };
        match control::control(
            &self.paths,
            Control::Folder {
                path,
                control: FolderControl::ConfirmDeletes {
                    token: token.clone(),
                },
            },
        ) {
            Ok(()) => {
                (self.send)(Event::Wake);
                "Confirmed: the deletions go through now.".into()
            }
            Err(e) => format!("Not confirmed: {e}"),
        }
    }
}
