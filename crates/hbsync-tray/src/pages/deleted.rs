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
             <p>Filesync holds back deletions this large until you confirm them. In \
             <code>{folder}</code>:</p>{what}\
             <ul>{examples}{more}</ul>\
             <form method=post action=\"{url}\">\
             <input type=hidden name=token value=\"{token}\">\
             <button>Delete them</button></form>\
             <p class=small>Not sure? Leave this page: nothing is deleted, and the files stay \
             as they are on every device until you confirm.</p>",
            held.total(),
            folder = escape(&status.path.display().to_string()),
            url = self.pages.url("deletions"),
            token = escape(&held.token),
        )
    }

    /// Recently deleted, as the user deleted it: each item (a folder with what was in it),
    /// newest first.
    pub(super) fn deleted_page(&self) -> String {
        let items = match control::deleted(&self.paths, usize::MAX) {
            Ok(Some(items)) => items,
            Ok(None) => return "<h1>Recently deleted</h1><p>Filesync isn't running.</p>".into(),
            Err(e) => {
                return format!(
                    "<h1>Recently deleted</h1><p class=error>{}</p>",
                    escape(&e.to_string())
                );
            }
        };
        if items.is_empty() {
            return "<h1>Recently deleted</h1><p>Nothing is in Recently deleted.</p>".into();
        }
        let now = words::now();
        let main = control::configured(&self.paths)
            .ok()
            .flatten()
            .map(|r| r.folder);
        // Shown: the items whose folder isn't in Recently deleted too; each with how many items
        // below it its restore brings back (those deleted with it).
        let by_node: HashMap<_, _> = items.iter().map(|i| (i.deleted.node, &i.deleted)).collect();
        let mut along: HashMap<hbsync_types::NodeId, usize> = HashMap::new();
        for d in items.iter().map(|i| &i.deleted) {
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
        let rows: String = items
            .iter()
            .filter(|i| !by_node.contains_key(&i.deleted.parent))
            .take(MAX_DELETED)
            .map(|item| {
                let d = &item.deleted;
                let along = match along.get(&d.node).copied().unwrap_or(0) {
                    0 => String::new(),
                    n => format!(" <span class=small>with {n} items in it</span>"),
                };
                format!(
                    "<li><form method=post action=\"{url}\">\
                     <input type=hidden name=node value=\"{node}\">\
                     <input type=hidden name=identity value=\"{identity}\">{what}{along} \
                     <button class=secondary>Restore</button></form></li>",
                    url = self.pages.url("deleted/restore"),
                    node = d.node.0,
                    identity = escape(&item.identity),
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
            count = items.len(),
            // Newest first: what the user sees, nothing deleted after this look.
            until = items[0].deleted.removed_at_ms,
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
        let node = form
            .get("node")
            .and_then(|n| n.parse().ok())
            .map(hbsync_types::NodeId);
        let (Some(node), Some(identity)) = (node, form.get("identity")) else {
            return "Nothing was restored.".into();
        };
        self.ask_account(
            identity,
            AccountControl::Restore { nodes: vec![node] },
            "Restoring: it comes back with the next sync, on every device.",
        )
    }

    /// Empties Recently deleted of what the page showed (deleted until then): every account's,
    /// as anything deleted after the look is newer.
    pub(super) fn empty(&self, form: &HashMap<String, String>) -> String {
        let Some(at_ms) = form.get("until").and_then(|t| t.parse().ok()) else {
            return "Nothing was deleted.".into();
        };
        let emptied = control::deleted(&self.paths, usize::MAX).and_then(|items| {
            control::empty_deleted_until(&self.paths, &items.unwrap_or_default(), at_ms)
        });
        match emptied {
            Ok(()) => {
                (self.send)(Event::Wake);
                "Emptying Recently deleted.".into()
            }
            Err(e) => format!("That didn't work: {e}"),
        }
    }

    /// Sends `control` to the account `identity`; `done` if the daemon took it, else why not.
    pub(super) fn ask_account(
        &self,
        identity: &str,
        control: AccountControl,
        done: &str,
    ) -> String {
        match control::account(&self.paths, identity, control) {
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
        match control::confirm_deletes(&self.paths, &path, token.clone()) {
            Ok(()) => {
                (self.send)(Event::Wake);
                "Confirmed: the deletions go through now.".into()
            }
            Err(e) => format!("Not confirmed: {e}"),
        }
    }
}
