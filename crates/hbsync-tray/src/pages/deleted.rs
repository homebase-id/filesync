//! Reviewing held deletes, and Recently deleted: its items, restoring and emptying them.

use hbsync_app::protocol::{DeletedAccount, DeletedList};
use hbsync_app::{HeldAnswer, State};

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
            return "<h1>Deletions</h1><p>No deletions to review.</p>".into();
        };
        let what: String = words::held_deletes(held, "Delete", "Put them back")
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
            "<h1>{} deletions to review</h1>\
             <p>Filesync holds back deletions this large until you answer. In \
             <code>{folder}</code>:</p>{what}\
             <ul>{examples}{more}</ul>\
             <form method=post action=\"{url}\">\
             <input type=hidden name=token value=\"{token}\">\
             <button name=answer value=delete>Delete</button> \
             <button name=answer value=put_back>Put them back</button></form>\
             <p class=small>Not sure? Leave this page. Nothing changes, here or on Homebase, \
             until you answer. Filesync reminds you until you do.</p>",
            held.total(),
            folder = escape(&status.path.display().to_string()),
            url = self.pages.url("deletions"),
            token = escape(&held.token),
        )
    }

    /// Recently deleted, as the user deleted it: each item (a folder with what was in it),
    /// newest first.
    pub(super) fn deleted_page(&self) -> String {
        let list = match control::deleted(&self.paths, MAX_DELETED) {
            Ok(Some(list)) => list,
            Ok(None) => return "<h1>Recently deleted</h1><p>Filesync isn't running.</p>".into(),
            Err(e) => {
                return format!(
                    "<h1>Recently deleted</h1><p class=error>{}</p>",
                    escape(&e.to_string())
                );
            }
        };
        if list.accounts.is_empty() {
            return "<h1>Recently deleted</h1><p>Nothing is in Recently deleted.</p>".into();
        }
        let now = words::now();
        // Shown: the items deleted on their own, each with how many items are in it, and whether
        // emptying leaves it.
        let rows: String = list
            .items
            .iter()
            .map(|item| {
                let d = &item.deleted;
                let along = match item.along {
                    0 => String::new(),
                    n => format!(" <span class=small>with {n} items in it</span>"),
                };
                let along = match d.stays {
                    true => format!("{along} <span class=small>({})</span>", words::STAYS),
                    false => along,
                };
                format!(
                    "<li><form method=post action=\"{url}\">\
                     <input type=hidden name=node value=\"{node}\">\
                     <input type=hidden name=identity value=\"{identity}\">{what}{along} \
                     <button class=secondary>Restore</button></form></li>",
                    url = self.pages.url("deleted/restore"),
                    node = d.node.0,
                    identity = escape(&item.identity),
                    what = escape(&words::deleted(item, now)),
                )
            })
            .collect();
        format!(
            "<h1>Recently deleted</h1>\
             <p>Deleted items are kept on your Homebase for {days} days, then deleted for good. \
             A restored item comes back on every device.</p><ul class=deleted>{rows}</ul>{empty}",
            days = hbsync_app::KEEP_DAYS,
            empty = empty_offer(&list, &self.pages.url("deleted/empty")),
        )
    }

    pub(super) fn empty_page(&self, form: &HashMap<String, String>) -> String {
        let field = |name| escape(form.get(name).map_or("", String::as_str));
        let stays = form
            .get("stays")
            .and_then(|n| n.parse().ok())
            .map(words::staying)
            .filter(|said| !said.is_empty())
            .map(|said| format!("<p>{said}</p>"))
            .unwrap_or_default();
        format!(
            "<h1>Empty Recently deleted?</h1>\
             <p>The {count} items in it that can go are deleted for good, from your Homebase \
             and every device. This can't be undone.</p>{stays}\
             <form method=post action=\"{url}\">\
             <input type=hidden name=listed value=\"{listed}\">\
             <button>Delete them for good</button></form>\
             <p class=small>Changed your mind? Leave this page: nothing is deleted.</p>",
            count = field("count"),
            listed = field("listed"),
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
            words::restoring(),
        )
    }

    /// Empties Recently deleted of everything it held when the page listed it, in the accounts
    /// that held anything.
    pub(super) fn empty(&self, form: &HashMap<String, String>) -> String {
        let listed = listed_from_field(form.get("listed").map_or("", String::as_str));
        if listed.is_empty() {
            return "Nothing was deleted.".into();
        }
        // What stays, as the page said (`empty_offer`).
        let stays = form.get("stays").and_then(|s| s.parse().ok()).unwrap_or(0);
        match control::empty_listed(&self.paths, &listed) {
            Ok(()) => {
                (self.send)(Event::Wake);
                words::emptying(stays, "the Filesync menu")
            }
            Err(e) => {
                (self.send)(Event::Wake);
                format!("That didn't work everywhere: {e}")
            }
        }
    }

    /// Sends `control` to the account `identity`; `done` if the daemon took it, else why not.
    pub(super) fn ask_account(
        &self,
        identity: &str,
        control: AccountControl,
        done: &str,
    ) -> String {
        match control::account(&self.paths, &identity.into(), control) {
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

    pub(super) fn confirm_deletes(&self, form: &HashMap<String, String>) -> String {
        let folder = lock(&self.pages.shared)
            .status
            .as_ref()
            .map(|s| (s.path.clone(), s.state == State::Paused));
        let (Some(token), Some((path, paused))) = (form.get("token"), folder) else {
            return "Nothing was confirmed.".into();
        };
        // Each answer by name: a form without one (a page from an older build) does nothing.
        let (answer, done, not) = match form.get("answer").map(String::as_str) {
            Some("put_back") => (
                HeldAnswer::PutBack,
                control::put_back(&self.paths, &path, token.clone()),
                "Not put back",
            ),
            Some("delete") => (
                HeldAnswer::Delete,
                control::confirm_deletes(&self.paths, &path, token.clone()),
                "Not confirmed",
            ),
            _ => return "Nothing was done: answer again from the page.".into(),
        };
        match done {
            Ok(()) => {
                (self.send)(Event::Wake);
                words::held_answered(answer, paused).into()
            }
            Err(e) => format!("{not}: {e}"),
        }
    }
}

/// The button that empties Recently deleted as listed (`url`): all it held now, nothing deleted
/// after this look. When emptying would take nothing (every folder in it still holds a live
/// item), it says so instead.
fn empty_offer(list: &DeletedList, url: &str) -> String {
    if list.total() == 0 {
        return "<p class=small>Nothing in it can be emptied yet: the folders in it still hold \
                items that aren't deleted. You can restore them.</p>"
            .into();
    }
    format!(
        "<form method=post action=\"{url}\">\
         <input type=hidden name=count value=\"{count}\">\
         <input type=hidden name=stays value=\"{stays}\">\
         <input type=hidden name=listed value=\"{listed}\">\
         <button class=secondary>Empty now…</button></form>",
        count = list.total(),
        stays = list.staying(),
        listed = escape(&listed_field(&list.accounts)),
    )
}

/// `accounts` as a form field (what emptying all of them takes): `identity/db/seq`,
/// comma-separated.
fn listed_field(accounts: &[DeletedAccount]) -> String {
    let parts: Vec<String> = accounts
        .iter()
        .map(|a| format!("{}/{}/{}", a.identity, a.db, a.seq))
        .collect();
    parts.join(",")
}

/// [`listed_field`] read back (without the counts); what doesn't read is left out.
fn listed_from_field(field: &str) -> Vec<DeletedAccount> {
    field
        .split(',')
        .filter_map(|part| {
            let (rest, seq) = part.rsplit_once('/')?;
            let (identity, db) = rest.rsplit_once('/')?;
            Some(DeletedAccount {
                identity: identity.into(),
                deletes: 0,
                items: 0,
                stays: 0,
                db: db.to_owned(),
                seq: seq.parse().ok()?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Empty is offered only when emptying takes something; else the page says why not.
    #[test]
    fn empty_is_offered_only_when_it_takes_something() {
        let list = |items| DeletedList {
            items: Vec::new(),
            accounts: vec![DeletedAccount {
                identity: "a.example".into(),
                deletes: 1,
                items,
                stays: 1,
                db: "d1".into(),
                seq: 3,
            }],
        };
        let offer = empty_offer(&list(2), "/e");
        assert!(offer.contains("Empty now") && offer.contains("name=stays value=\"1\""));
        let none = empty_offer(&list(0), "/e");
        assert!(
            !none.contains("<form") && none.contains("can be emptied yet"),
            "{none}"
        );
    }

    /// What emptying everything takes goes through the tray's form field and back.
    #[test]
    fn the_listed_accounts_go_through_a_form_field() {
        let a = |identity: &str, seq| DeletedAccount {
            identity: identity.into(),
            deletes: 0,
            items: 0,
            stays: 0,
            db: "d1".into(),
            seq,
        };
        let accounts = [a("a.example", 9), a("b/c.example", 4)];
        assert_eq!(listed_from_field(&listed_field(&accounts)), accounts);
        assert_eq!(listed_from_field(""), []);
    }
}
