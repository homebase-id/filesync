//! Logging in and choosing the folder: the form, the browser's trip to the identity, and
//! what came of it.

use super::*;

impl Pages {
    /// The picker [`Event::PickFolder`] opened closed, with the folder chosen, if any. Taken
    /// unless a login started meanwhile.
    pub(crate) fn picked(&self, folder: Option<PathBuf>) {
        let mut s = lock(&self.shared);
        s.picking = false;
        if let Some(folder) = folder
            && s.phase == Phase::Form
        {
            s.folder = Some(folder);
            s.clear_form();
        }
    }
}

impl Server {
    /// Asks the tray (its main thread) for the OS's folder picker, keeping the address typed
    /// in `form`, and returns without waiting for it: the page says a window opened until it
    /// closes. Only while the form is shown (not for a stale page while a login runs), and
    /// one picker at a time.
    pub(super) fn choose_folder(&self, form: &HashMap<String, String>) {
        let mut s = lock(&self.pages.shared);
        if s.phase != Phase::Form || s.picking {
            return;
        }
        if let Some(typed) = form.get("identity") {
            s.identity = hbsync_app::Identity::new(typed).to_string();
        }
        s.picking = true;
        drop(s);
        (self.send)(Event::PickFolder);
    }

    pub(super) fn setup_page(&self) -> String {
        let s = lock(&self.pages.shared);
        if s.phase != Phase::Form {
            drop(s);
            return self.done_page();
        }
        // In case the picker isn't in front (focus rules: a Linux portal's, Windows's): where
        // to look for it, until it closes. Looked at again every 2 s meanwhile (the page and
        // its icon, on 127.0.0.1), as while connecting.
        if s.picking {
            return "<meta http-equiv=refresh content=2>\
                    <h1>Choose the folder</h1>\
                    <p>Choose the folder to sync in the window that opened. This page goes on \
                    once you have.</p>\
                    <p class=small>Don't see it? It may be behind this window.</p>"
                .to_owned();
        }
        let folder = s
            .folder()
            .map_or_else(|| "(choose one)".into(), |f| f.display().to_string());
        let error = s
            .error
            .as_ref()
            .map(|e| format!("<p class=error>{}</p>", escape(e)))
            .unwrap_or_default();
        let merge = if s.merge_asked {
            "<label class=check><input type=checkbox name=merge> This folder already has files: \
             upload them and merge them with your Homebase</label>"
        } else {
            ""
        };
        // Enter in the address submits with the form's first button: an unseen *Continue*
        // ahead of *Change…* (which opened the folder picker instead).
        format!(
            "<h1>Connect to your Homebase</h1>\
             <p>Filesync keeps a folder on this computer in sync with your Homebase, \
             encrypted.</p>{error}\
             <form method=post action=\"{setup}\">\
             <button class=enter tabindex=-1 aria-hidden=true>Continue</button>\
             <label for=identity>Your Homebase address</label>\
             <input id=identity name=identity placeholder=\"anna.example.com\" value=\"{identity}\" \
             autofocus required>\
             <label>Folder on this computer</label>\
             <div class=folder><code>{folder}</code>\
             <button class=secondary formaction=\"{choose}\" formnovalidate>Change…</button></div>\
             {merge}\
             <button>Continue</button>\
             </form>\
             <p class=small>Next, your Homebase asks you to approve this computer.</p>",
            setup = self.pages.url("setup"),
            choose = self.pages.url("setup/folder"),
            identity = escape(&s.identity),
            folder = escape(&folder),
        )
    }

    /// Checks the form and starts connecting, one login at a time. The browser goes to the
    /// identity's approval page (`Some`), or back to the form with what's wrong (`None`).
    pub(super) fn start_setup(&self, form: &HashMap<String, String>) -> Option<String> {
        let typed = form.get("identity").map_or("", |s| s.as_str());
        let s = lock(&self.pages.shared);
        // Continue in another tab while the picker is open: the folder isn't chosen yet (the
        // page says so).
        if s.picking {
            return None;
        }
        let folder = s.folder();
        drop(s);
        let planned = match (Config::load(&self.paths.config_file()), folder) {
            (Ok(config), Some(folder)) => setup::plan(
                &config.unwrap_or_default(),
                typed,
                &folder,
                form.contains_key("merge"),
                hbsync_app::paths::home().as_deref(),
            ),
            (Err(e), _) => Err(Refusal::No(e.to_string())),
            (_, None) => Err(Refusal::No("choose a folder".into())),
        };
        let request = match planned {
            Ok(request) => request,
            Err(refused) => {
                let mut s = lock(&self.pages.shared);
                s.identity = hbsync_app::Identity::new(typed).to_string();
                s.error = Some(match refused {
                    Refusal::NeedsMerge(_) => {
                        s.merge_asked = true;
                        "This folder has files in it. Tick the box to merge them with your \
                         Homebase, or choose an empty or new folder."
                            .into()
                    }
                    Refusal::No(why) => why,
                });
                return None;
            }
        };

        let cancel = {
            let mut s = lock(&self.pages.shared);
            // A second submit (a double click) joins the login in progress.
            if s.phase == Phase::Connecting {
                return Some(self.pages.url("setup/done"));
            }
            s.clear_form();
            s.identity = request.identity.to_string();
            s.phase = Phase::Connecting;
            let cancel = Arc::new(AtomicBool::new(false));
            s.cancel = Some(Arc::clone(&cancel));
            cancel
        };
        (self.send)(Event::Wake);
        let (url_tx, url_rx) = mpsc::channel::<String>();
        let (paths, pages, send) = (self.paths.clone(), self.pages.clone(), self.send.clone());
        let mut owner = InTray {
            first: Some(url_tx),
            done: self.pages.url("setup/done"),
            cancel: Arc::clone(&cancel),
            pages: self.pages.clone(),
        };
        std::thread::spawn(move || {
            let result = setup::set_up(&paths, &request, &mut owner, &mut |line| {
                tracing::info!("{line}");
            });
            let result = result
                .map(|()| {
                    format!(
                        "{} syncs with {}.",
                        request.folder.display(),
                        request.identity
                    )
                })
                .map_err(|e| e.to_string());
            match &result {
                Ok(done) => tracing::info!("set up: {done}"),
                Err(e) => tracing::warn!("setting up failed: {e}"),
            }
            // Unless given up (started over): then a later login's page is showing.
            if !cancel.load(Ordering::Relaxed) {
                let mut s = lock(&pages.shared);
                s.phase = Phase::Done(result);
                s.cancel = None;
            }
            send(Event::Wake);
        });
        // The approval page, or (already authorized, or failed early) the outcome.
        Some(
            url_rx
                .recv_timeout(Duration::from_secs(60))
                .unwrap_or_else(|_| self.pages.url("setup/done")),
        )
    }

    pub(super) fn done_page(&self) -> String {
        let s = lock(&self.pages.shared);
        match &s.phase {
            Phase::Form => "<h1>Not connected yet</h1>".to_owned(),
            Phase::Connecting => format!(
                "<meta http-equiv=refresh content=2>\
                 <h1>Connecting…</h1><p>Approve this computer on your Homebase, in the page \
                 that opened. This page follows along.</p>\
                 <form method=post action=\"{}\"><p class=small>A typo in the address, or the \
                 page closed? <button class=secondary>Start over</button></p></form>",
                self.pages.url("setup/again")
            ),
            Phase::Done(Ok(what)) => format!(
                "<h1>Connected</h1><p>{}</p><p>The Filesync icon in your tray shows what it \
                 does. You can close this page.</p>",
                escape(what)
            ),
            Phase::Done(Err(why)) => format!(
                "<h1>Not connected</h1><p class=error>{}</p>\
                 <form method=post action=\"{}\"><button>Try again</button></form>",
                escape(why),
                self.pages.url("setup/again")
            ),
        }
    }
}

/// The owner's answers in the browser that came from the form: each owner-app page comes back
/// to the page that follows along.
struct InTray {
    /// Where the first page goes: the form's answer sends the browser there.
    first: Option<mpsc::Sender<String>>,
    done: String,
    cancel: Arc<AtomicBool>,
    pages: Pages,
}

impl InTray {
    /// Sends the browser to `url`: the first page as the form's answer, a later one from the
    /// page that follows along (once the browser is back on it).
    fn show(&mut self, url: &str) {
        match self.first.take() {
            Some(first) => drop(first.send(url.to_owned())),
            None => lock(&self.pages.shared).owner_page = Some(url.to_owned()),
        }
    }
}

impl setup::Owner for InTray {
    fn authorize(
        &mut self,
        identity: &str,
        device: &str,
    ) -> Result<setup::Credentials, setup::HomebaseError> {
        let login = setup::begin(identity, device)?.return_to(&self.done);
        self.show(login.authorize_url());
        login.finish_unless(setup::APPROVAL_TIMEOUT, &self.cancel)
    }

    fn restore_drive(&mut self, identity: &str) -> Result<bool, setup::HomebaseError> {
        let restore = setup::begin_restore(identity)?.return_to(&self.done);
        self.show(restore.url());
        let given = restore.finish_unless(setup::APPROVAL_TIMEOUT, &self.cancel);
        // Not taken (the page wasn't looked at again): not sent there later.
        lock(&self.pages.shared).owner_page = None;
        given
    }
}
