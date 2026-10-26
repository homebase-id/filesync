//! Logging in and choosing the folder: the form, the browser's trip to the identity, and
//! what came of it.

use super::*;

impl Server {
    /// Asks the tray (its main thread) for the OS's folder picker, keeping the address typed
    /// in `form`. Only while the form is shown: not for a stale page while a login runs.
    pub(super) fn choose_folder(&self, form: &HashMap<String, String>) {
        {
            let mut s = lock(&self.pages.shared);
            if s.phase != Phase::Form {
                return;
            }
            if let Some(typed) = form.get("identity") {
                s.identity = typed.clone();
            }
        }
        let (tx, rx) = mpsc::channel();
        (self.send)(Event::PickFolder(tx));
        if let Ok(Some(folder)) = rx.recv() {
            let mut s = lock(&self.pages.shared);
            if s.phase == Phase::Form {
                s.folder = Some(folder);
                s.clear_form();
            }
        }
    }

    pub(super) fn setup_page(&self) -> String {
        let s = lock(&self.pages.shared);
        if s.phase != Phase::Form {
            drop(s);
            return self.done_page();
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
        let folder = lock(&self.pages.shared).folder();
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
        let done = self.pages.url("setup/done");
        let (paths, pages, send) = (self.paths.clone(), self.pages.clone(), self.send.clone());
        std::thread::spawn(move || {
            let mut authorize = |identity: &str, device: &str| {
                let login = setup::begin(identity, device)?.return_to(&done);
                let _ = url_tx.send(login.authorize_url().to_owned());
                login.finish_unless(setup::APPROVAL_TIMEOUT, &cancel)
            };
            let result = setup::set_up(&paths, &request, &mut authorize, &mut |line| {
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
