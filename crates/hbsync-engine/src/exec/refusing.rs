//! The run's side of the folder's refusals ([`crate::refused`]): a download whose bytes don't
//! match the drive's word strikes, a second strike at one version refuses it, and an
//! operation done ends its refusal.

use super::*;

impl Executor<'_> {
    /// What `op`'s `outcome` makes of its refusals: the outcome to handle (a second strike is
    /// [`Skip::Refused`]), with the record to write with it (a strike's; a refusal's, one a
    /// second strike became carrying the strike's key). An operation done ends its refusal
    /// here, in a write of its own.
    pub(super) fn refusals_after(
        &mut self,
        op: &Op,
        outcome: Outcome,
    ) -> Result<(Outcome, Option<Refused>), StateError> {
        // A download whose bytes didn't match: its strike, or a second time at one version,
        // refused. (The record either way: one refused carries the strike's key.)
        let (outcome, record) = match outcome {
            Outcome::Mismatch(why) => {
                let strike = self.refusal(op, &why, false)?;
                match self.refusals.twice(op, &strike) {
                    true => {
                        let why = format!("{why}, twice: the drive holds other bytes than it says");
                        let refused = Refused {
                            refused: true,
                            why: why.clone(),
                            ..strike
                        };
                        (Outcome::Skip(Skip::Refused(why)), Some(refused))
                    }
                    false => (Outcome::Mismatch(why), Some(strike)),
                }
            }
            Outcome::Skip(Skip::Refused(why)) => {
                let refused = self.refusal(op, &why, true)?;
                (Outcome::Skip(Skip::Refused(why)), Some(refused))
            }
            outcome => (outcome, None),
        };
        // Done: a refusal of it is over.
        if matches!(outcome, Outcome::Done | Outcome::Settled) && self.refusals.done(op) {
            let (node, kind) = (op.node(), op.kind());
            self.db.write(|s| s.folder().delete_refused(node, kind))?;
        }
        Ok((outcome, record))
    }

    /// The record of `op`'s refusal (`refused`; else its first strike), with `why`, now.
    fn refusal(&mut self, op: &Op, why: &str, refused: bool) -> Result<Refused, StateError> {
        self.db
            .read(|s| crate::refused::record(s, op, why, refused))
    }
}
