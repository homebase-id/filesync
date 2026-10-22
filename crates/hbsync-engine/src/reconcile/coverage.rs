//! What a plan judged from a narrow view needs more of ([`Plan::uncovered`]).
//!
//! The contract: `view::build_narrow` loads what the rules may need, and a rule that needs more
//! of a folder than the view holds says so here, in `Plan::uncovered`; the engine plans again
//! with those folders whole. A rule that needs more and doesn't say plans wrong in silence
//! (#208). The engine tests check every plan against one from whole folders (`check_narrow`),
//! the simulator every fourth one. The producers, all of them:
//! - a folder whose names [`Reconciler::fix_names`] judged without all its children in the input
//!   ([`Reconciler::judged_in_part`]) where a name may clash with one not in it
//!   ([`Reconciler::may_clash_unseen`]), or where it renamed one (a numbered name must be free
//!   among all of them): `fix_names` adds it;
//! - a folder with a masked child, or a file with a node in it ([`masked_and_in_files`]).

use super::*;

/// What a masked node does to its folder (keeps it, takes its name, leaves with it) is judged
/// with all of the folder's children. And so is what another writer put in a file on the drive
/// (§6.3: a node in a file; a file this device holds for a version it can't read, #175): any of
/// it may keep it (a masked one), so all of it is read. Hardly ever any.
pub(super) fn masked_and_in_files(input: &Input, masks: &Masks, plan: &mut Plan) {
    let Some(coverage) = &input.coverage else {
        return;
    };
    plan.uncovered.extend(
        masks
            .masked
            .iter()
            .filter_map(|m| input.remote.get(*m).or(input.base.get(*m)))
            .map(|s| s.parent)
            .filter(|p| !coverage.full.contains(p)),
    );
    plan.uncovered
        .extend(input.remote.iter().map(|(_, s)| s.parent).filter(|p| {
            input
                .remote
                .get(*p)
                .is_some_and(|f| f.kind != NodeKind::Folder)
                && !coverage.full.contains(p)
        }));
}

impl Reconciler<'_> {
    /// Whether the input has only part of the children of the folder covered as `covered_as`
    /// ([`Reconciler::covered_as`]). Its names judged so are right only for the names whose
    /// possible clashes the input has, and with no rename (a numbered name must be free among
    /// all).
    pub(super) fn judged_in_part(&self, covered_as: NodeId) -> bool {
        self.input
            .coverage
            .as_ref()
            .is_some_and(|c| !c.full.contains(&covered_as))
    }

    /// Whether a scoped child of `parent` (covered as `covered_as`) may clash with a child not
    /// in the input.
    pub(super) fn may_clash_unseen(&self, parent: NodeId, covered_as: NodeId) -> bool {
        let Some(coverage) = &self.input.coverage else {
            return false;
        };
        self.merged.children(parent).any(|c| {
            self.scope.contains(&c)
                && self
                    .merged
                    .get(c)
                    .is_some_and(|s| !coverage.holds(covered_as, &s.name))
        })
    }
}
