//! The debugging trace ([`Sim::verbose`]).

use crate::Sim;

impl Sim {
    /// Adds the line `line` makes of the simulation to the trace, when tracing.
    pub(crate) fn note(&mut self, line: impl FnOnce(&Sim) -> String) {
        if self.verbose {
            let line = line(self);
            self.trace.push(line);
        }
    }
}
