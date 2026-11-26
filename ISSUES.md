# Working on an issue

How an issue is worked on in this repo, from reading it to opening its PR. `CLAUDE.md` points
here.

## The essence first

Before reading code or planning, write the issue's essence on screen: in one or two sentences,
the problem as the person who hit it sees it (what goes wrong, for whom, when), not the fix. A
task given in chat gets the same.

- **Stay with the problem.** Fix it where it's caused, even when that's far from where it shows:
  a root-cause fix is part of the problem, a workaround at the symptom is not a good fix. The
  same bug in other places is the same problem: fix it there too. Small changes in the code
  you're already touching are fine when they make the fix clean (a rename, a test, a caller that
  has to follow). Code that works and isn't part of the problem stays as it is, even if you'd
  have written it differently: every change the problem didn't need is one more place to break
  something.
- **Check each change:** can you say in a few words how it serves the essence? If not, leave it
  out. If what you found is really broken, mention it or file it.
- If the issue holds several unrelated problems, say so, name the one you're taking, and leave
  the rest for their own PRs.
- If, after a first look at the code, you still can't state the essence, or the issue reads two
  ways, ask.
- If the work shows the real problem is a different one, say so and restate the essence before
  going on.

## Opening a PR for an issue

A PR that works on an issue starts its description with two lines (other PRs don't need them):

```
Problem: <the essence from "Working on an issue", updated if the work changed it>
Core change: <file:line> <the essence of the code that fixes it>: why it fixes the Problem
```

The Core change is the heart of the fix, not the whole diff: often one line, sometimes a few
lines or a short block, quoted or named. If the Core change doesn't answer the Problem, the PR
solves something else. After opening the PR, print on screen the PR title followed by its bare
full URL, then the Problem and Core change lines. (What the simplify and code-review passes
fixed and skipped follows, as `~/odin/CLAUDE.md` asks.)
