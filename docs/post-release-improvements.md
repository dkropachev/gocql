# Possible improvements after release

Ideas that were considered and deliberately left out of the first release.
Each says why it was deferred and what would bring it back. None is a
commitment; reconsider each one after the release, with user feedback.

## A non-generic session interface (`gocql.AnySession`)

`Session` is generic over its dialect (`docs/server-differences.md`,
"Dialects"), so a library that accepts sessions of several dialects has to be
generic over the dialect too: `func Load[D gocql.Dialect](s *gocql.Session[D])`.
A non-generic interface that every `*Session[D]` satisfies would let such code
take a plain parameter instead.

Deferred because the generic form works today, and an interface over the
session's generic methods cannot be written in Go: an interface cannot declare
a method with type parameters such as `Prepare[P, R]`. It would cover only the
non-generic methods (`Close`, `Metadata`, `ExecBatch`, ...) or need wrapper
functions. Reconsider if users ask for it.

## Register held pooled queries once

`Prepared.Run` and `Session.RunBatch` register the pooled query their rows
hold, so that `Session.Close` stops open rows and dropped rows release their
query (`internal/session/query_registry.go`). The registration allocates a
registry entry and a GC cleanup on every call: three allocations, the only
ones a warm `RunBatch` makes (`TestRunBatch/Allocs`). Registering each pooled
query once, when the pool creates it, and flipping a held flag per call would
remove them for both.

Deferred because it changes how every pooled handle is accounted for, which is
a change to the query registry of its own, with its own benchmark gate.

## Encode only the session's protocol in option templates

`newOptionTemplates` (`internal/session/params_template.go`) encodes the
parameters block of compiled options for both protocol 4 and protocol 5. A
session now speaks one version, fixed by its dialect, so options compiled for
a session could encode only that version's templates, halving the memory and
work of `Session.CompileOptions`. Options compiled on one session stay usable
on another, so the other version's templates would be built on first use.
