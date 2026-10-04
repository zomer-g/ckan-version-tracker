# Security policy

**Report a vulnerability by email to guy@z-g.co.il.** Email is the only
channel: GitHub's private vulnerability reporting is deliberately disabled on
this repository, because its notifications reach only GitHub's own app and
inbox, which the maintainer does not watch, and a report there could go
unanswered. Please do not open a public issue for something that exposes
data; let it be fixed first.

What helps: the URL or endpoint, the steps that got you there, and, if data
was exposed, what kind and roughly how many records, without copying the data
itself into the report.

What to expect: an acknowledgement within a day or two (one volunteer
maintainer), a fix before anything else when personal data is involved, and
an entry in the public log at <https://over.org.il/security> naming what was
wrong, what was exposed and the commit that fixed it. Reporters are credited
there only if they ask to be. No action is taken against anyone reporting in
good faith who kept and spread nothing they saw.

The log's source is `frontend/src/data/securityLog.ts`; the machine-readable
contact is served at `/.well-known/security.txt`.
