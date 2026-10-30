# Security

Please don't open a public issue for a vulnerability. Report it privately through [GitHub security advisories](https://github.com/hanishi/ohra/security/advisories/new) instead. You'll get a reply within a week.

Remoting (`ohra.remote`, which the cluster also uses) and the management HTTP server listen on plain TCP, with no authentication and no encryption. Any host that can reach those ports can send messages to registered `@remotable` types. Messages are JSON, not pickle, and decode only into registered types, so a peer can't run arbitrary code. It can still send any registered message, with field values of any JSON type (decoding doesn't check them against the annotations), to any actor on the node, including the cluster's own actors under `/system`, so it can make a node leave or down another node. Bind these ports only on a trusted network.
