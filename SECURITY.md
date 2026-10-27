# Security

Please don't open a public issue for a vulnerability. Report it privately through [GitHub security advisories](https://github.com/hanishi/pekko-py/security/advisories/new) instead. You'll get a reply within a week.

Remoting (`pekko_py.remote`, which the cluster also uses) and the management HTTP server listen on plain TCP, with no authentication and no encryption. Any host that can reach those ports can send messages to registered `@remotable` types. Messages are JSON, not pickle, so a peer can't run code. Bind these ports only on a trusted network.
