# Security

Please don't open a public issue for a vulnerability. Report it privately through [GitHub security advisories](https://github.com/hanishi/ohra/security/advisories/new) instead. You'll get a reply within a week.

By default, remoting (`ohra.remote`, which the cluster also uses) and the management HTTP server listen on plain TCP, with no authentication and no encryption. Any host that can reach those ports can send messages to registered `@remotable` types. Messages are JSON, not pickle, so a peer can't run code. Bind these ports only on a trusted network.

`ActorSystem(..., ssl=RotatingKeysSSLEngineProvider())` runs remoting over TLS 1.3 with mutual authentication: a peer needs a certificate from the cluster's CA that shares a subject name with the node's own, or it can't connect, join the cluster, or gossip. Keep that CA private to the cluster. The management HTTP server stays plain HTTP either way, so bind its port only on a trusted network. See [chapter 4](docs/book/04-remoting.md#tls).
