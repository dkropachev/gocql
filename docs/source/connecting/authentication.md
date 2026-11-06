# Authentication

Set `config.Auth = auth.Password(username, password)` (package
`github.com/scylladb/gocql/v2/auth`) before Connect.
Custom `auth.Provider.Start` creates an `auth.Exchange` per connection; providers
must support concurrent calls, and exchanges implement typed challenge and
success handling with context cancellation. Authentication bytes use typed
nullable values. Configuration and logs should not expose credentials.

Authentication is independent of encryption; configure TLS when the deployment
requires encrypted credentials and transport.
