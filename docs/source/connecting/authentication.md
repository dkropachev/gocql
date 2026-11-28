# Authentication

Call `WithAuth(auth.Password(username, password))` (package
`github.com/scylladb/gocql/v2/auth`) on the `ConfigBuilder`. Name the
authenticator classes the credentials may go to after the password, and a
server that asks for another class fails the connection before they are sent:

<!-- snippet
var username, password string
-->
```go
auth.Password(username, password, auth.PasswordAuthenticator, auth.ScyllaTransitionalAuthenticator)
```

With no classes named, any server that asks gets the credentials. The
constants `auth.PasswordAuthenticator`, `auth.ScyllaTransitionalAuthenticator`
and `auth.ScyllaSaslauthdAuthenticator` name the classes that check a
password.

Custom `auth.Provider.Start` creates an `auth.Exchange` per connection; providers
must support concurrent calls, and exchanges implement typed challenge and
success handling with context cancellation. Authentication bytes use typed
nullable values and are borrowed: a server message is valid only during the
call that receives it, and a response returned to the driver must stay
unchanged until the exchange is called again. Configuration and logs should
not expose credentials.

Authentication is independent of encryption; configure TLS when the deployment
requires encrypted credentials and transport.
