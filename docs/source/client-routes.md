# Client routes

Set `Config.ClientRoutes` to typed `ClientRoutesOptions` before Connect. It
supplies connection identities and hostname overrides for route discovery.
The driver uses typed internal metadata queries and SNI-aware per-host hooks;
route refresh and connection lifecycle obey context cancellation.
