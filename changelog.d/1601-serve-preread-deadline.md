### Fixed
- `phdb serve`: an HTTP/1.1 request's deadline (`--request-timeout-ms`) now starts when its head arrives, before the server reads its body. A slow body used to get a fresh timeout once read, so a request could take up to twice the timeout (perf287 on #1613). A body that never arrives is still answered `DeadlineExceeded` (504), as every deadline in `phdb serve` is.
