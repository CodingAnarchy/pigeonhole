### Added
- **The network API's contract** (D313, ICR 0112, #1397): `proto/pigeonhole/v1/*.proto`, the gRPC services and messages `phdb serve` will speak, with the HTTP/JSON mapping (`/v1`) and the error table, including which responses make a write safe to retry. `phdb`'s `serve` feature (off by default) compiles the generated code. The server itself follows.
