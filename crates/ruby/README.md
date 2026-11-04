# pigeonhole-db

Ruby bindings for [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole): an embedded, single-file, wide-column store, with BigTable's data model and SQLite's deployment model. The gem binds the Rust crate directly through magnus and rb-sys, with no C layer. Every engine call releases the GVL.

Install it with `gem install pigeonhole-db` (or `gem "pigeonhole-db"` in a Gemfile); you still `require "pigeonhole"`, and the module is `Pigeonhole`.

```ruby
require "pigeonhole"

Pigeonhole.open("app.phdb", create: true) do |db|
  users = db.table("users").family("profile").family("hits", Pigeonhole::Family.counter).create_if_missing
  users.put("user:ada", "profile", "name", "Ada Lovelace")
  users.incr("user:ada", "hits", "visits", 1)
  p users.get("user:ada", "profile", "name").value   # "Ada Lovelace" (binary)
end
```

- **Requirements:** Ruby 3.3 or later. Native gems cover Linux (glibc and musl, x86_64 and aarch64), macOS (x86_64 and arm64) and Windows (x64, UCRT). On other platforms the source gem compiles the extension, which needs Rust 1.96 or later and network access to crates.io (for the third-party crates `Cargo.lock` pins).
- **Also covered:** vector families, graphs, byte budgets, the change feed, agent memory (`Pigeonhole::Memory`), full-text search (`Pigeonhole::Text`) and the vector index (`Pigeonhole::Vector`). The [Ruby guide](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/guide/ruby.md) covers them.
- **Types:** the gem ships RBS signatures in `sig/`.

The gem's version is the `pigeonhole` crate's. License: MIT.
