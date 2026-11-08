### Added
- **`phdb serve --feed-threads`:** the change feed's delivery (its shared subscriptions and every stream) runs on a runtime of its own, by default a quarter of the CPUs the shards don't use, carved out of the network threads' default. A put never waits behind feed streams for a worker (D313 ticket 5, #1397).
