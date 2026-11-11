### Added
- `phdb serve --cluster` nodes keep the schema in the partition map (D317 ticket 5, ICR 0117). `Schema.CreateTable` takes a `placement`. `CreateTable`, `DropTable`, `ListTables`, `DescribeTable` and `GET /v1/tables` work through any node. Each node makes its partitions' files follow the map; it leaves hand-made tables alone and reports them in `Cluster.Status`.
- One map writer per cluster, the node with the smallest id (D317 amendment 1). Schema changes and `phdb cluster set-map` go through it. `phdb cluster set-map --force` sets a map at another node when the writer is down.
