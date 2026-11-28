### Fixed
- Replicas: an apply no longer reads the replica's state cell to check that its run chains on the last one; it uses the state the last apply stored, kept in memory (#2264). The read cost grew with every apply since the last flush, so a replica's apply rate fell as writes arrived, and on Linux a `phdb serve` partition's k1 writes ran at 0.05–0.14× its k0 writes. Present since 1.2.0.
