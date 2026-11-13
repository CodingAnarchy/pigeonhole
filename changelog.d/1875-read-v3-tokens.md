### Fixed
- After a rollback from 1.3, a token of a feed of every table that 1.3 took resumes. Before, it was refused with `InvalidArgument`. 1.3 adds the catalog at the token's position to such tokens (version 3, D325); this release reads them and ignores that part (#1853).
