### Fixed
- A change feed far behind no longer slows down as it falls further behind: each step's resume position costs the same however much is still undelivered, so a feed that falls behind catches up again (#1397).
