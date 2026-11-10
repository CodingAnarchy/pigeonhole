### Fixed
- A change feed far behind its frontier no longer slows down as it falls further behind: `FeedReader`'s resume token costs O(streams), not O(streams × undelivered commits), so a `phdb serve` feed hub that falls behind catches up again (#1397 ticket 5).
