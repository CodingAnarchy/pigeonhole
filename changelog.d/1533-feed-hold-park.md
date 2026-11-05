### Changed
- A blocking change-feed wait (`Feed::next`) that is held back by the feed's own pending sync now sleeps until that sync completes, instead of checking every millisecond, so an idle durable feed costs no CPU while it waits (#1417).
