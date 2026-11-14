### Changed
- A token of a feed of every table now records the tables it was taken among (token version 3, D325). Resuming it after a session that changed the schema without recording it in the feed answers `FeedExpired`: a table created, dropped or extended while rolled back to 1.1.x. The rollback note in the change-feed guide says what each version catches (#1853).
  - **Rolling back from 1.3 to 1.2:** 1.2.0 refuses such a token (`InvalidArgument`), so start those feeds again; 1.2.1 resumes it. Tokens of one or some tables are unchanged.
