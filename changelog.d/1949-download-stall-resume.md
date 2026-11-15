### Changed
- `phdb cluster backup` resumes a broken download by the rule a replica uses: it gives up after three breaks in a row without progress, where it gave up at its sixth break in total, and it makes a new connection after each break. It also retries any error once the node has named its copy, and starts a fresh copy when the node's copy is gone (#1830).
- `phdb serve --replica-of`: a replica refuses a download message over 4 MiB plus framing from its primary, with an error that says so. It decoded a message of any size (#1830).

### Fixed
- `phdb cluster backup` and `phdb cluster move` no longer wait for ever on a node that stops sending with the connection up: 60 s of nothing is a break. The backup resumes, and the move fails and says to run it again (#1830).
- `phdb cluster move`: when the new node refuses an upload part-way through a file, the error gives the node's reason, where it said only that the node ended the upload (#1830).
