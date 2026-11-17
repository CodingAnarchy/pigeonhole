### Fixed
- `phdb cluster move` and `phdb cluster restore`: a node that keeps its connection up and stops reading an upload no longer hangs the command for ever. After 60 s with nothing taken, a move fails with a message saying to run it again to resume, and a restore resumes the file as it does after a dropped connection (#1950).
