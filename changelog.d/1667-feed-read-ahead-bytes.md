### Fixed
- A change-feed reader's read-ahead budget (64 MiB) now counts the memory its kept records really take; they used to take up to about three times the count. Kept records are also packed tighter, so a reader far behind uses about half the memory it did (#1637).
