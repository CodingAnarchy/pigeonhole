### Fixed
- **Cluster: the decision sweep's log line counts records whose asks ran out of time** (#2164's follow-up). A sweep round that timed out with records still being asked now counts their branches as unanswered and says how many records ran out of time; before, it said nothing unless an answered record was refused.
