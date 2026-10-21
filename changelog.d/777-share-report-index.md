### Fixed
- A flush on a store running many cross-shard commits no longer scans every unreported share on its shard thread to find the flushed ones (17-21 ms with ~100K): the shares are indexed by the slots they write, so a report touches only the shares it reports (#777).
