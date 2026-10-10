### Added
- `pigeonhole-sst` writes through a direct-I/O handle too (#403): `SstWriter` writes whole pages and pads its last page and footer within the extent (the SST's length and layout are unchanged), and `BlobWriter` writes records through aligned pages.
