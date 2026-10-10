### Changed
- A scan unescapes each row key in one copy when it is 16 bytes or longer and has no escape, and checks whether a source is still on the current row by prefix. Writes escape their keys with a word-at-a-time search for zero bytes. A time-series newest-ten scan with 28-byte row keys costs 13% fewer instructions, and a timestamped commit 1% fewer (#406).
