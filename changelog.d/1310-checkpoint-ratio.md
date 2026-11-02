### Changed
- Reads keep a folded aggregate value for later reads when the operands they folded add up to at least a quarter of the value's size, beyond the existing step count. One rule now covers every aggregate kind, in place of the 4 KiB limit (and the 256 KiB limit for sets). The value the fold starts from doesn't count toward the quarter (D298 §4.3, amended).
