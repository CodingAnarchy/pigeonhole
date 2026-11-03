### Changed
- A nearest-neighbour search or any other reduction reads a range of a family that sits in one SST, with no deletes or merge operands, straight from its blocks, skipping the merge and the per-cell MVCC resolution (#1283). On Linux callgrind an exact nearest search scores a vector in about 30% fewer instructions. Results are unchanged.
