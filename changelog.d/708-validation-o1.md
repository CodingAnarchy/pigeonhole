### Changed
- Transactions and conditional writes that read a hot row validate faster: validation reads each column's newest version only, instead of every buffered version, wherever every version there carries a default timestamp (D273, #708). A memtable holding an explicitly timestamped cell, or rebuilt by recovery, is still checked in full.
