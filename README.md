# Assumptions
- External funds drop the data file in a fixed, shared directory on a periodic basis.
- The column ordering is the same for every files, although some funds, like Magnum, use SEDOL as column name instead of ISIN.
- If the source schema does change, it would change for every fund.
- Each source file should only be ingested once.