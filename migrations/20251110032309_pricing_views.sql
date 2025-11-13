CREATE VIEW instrument_pricing AS
WITH all_instruments AS (
    SELECT
        'Government Bond' AS "FINANCIAL TYPE",
        "ISIN" AS "INSTRUMENT ID",
        "PRICE",
        UNIXEPOCH("DATETIME") AS "TIMESTAMP"
    FROM bond_prices
    UNION ALL
    SELECT
        'Equities' AS "FINANCIAL TYPE",
        "SYMBOL" AS "INSTRUMENT ID",
        "PRICE",
        UNIXEPOCH(SUBSTR("DATETIME", LENGTH("DATETIME") - 3, 4) || '-' ||
                  CASE INSTR("DATETIME", '/')
                      WHEN 2 THEN PRINTF('%02d', SUBSTR("DATETIME", 1, 1))
                      ELSE SUBSTR("DATETIME", 1, 2)
                      END || '-' ||
                  PRINTF(
                          '%02d',
                          SUBSTR(
                                  "DATETIME",
                                  INSTR("DATETIME", '/') +1,
                                  LENGTH("DATETIME") - 5 - INSTR("DATETIME", '/')
                          )
                  )) AS "TIMESTAMP"
    FROM equity_prices
)

SELECT
    *,
    CAST(STRFTIME('%d', "TIMESTAMP", 'unixepoch') AS INTEGER) AS "DAY",
    CAST(STRFTIME('%m', "TIMESTAMP", 'unixepoch') AS INTEGER) AS "MONTH",
    CAST(STRFTIME('%Y', "TIMESTAMP", 'unixepoch') AS INTEGER) AS "YEAR"
FROM all_instruments