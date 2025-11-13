CREATE TABLE IF NOT EXISTS fund_positions (
    "FINANCIAL TYPE" TEXT,
    "SYMBOL" TEXT,
    "SECURITY NAME" TEXT,
    "SECURITY IDENTIFIER" TEXT,
    "PRICE" REAL,
    "QUANTITY" REAL,
    "REALISED P/L" REAL,
    "MARKET VALUE" REAL,
    "FUND NAME" TEXT,
    "TIMESTAMP" INTEGER,
    "YEAR" INTEGER,
    "MONTH" INTEGER,
    CONSTRAINT unique_position UNIQUE ("FUND NAME", "YEAR", "MONTH", "SYMBOL")
);
CREATE INDEX idx_fund_name ON fund_positions("FUND NAME");
CREATE INDEX idx_fund_year ON fund_positions("YEAR");
CREATE INDEX idx_fund_month ON fund_positions("MONTH");
