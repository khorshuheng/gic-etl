CREATE TABLE IF NOT EXISTS fund_positions (
    financial_type TEXT,
    symbol TEXT,
    security_name TEXT,
    security_identifier TEXT,
    price REAL,
    quantity INTEGER,
    realised_pl REAL,
    market_value REAL,
    ingestion_date DATE
);
CREATE INDEX idx_ingestion_date ON fund_positions(ingestion_date);
