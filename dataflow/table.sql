CREATE TABLE IF NOT EXISTS go_txn (
    TransactionDate TIMESTAMP NOT NULL,
    SequenceNumber INT NOT NULL,
    ServiceProvider VARCHAR(50) NOT NULL,
    Location VARCHAR(100),
    Type VARCHAR(50),
    Service VARCHAR(50),
    Discount FLOAT,
    Amount FLOAT,
    Balance FLOAT
);
