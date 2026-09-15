CREATE CATALOG IF NOT EXISTS concert_rec;
CREATE SCHEMA IF NOT EXISTS concert_rec.bronze;
CREATE SCHEMA IF NOT EXISTS concert_rec.silver;
CREATE SCHEMA IF NOT EXISTS concert_rec.features;
CREATE SCHEMA IF NOT EXISTS concert_rec.gold;

-- Streaming jobs create Delta tables on first write. This script intentionally
-- creates only the namespaces, so schemas can evolve from the Scala contracts.
