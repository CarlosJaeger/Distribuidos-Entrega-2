import pg from 'pg';
const { Pool } = pg;

export const pool = new Pool({
  user: 'user',
  host: 'localhost',
  database: 'yahoo_dataset',
  password: '1234',
  port: 5432
});
