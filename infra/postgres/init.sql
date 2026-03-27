-- Create additional databases needed by the stack
-- This runs on first Postgres startup only

-- Prefect server database
CREATE DATABASE prefect;

-- Grant permissions to the main user
GRANT ALL PRIVILEGES ON DATABASE prefect TO current_user;
