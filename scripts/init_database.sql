/*
-------------------------------------------------
Create a database and schemas
-------------------------------------------------
Script purpose:
    This Script creates a new database named 'DataWarehouse'. Additionally, the script sets up three schemas within the databes:
    'bronze', 'silver', 'gold'.
*/


USE master;
GO
CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;
GO
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
