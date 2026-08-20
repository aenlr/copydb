-- The lite version of the free database doesn't come with
-- a USERS tablespace.
--
-- The SYSTEM tablespace is default and it doesn't have ASSM
-- enabled which is required for JSON columns.
-- ASSM is enabled by default but is included for clarity.
WHENEVER OSERROR EXIT 1
WHENEVER SQLERROR EXIT 1

DEFINE DATAFILE=&&1

-- Create the tablespace
CREATE TABLESPACE USERS
DATAFILE '&DATAFILE' SIZE 10M
AUTOEXTEND ON NEXT 50M MAXSIZE 500M
EXTENT MANAGEMENT LOCAL
SEGMENT SPACE MANAGEMENT AUTO
;

-- Change the default
ALTER DATABASE DEFAULT TABLESPACE USERS;

-- Can be checked with:
--  SELECT PROPERTY_NAME, PROPERTY_VALUE FROM DATABASE_PROPERTIES WHERE PROPERTY_NAME IN ('DEFAULT_PERMANENT_TABLESPACE','DEFAULT_TEMP_TABLESPACE');
