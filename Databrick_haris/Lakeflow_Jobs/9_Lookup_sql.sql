---LET'S CREATE A TABLE FOR LOOKUP
-- CREATE TABLE databricksharis.bronze.lookup_table 
-- (
--   parent_folder_name STRING,
--   folder_name STRING
-- );

-- insert into databricksharis.bronze.lookup_table  values
-- ('raw', 'customers'),
-- ('raw', 'products'),
-- ('raw', 'stores'),
-- ('raw', 'sales')


select * from databricksharis.bronze.lookup_table
---we'll get a list of dictionaries