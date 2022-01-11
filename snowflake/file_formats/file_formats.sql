create or replace file format integral_stagefile_format
TYPE=CSV 
COMPRESSION=AUTO 
FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
EMPTY_FIELD_AS_NULL=TRUE 
NULL_IF='NULL' 
RECORD_DELIMITER='\r\r\n' 
DATE_FORMAT='DD/MM/YYYY' 
TRIM_SPACE=TRUE 
SKIP_HEADER=1;


create or replace file format integral_masterfile_format
TYPE=CSV 
COMPRESSION=AUTO 
FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
EMPTY_FIELD_AS_NULL=TRUE 
NULL_IF='NULL' 
RECORD_DELIMITER='\r\n' 
DATE_FORMAT='DD/MM/YYYY' 
TRIM_SPACE=TRUE 
SKIP_HEADER=1;