create or replace function gets3ParentDailyFileList()
returns variant
as 
$$
    select metadata:fileName from s3_metadata
$$