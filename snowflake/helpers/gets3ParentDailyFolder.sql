create or replace function gets3ParentDailyFolder()
returns variant
as 
$$
    select metadata:folderName from s3_metadata
$$