CREATE OR REPLACE PROCEDURE INTEGRAL_STAGE(start_date date,end_date date,incremental_flag string)
  RETURNS STRING 
  LANGUAGE JAVASCRIPT
  AS
  $$
    try{
        //read the metadata from json      
        
        let command = "SELECT gets3ParentDailyFolder() FROM DUAL"
        let preparedStatement = snowflake.createStatement({sqlText: command});
        let results = preparedStatement.execute();
        results.next()
        const integrals3ParentDailyFolder = results.getColumnValue(1);
        
        command = "SELECT gets3ParentDailyFileList() FROM DUAL"
        preparedStatement = snowflake.createStatement({sqlText: command});
        results = preparedStatement.execute();
        results.next()
        const integrals3DailyFiles = results.getColumnValue(1);       
        
        //loop through daily list and stage the data
         
           for(const fileName of integrals3DailyFiles){
                try{
                    let tableName = fileName.split('_')[0].concat('_stage');     
                    command = `COPY INTO ${tableName} FROM @INTEGRAL_STAGE_S3/${integrals3ParentDailyFolder}/${fileName} file_format = (format_name = integral_stagefile_format) on_error=continue`;
                    preparedStatement = snowflake.createStatement({sqlText:command});
                    results = preparedStatement.execute();                    
                                        
                }catch(error){    
                    //log the error for the table in the error log table
                    return error
                }finally{
                    console.log('finishing tasks')
                };
            
            };
    }catch(error){
        return error;    
    };
    
  $$;