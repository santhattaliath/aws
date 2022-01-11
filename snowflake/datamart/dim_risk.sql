CREATE OR REPLACE PROCEDURE "integralba"."DATAMART".DIM_RISK_PR()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  try{
    let command = `MERGE INTO "integralba"."DATAMART".DIM_RISK RISK
    USING(
    WITH STAGE_DATA AS(
SELECT UNIQUE_NUMBER AS "Risk Key", RSKPFX, RSKCOY, CHDRNO, RSKNO, VFLAG, CNTTYP, RSKTYP AS "Risk Type Code", TRANNO, DTEEFF, DTEATT, DTETER, SICURR, SIRAT, 
  TOTSI, TOTSIL, TOTPRE,  DATIME, CONCAT('T4677|',RSKTYP) AS "Risk Type Key", CONCAT(RSKCOY,'|',CHDRNO,'|', cast(RSKNO as varchar(10))) AS "Risk Durable Key",
CONCAT(RSKPFX,'|',RSKCOY,'|',CHDRNO,'|',cast(RSKNO as varchar(10)),'|',cast(DTEEFF as varchar(10)),'|',cast(TRANNO as varchar(10)),'|1') AS "Risk RI Retention Key", 
CONCAT(RSKCOY,'|',CHDRNO) AS "Policy Durable Key",CONCAT(ltrim(rtrim(RSKPFX)),'|',ltrim(rtrim(RSKCOY)),'|',ltrim(rtrim(CHDRNO)),'|',cast(RSKNO as varchar(10)),'|',cast(DTEEFF as varchar(10))) AS "Risk Proofing Key" 
from "integralba"."integral_stage"."RISKPF_STAGE" WHERE RSKCOY ='1')
SELECT T.*,"Risk Type".description "Risk Type Description",
CASE WHEN RANK()OVER(PARTITION BY "Risk Durable Key" ORDER BY "Risk Key")=1 THEN 'Y' ELSE 'N' END AS "Show Latest Risk Key"
FROM STAGE_DATA T JOIN TABLE("integralba"."integral_stage".descpLookupFn(T."Risk Type Key")) "Risk Type") SOURCE 
    ON(RISK."Risk Key" = SOURCE."Risk Key")
    WHEN MATCHED THEN  
        UPDATE SET DATIME = CURRENT_DATE
    WHEN NOT MATCHED THEN 
        INSERT("Risk Key", RSKPFX, RSKCOY, CHDRNO, RSKNO, VFLAG, CNTTYP,"Risk Type Code", TRANNO, DTEEFF, DTEATT, DTETER, SICURR, SIRAT, 
TOTSI, TOTSIL, TOTPRE,  DATIME, "Risk Type Key", "Risk Durable Key", "Risk RI Retention Key", "Policy Durable Key","Risk Proofing Key",
"Risk Type Description","Show Latest Risk Key") VALUES(SOURCE."Risk Key", SOURCE.RSKPFX, SOURCE.RSKCOY, SOURCE.CHDRNO, SOURCE.RSKNO, SOURCE.VFLAG, SOURCE.CNTTYP,"Risk Type Code", SOURCE.TRANNO, SOURCE.DTEEFF, 
SOURCE.DTEATT, SOURCE.DTETER, SOURCE.SICURR, SOURCE.SIRAT,SOURCE.TOTSI, SOURCE.TOTSIL, SOURCE.TOTPRE,  SOURCE.DATIME, SOURCE."Risk Type Key", 
SOURCE."Risk Durable Key", SOURCE."Risk RI Retention Key", SOURCE."Policy Durable Key",SOURCE."Risk Proofing Key",SOURCE."Risk Type Description",SOURCE."Show Latest Risk Key")`;
    let preparedStatement = snowflake.createStatement({sqlText:command});
    let results = preparedStatement.execute();
    return results.getNumRowsAffected(); //log the rowcount
    
    }catch(error){
        return error
    };
$$