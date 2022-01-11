CREATE OR REPLACE PROCEDURE "integralba"."DATAMART".DIM_POLICY_PR()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  try{
    let command = `MERGE INTO "integralba"."DATAMART".DIM_POLICY POLICY
    USING(
    WITH STAGE_DATA AS (SELECT TOP 100
    UNIQUE_NUMBER, CHDRCOY, CHDRNUM, CURRFROM, CURRTO, VALIDFLAG, CNTTYPE, CNTBRANCH, CNTCURR, STCC, STCA, STCD,STCE,TRANNO, STATCODE, CCDATE, CRDATE, COWNCOY, 
    COWNNUM, AGNTCOY, AGNTNUM, SRCEBUS,BILLFREQ, BILLCHNL, ZENDNO , ZRENNO, PAYRNUM, MANDREF, DATIME,USRPRF, STATREASN, DTECAN, 
    TO_DATE(CAST(CASE WHEN CRDATE=99999999 or CRDATE=00000000 OR CRDATE IS NULL THEN 90001231 ELSE CRDATE END AS VARCHAR),'YYYYMMDD') AS "Contract Renewal Date", 
    TO_DATE(CAST(CASE WHEN CCDATE=99999999 or CCDATE=00000000 OR CCDATE IS NULL THEN 90001231 ELSE CCDATE END AS VARCHAR),'YYYYMMDD') AS "Contract Commencement Date", 
    TO_DATE(CAST(CASE WHEN CRDATE=99999999 or CRDATE=00000000 OR CRDATE IS NULL THEN 90001231 ELSE CRDATE END AS VARCHAR),'YYYYMMDD') AS "Contract Expiry Date", 
    TO_DATE(CAST(CASE WHEN OCCDATE=99999999 or OCCDATE=00000000 OR OCCDATE IS NULL THEN 90001231 ELSE OCCDATE END AS VARCHAR),'YYYYMMDD') AS "Original Commencement Date",
    CONCAT(CHDRCOY,'|',CHDRNUM) AS "Policy Durable Key",
    CONCAT(CHDRPFX,'|',CHDRCOY,'|',CHDRNUM,'|',CAST(TRANNO as VARCHAR(10))) AS "Policy Trans Key", 
    CONCAT(COWNCOY,'|',COWNNUM) AS "Policy Owner Key",
    CONCAT('T1692|',CNTBRANCH) AS "Policy Branch Key", 
    CONCAT('T3597|',STCC) AS "Major Class Key",
    CONCAT('T3681|',CNTTYPE) AS "Contract Type Key",  
    CONCAT(AGNTCOY,'|',AGNTNUM) AS "Agent Key",
    CONCAT('T3617|',STATREASN) AS "Reason Code Key", 
    CONCAT('T3595|',ltrim(rtrim(STCA))) AS "Fund Key", 
    CONCAT('T3620|',ltrim(rtrim(BILLCHNL))) AS "Billing Channel Key", 
    CONCAT('T3692|',ltrim(rtrim(SRCEBUS))) AS "Source of Business Key",
    CONCAT(ltrim(rtrim(COWNCOY)),'|',rtrim(ltrim(CASE WHEN ltrim(rtrim(PAYRNUM)) = ''  THEN COWNNUM  ELSE PAYRNUM END)),'|',ltrim(rtrim(MANDREF))) AS "Mandate Owner Key", 
    CASE  WHEN STATCODE = 'IF' THEN CONCAT(CHDRCOY,'|',CHDRNUM,'|',CAST(CCDATE AS VARCHAR(10)),'|',CAST(TRANNO AS VARCHAR(10)))  ELSE NULL END AS "Policy Installment Key", 
    CASE WHEN ltrim(rtrim (PAYRNUM)) = ''  THEN COWNNUM  ELSE PAYRNUM END AS "Payor Number", 
    TO_DATE(CAST(CASE WHEN CURRFROM=99999999 or CURRFROM=00000000 OR CURRFROM IS NULL THEN 90001231 ELSE OCCDATE END AS VARCHAR),'YYYYMMDD') AS "Row Effective Date", 
    CONCAT(CHDRNUM,'|',CAST(CURRTO AS VARCHAR(8)),'|',CAST(TRANNO AS VARCHAR(10))) AS "Cancellation Key",
    CONCAT('T3599|',ltrim(rtrim(STCE))) AS "Distribution Channel Key",COTYPE,
    COPPN 
    --(CONCAT(CHDRNUM,'-', RIGHT(REPEAT('0', 8)) , LEFT(CURRTO, 8), 8),'-',RIGHT(REPEAT('0', 5) , LEFT(TRANNO, 5), 5))) AS "MAX_TRAN" 
    FROM "integralba"."integral_stage".CHDRPF_STAGE ),
    DESCPF AS (SELECT CONCAT(DESCTABL,'|',DESCITEM) "DESCPF_KEY",
                    LONGDESC AS "Description"
               FROM "integralba"."integral_stage".DESCPF_STAGE 
              WHERE DESCPFX='IT' AND LANGUAGE='E' AND DESCCOY='1')          
    SELECT T.*,
    CASE WHEN RANK() OVER(PARTITION BY T."Policy Durable Key" ORDER BY T.UNIQUE_NUMBER) =1 
    THEN 'Y' ELSE 'N' END AS "Show Latest Policy Information" ,
    "Major Class".description AS "Major Class Description",
    "Fund".description AS "Fund Description",
    DATEDIFF(month,T."Contract Renewal Date", CURRENT_DATE) "RENEWALS DUE"
    FROM STAGE_DATA T JOIN TABLE("integralba"."integral_stage".descpLookupFn(T."Major Class Key")) "Major Class"
    JOIN TABLE("integralba"."integral_stage".descpLookupFn(T."Fund Key")) "Fund") SOURCE 
    ON(POLICY.UNIQUE_NUMBER = SOURCE.UNIQUE_NUMBER)
    WHEN MATCHED THEN  
        UPDATE SET DATIME = CURRENT_DATE
    WHEN NOT MATCHED THEN 
        INSERT (UNIQUE_NUMBER,CHDRCOY, CHDRNUM, CURRFROM, CURRTO, VALIDFLAG, CNTTYPE, CNTBRANCH, CNTCURR, STCC,STCA, STCD,STCE,TRANNO, STATCODE,CCDATE,CRDATE,COWNCOY,COWNNUM, AGNTCOY, AGNTNUM, SRCEBUS,BILLFREQ, BILLCHNL, ZENDNO , ZRENNO, PAYRNUM, 
        MANDREF, DATIME,USRPRF, STATREASN,DTECAN,"Contract Commencement Date","Contract Expiry Date","Original Commencement Date","Policy Durable Key",
    "Policy Trans Key","Policy Owner Key","Policy Branch Key","Major Class Key","Contract Type Key","Agent Key","Reason Code Key","Fund Key", 
    "Billing Channel Key","Source of Business Key","Mandate Owner Key","Policy Installment Key","Payor Number","Row Effective Date","Cancellation Key",
    "Distribution Channel Key",COTYPE,COPPN ) VALUES(SOURCE.UNIQUE_NUMBER,SOURCE.CHDRCOY, SOURCE.CHDRNUM, SOURCE.CURRFROM, SOURCE.CURRTO, SOURCE.VALIDFLAG, SOURCE.CNTTYPE, SOURCE.CNTBRANCH, SOURCE.CNTCURR, SOURCE.STCC, 
        SOURCE.STCA, SOURCE.STCD,SOURCE.STCE,SOURCE.TRANNO, SOURCE.STATCODE,SOURCE.CCDATE,SOURCE.CRDATE,SOURCE.COWNCOY,SOURCE.COWNNUM, SOURCE.AGNTCOY, SOURCE.AGNTNUM, SOURCE.SRCEBUS,SOURCE.BILLFREQ, SOURCE.BILLCHNL, SOURCE.ZENDNO , SOURCE.ZRENNO, SOURCE.PAYRNUM, 
        SOURCE.MANDREF, SOURCE.DATIME,SOURCE.USRPRF, SOURCE.STATREASN,SOURCE.DTECAN,SOURCE."Contract Commencement Date",SOURCE."Contract Expiry Date",SOURCE."Original Commencement Date",SOURCE."Policy Durable Key",
    SOURCE."Policy Trans Key",SOURCE."Policy Owner Key",SOURCE."Policy Branch Key","Major Class Key",SOURCE."Contract Type Key",SOURCE."Agent Key",SOURCE."Reason Code Key",SOURCE."Fund Key", 
    SOURCE."Billing Channel Key",SOURCE."Source of Business Key",SOURCE."Mandate Owner Key",SOURCE."Policy Installment Key",SOURCE."Payor Number",SOURCE."Row Effective Date",SOURCE."Cancellation Key",
    SOURCE."Distribution Channel Key",SOURCE.COTYPE,SOURCE.COPPN )`;
    let preparedStatement = snowflake.createStatement({sqlText:command});
    let results = preparedStatement.execute();
    return results.getNumRowsAffected(); //log the rowcount
    
    }catch(error){
        return error
    };
$$
--drop procedure "integralba"."DATAMART".DIM_POLICY_PR()
call "integralba"."DATAMART".DIM_POLICY_PR()

