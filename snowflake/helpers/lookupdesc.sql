create or replace function descpLookupFn(lookupKey string)
returns TABLE (description VARCHAR)
AS 'SELECT LONGDESC FROM \"integralba\".\"integral_stage\".\"DESCPF_STAGE\" WHERE CONCAT(DESCTABL,\'|\',DESCITEM) = lookupKey AND LANGUAGE =\'E\' AND DESCPFX = \'IT\' AND DESCCOY=\'1\''
 