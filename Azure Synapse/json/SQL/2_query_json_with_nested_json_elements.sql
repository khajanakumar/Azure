-- Opening a Standard Json File
-- Every Row in the file is in JSON format with nested JSON elements
-- Note Format is "CSV" & Not JSON while Opening the file
-- Do Upload the file 
-- Nested  Elements are displayed as separate rows
SELECT payment_type,sub_type,payment_sub_type_desc
FROM
    OPENROWSET(
        BULK 'https://Storage Account Name>.blob.core.windows.net/dp203synapseprimarycontainer/payment_type_array.json',
        FORMAT = 'CSV',
        PARSER_VERSION = '1.0',
        FIELDQUOTE = '0x0b',
        FIELDTERMINATOR ='0x0b',
        ROWTERMINATOR = '0x0a'
    )
    WITH (
        jsonContent varchar(MAX)    ) AS payment_type
  CROSS APPLY  OPENJSON(jsonContent)
  WITH
  ( payment_type int,
    payment_type_desc nvarchar(max) as json
  )
   CROSS APPLY  OPENJSON(payment_type_desc)
   WITH
   (sub_type SMALLINT,
    payment_sub_type_desc  varchar(50) '$.value'
    )