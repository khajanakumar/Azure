-- Opening a Standard Json File
-- Every Row in the file is in JSON format
-- Note Format is "CSV" & Not JSON while Opening the file
-- Do Upload the file 
SELECT payment_type,payment_type_desc
FROM
    OPENROWSET(
        BULK 'https://<accountname>.dfs.core.windows.net/dp203synapseprimarycontainer/payment_type.json',
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
    payment_type_desc varchar(50)
  )
