-- Opening a Classic Json File
-- Every Row in the file is in JSON format with nested JSON elements
-- Note Format is "CSV" & Not JSON while Opening the file
-- Note the ROWTERMINATOR is =  '0x0b' 
-- Nested  Elements are displayed as separate rows
SELECT rate_code_id,rate_code
FROM
    OPENROWSET(
        BULK 'https://<storage Account Name>.blob.core.windows.net/dp203synapseprimarycontainer/rate_code.json',
        FORMAT = 'CSV',
        PARSER_VERSION = '1.0',
        FIELDQUOTE = '0x0b',
        FIELDTERMINATOR ='0x0b',
        ROWTERMINATOR = '0x0b'
    )
    WITH (
        jsonContent varchar(MAX)    ) AS rate_code
  CROSS APPLY  OPENJSON(jsonContent)
  WITH
  ( rate_code_id int,
    rate_code    varchar(100)
  )
