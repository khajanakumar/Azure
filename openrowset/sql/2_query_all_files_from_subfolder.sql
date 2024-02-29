/* code to generate from a particular file in a sub folder */
/* to select all files in a subfolder  */
/* File Name and File Path are also selected */

SELECT
   -- top 100 
   distinct   result.filename() as file_name 
    -- result.filepath(1) as file_path,
    -- result.*
FROM
    OPENROWSET(
        BULK 'https://<storage Account name>.dfs.core.windows.net/dp203synapseprimarycontainer/trip_data_green_csv/year=2021/**',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = True
    ) AS [result]
