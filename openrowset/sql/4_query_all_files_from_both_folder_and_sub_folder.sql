/* Read from all the files and all the subflders sub folders */

SELECT
    result.filename() as file_name,
    result.filepath(1) as file_path,
    result.filepath(2) as file_path,
    count(1)
FROM
    OPENROWSET(
        BULK 'https://<storage Account name>.dfs.core.windows.net/dp203synapseprimarycontainer/trip_data_green_csv/year=*/month=*/*.csv',           
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = True
    ) AS [result]
group by result.filename(),result.filepath(1),result.filepath(2)
