/* select files from specific sub folder */

SELECT
    distinct result.filename() as file_name
FROM
    OPENROWSET(
        BULK ( 'https://dp203synapsecoursedl.dfs.core.windows.net/dp203synapseprimarycontainer/trip_data_green_csv/year=2021/month=01/*',
                'https://dp203synapsecoursedl.dfs.core.windows.net/dp203synapseprimarycontainer/trip_data_green_csv/year=2021/month=02/*'
            ),
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = True
    ) AS [result]