IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'demodb')
    CREATE EXTERNAL DATA SOURCE nyc_taxi_src1
    WITH
    (    LOCATION         = 'https://<storageaccountname>.blob.core.windows.net',
         CREDENTIAL       = democredential
    )