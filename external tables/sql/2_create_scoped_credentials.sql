/* this is only for serverless pool */
/* master key is not mandatory but a good practise to create one */
create master key encryption by password = 'xxx'
CREATE DATABASE SCOPED CREDENTIAL democredential
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'xxx';