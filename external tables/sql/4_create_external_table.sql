use demodb
CREATE EXTERNAL TABLE employee_ext
    (   Emp_id SMALLINT,
        Name varchar(100),
        Gender varchar(1),
        salary integer,
        Dept_id smallint )
  WITH (    LOCATION = 'dp203synapseprimarycontainer/employee.csv',  
            DATA_SOURCE = nyc_taxi_src1,  
            FILE_FORMAT = csv_file_format,
            REJECT_VALUE = 10
    );

select * from employee_ext