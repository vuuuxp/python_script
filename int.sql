CREATE DATABASE IF NOT EXISTS Cisco ;

CREATE TABLE IF NOT EXISTS Cisco.Employee
(
  emp_no int NOT NULL AUTO_INCREMENT,
  status varchar(45) NOT NULL,
  gender varchar(45) NOT NULL,
  firstname varchar(45) NOT NULL,
  lastname varchar(45) NOT NULL,
  age int(11) NOT NULL,
  emp_hire_date datetime NOT NULL,
  time_inserted datetime NOT NULL,
  CONSTRAINT PRI PRIMARY KEY (emp_no)
);