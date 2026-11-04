SQL INSERT Operations & Constraints Demo

-- ===================================================== -- 1. CREATE DATABASE / SCHEMA -- =====================================================

CREATE SCHEMA IF NOT EXISTS COMPANY_DB;

USE COMPANY_DB;

-- ===================================================== -- 2. CREATE DEPARTMENT TABLE -- =====================================================

DROP TABLE IF EXISTS EMPLOYEE; DROP TABLE IF EXISTS DEPARTMENT;

CREATE TABLE DEPARTMENT ( DEPT_ID INT PRIMARY KEY, DEPT_NAME VARCHAR(30) NOT NULL UNIQUE, LOCATION VARCHAR(30) DEFAULT 'HYDERABAD' );

-- ===================================================== -- 3. INSERT SAMPLE RECORDS INTO DEPARTMENT -- =====================================================

INSERT INTO DEPARTMENT (DEPT_ID, DEPT_NAME, LOCATION) VALUES (10, 'DEPT_A', 'LOCATION_A');

INSERT INTO DEPARTMENT (DEPT_ID, DEPT_NAME, LOCATION) VALUES (20, 'DEPT_B', 'LOCATION_B');

INSERT INTO DEPARTMENT (DEPT_ID, DEPT_NAME, LOCATION) VALUES (30, 'DEPT_C', 'LOCATION_C');

INSERT INTO DEPARTMENT (DEPT_ID, DEPT_NAME, LOCATION) VALUES (40, 'DEPT_D', 'LOCATION_D');

INSERT INTO DEPARTMENT (DEPT_ID, DEPT_NAME, LOCATION) VALUES (50, 'DEPT_E', 'LOCATION_E');

-- ===================================================== -- 4. CREATE EMPLOYEE TABLE -- =====================================================

CREATE TABLE EMPLOYEE ( EMP_ID INT PRIMARY KEY, EMP_NAME VARCHAR(30) NOT NULL, EMAIL VARCHAR(50) UNIQUE, AGE INT CHECK (AGE >= 18), SALARY INT CHECK (SALARY > 0), GENDER VARCHAR(10) CHECK (GENDER IN ('MALE', 'FEMALE')), DEPT_ID INT, MANAGER_ID INT, JOIN_DATE DATE DEFAULT (CURRENT_DATE),

CONSTRAINT FK_EMP_DEPT
FOREIGN KEY (DEPT_ID)
REFERENCES DEPARTMENT(DEPT_ID),

CONSTRAINT FK_EMP_MANAGER
FOREIGN KEY (MANAGER_ID)
REFERENCES EMPLOYEE(EMP_ID)


);

-- ===================================================== -- 5. INSERT SAMPLE RECORDS INTO EMPLOYEE -- =====================================================

INSERT INTO EMPLOYEE VALUES (101, 'EMP_A', 'emp_a@example.com', 25, 35000, 'MALE', 20, NULL, DATE '2024-01-10');

INSERT INTO EMPLOYEE VALUES (102, 'EMP_B', 'emp_b@example.com', 28, 45000, 'FEMALE', 10, 101, DATE '2023-05-15');

INSERT INTO EMPLOYEE VALUES (103, 'EMP_C', 'emp_c@example.com', 30, 55000, 'MALE', 20, 101, DATE '2022-08-20');

INSERT INTO EMPLOYEE VALUES (104, 'EMP_D', 'emp_d@example.com', 24, 30000, 'FEMALE', 30, 103, DATE '2024-03-12');

INSERT INTO EMPLOYEE VALUES (105, 'EMP_E', 'emp_e@example.com', 35, 65000, 'MALE', 40, 101, DATE '2021-06-01');

INSERT INTO EMPLOYEE VALUES (106, 'EMP_F', 'emp_f@example.com', 27, 40000, 'FEMALE', 50, 105, DATE '2023-11-05');

INSERT INTO EMPLOYEE VALUES (107, 'EMP_G', 'emp_g@example.com', 32, 50000, 'MALE', 30, 103, DATE '2022-02-18');

INSERT INTO EMPLOYEE VALUES (108, 'EMP_H', 'emp_h@example.com', 29, 48000, 'FEMALE', 10, 102, DATE '2023-07-25');

INSERT INTO EMPLOYEE VALUES (109, 'EMP_I', 'emp_i@example.com', 26, 38000, 'MALE', 40, 105, DATE '2024-04-10');

INSERT INTO EMPLOYEE VALUES (110, 'EMP_J', 'emp_j@example.com', 31, 60000, 'FEMALE', 50, 106, DATE '2021-09-15');

-- ===================================================== -- 6. VERIFY INSERTED RECORDS -- =====================================================

SELECT * FROM DEPARTMENT;

SELECT * FROM EMPLOYEE;

-- ===================================================== -- 7. INVALID INSERT STATEMENTS -- These statements are expected to fail. -- =====================================================

-- 7.1 PRIMARY KEY VIOLATION -- Duplicate EMP_ID: 101 already exists.

INSERT INTO EMPLOYEE VALUES (101, 'EMP_K', 'emp_k@example.com', 25, 35000, 'MALE', 20, NULL, CURRENT_DATE);

-- 7.2 NOT NULL CONSTRAINT VIOLATION -- EMP_NAME cannot contain NULL.

INSERT INTO EMPLOYEE VALUES (111, NULL, 'emp_l@example.com', 25, 30000, 'MALE', 20, 101, CURRENT_DATE);

-- 7.3 UNIQUE CONSTRAINT VIOLATION -- Duplicate EMAIL: emp_a@example.com already exists.

INSERT INTO EMPLOYEE VALUES (111, 'EMP_L', 'emp_a@example.com', 25, 30000, 'MALE', 20, 101, CURRENT_DATE);

-- 7.4 CHECK CONSTRAINT VIOLATION - AGE -- AGE must be greater than or equal to 18.

INSERT INTO EMPLOYEE VALUES (112, 'EMP_M', 'emp_m@example.com', 15, 30000, 'MALE', 20, 101, CURRENT_DATE);

-- 7.5 CHECK CONSTRAINT VIOLATION - SALARY -- SALARY must be greater than zero.

INSERT INTO EMPLOYEE VALUES (113, 'EMP_N', 'emp_n@example.com', 25, -5000, 'MALE', 20, 101, CURRENT_DATE);

-- 7.6 CHECK CONSTRAINT VIOLATION - GENDER -- GENDER must be either MALE or FEMALE.

INSERT INTO EMPLOYEE VALUES (114, 'EMP_O', 'emp_o@example.com', 25, 30000, 'OTHER', 20, 101, CURRENT_DATE);

-- 7.7 FOREIGN KEY CONSTRAINT VIOLATION - DEPT_ID -- Department ID 99 does not exist in DEPARTMENT.

INSERT INTO EMPLOYEE VALUES (115, 'EMP_P', 'emp_p@example.com', 25, 30000, 'MALE', 99, 101, CURRENT_DATE);

-- 7.8 FOREIGN KEY CONSTRAINT VIOLATION - MANAGER_ID -- Manager ID 999 does not exist in EMPLOYEE.

INSERT INTO EMPLOYEE VALUES (116, 'EMP_Q', 'emp_q@example.com', 25, 30000, 'MALE', 20, 999, CURRENT_DATE);

-- ===================================================== -- 8. DEFAULT CONSTRAINT DEMONSTRATION - LOCATION -- =====================================================

-- LOCATION is not provided. -- The default value HYDERABAD should be inserted.

INSERT INTO DEPARTMENT (DEPT_ID, DEPT_NAME) VALUES (60, 'DEPT_F');

SELECT * FROM DEPARTMENT WHERE DEPT_ID = 60;

-- ===================================================== -- 9. FOREIGN KEY CONSTRAINT VIOLATION - DELETE PARENT -- =====================================================

-- Department ID 20 is referenced by employee records. -- Deleting this department should fail because of the -- FOREIGN KEY constraint (without cascading deletion).

DELETE FROM DEPARTMENT WHERE DEPT_ID = 20;

-- ===================================================== -- 10. FINAL DATA VERIFICATION -- =====================================================

SELECT * FROM DEPARTMENT;

SELECT * FROM EMPLOYEE;
