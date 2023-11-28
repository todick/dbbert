CREATE DATABASE tpch character set utf8mb4;
USE tpch;

CREATE TABLE NATION  ( 
	N_NATIONKEY INTEGER primary key,
    N_NAME       CHAR(25) NOT NULL,
    N_REGIONKEY  INTEGER NOT NULL,
    N_COMMENT    VARCHAR(152));
							
CREATE TABLE REGION  ( 
	R_REGIONKEY INTEGER primary key,
    R_NAME       CHAR(25) NOT NULL,
    R_COMMENT    VARCHAR(152));
							
CREATE TABLE PART  ( 
	P_PARTKEY INTEGER primary key,
    P_NAME        VARCHAR(55) NOT NULL,
    P_MFGR        CHAR(25) NOT NULL,
    P_BRAND       CHAR(10) NOT NULL,
    P_TYPE        VARCHAR(25) NOT NULL,
    P_SIZE        INTEGER NOT NULL,
    P_CONTAINER   CHAR(10) NOT NULL,
    P_RETAILPRICE DECIMAL(15,2) NOT NULL,
    P_COMMENT     VARCHAR(23) NOT NULL );
						  
CREATE TABLE SUPPLIER  ( 
	S_SUPPKEY INTEGER primary key,
    S_NAME        CHAR(25) NOT NULL,
    S_ADDRESS     VARCHAR(40) NOT NULL,
    S_NATIONKEY   INTEGER NOT NULL,
    S_PHONE       CHAR(15) NOT NULL,
    S_ACCTBAL     DECIMAL(15,2) NOT NULL,
    S_COMMENT     VARCHAR(101) NOT NULL);
							 
CREATE TABLE PARTSUPP  ( 
	PS_PARTKEY INTEGER NOT NULL,
    PS_SUPPKEY     INTEGER NOT NULL,
    PS_AVAILQTY    INTEGER NOT NULL,
    PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
    PS_COMMENT     VARCHAR(199) NOT NULL, primary key (PS_PARTKEY, PS_SUPPKEY) );
						
CREATE TABLE CUSTOMER  ( 
	C_CUSTKEY INTEGER primary key,
    C_NAME        VARCHAR(25) NOT NULL,
    C_ADDRESS     VARCHAR(40) NOT NULL,
    C_NATIONKEY   INTEGER NOT NULL,
    C_PHONE       CHAR(15) NOT NULL,
    C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
    C_MKTSEGMENT  CHAR(10) NOT NULL,
    C_COMMENT     VARCHAR(117) NOT NULL);
							 
CREATE TABLE ORDERS  ( 
	O_ORDERKEY INTEGER primary key,
    O_CUSTKEY        INTEGER NOT NULL,
    O_ORDERSTATUS    CHAR(1) NOT NULL,
    O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
    O_ORDERDATE      DATE NOT NULL,
    O_ORDERPRIORITY  CHAR(15) NOT NULL,
    O_CLERK          CHAR(15) NOT NULL,
    O_SHIPPRIORITY   INTEGER NOT NULL,
    O_COMMENT        VARCHAR(79) NOT NULL);
						   
CREATE TABLE LINEITEM ( 
	L_ORDERKEY INTEGER NOT NULL,
    L_PARTKEY     INTEGER NOT NULL,
    L_SUPPKEY     INTEGER NOT NULL,
    L_LINENUMBER  INTEGER NOT NULL,
    L_QUANTITY    DECIMAL(15,2) NOT NULL,
    L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
    L_DISCOUNT    DECIMAL(15,2) NOT NULL,
    L_TAX         DECIMAL(15,2) NOT NULL,
    L_RETURNFLAG  CHAR(1) NOT NULL,
    L_LINESTATUS  CHAR(1) NOT NULL,
    L_SHIPDATE    DATE NOT NULL,
    L_COMMITDATE  DATE NOT NULL,
    L_RECEIPTDATE DATE NOT NULL,
    L_SHIPINSTRUCT CHAR(25) NOT NULL,
    L_SHIPMODE     CHAR(10) NOT NULL,
    L_COMMENT      VARCHAR(44) NOT NULL,
    primary key(L_ORDERKEY,L_LINENUMBER));
	
	
	

SET GLOBAL local_infile = 1;

LOAD DATA LOCAL INFILE 'PATH/nation.tbl' INTO TABLE `NATION` FIELDS TERMINATED BY '|' LINES TERMINATED BY '|\n';
LOAD DATA LOCAL INFILE 'PATH/region.tbl' INTO TABLE `REGION` FIELDS TERMINATED BY '|' LINES TERMINATED BY '|\n';
LOAD DATA LOCAL INFILE 'PATH/part.tbl' INTO TABLE `PART` FIELDS TERMINATED BY '|' LINES TERMINATED BY '|\n';
LOAD DATA LOCAL INFILE 'PATH/supplier.tbl' INTO TABLE `SUPPLIER` FIELDS TERMINATED BY '|' LINES TERMINATED BY '|\n'; 
LOAD DATA LOCAL INFILE 'PATH/partsupp.tbl' INTO TABLE `PARTSUPP` FIELDS TERMINATED BY '|' LINES TERMINATED BY '|\n';	
LOAD DATA LOCAL INFILE 'PATH/customer.tbl' INTO TABLE `CUSTOMER` FIELDS TERMINATED BY '|' LINES TERMINATED BY '|\n';
LOAD DATA LOCAL INFILE 'PATH/orders.tbl' INTO TABLE `ORDERS` FIELDS TERMINATED BY '|' LINES TERMINATED BY '|\n'; 
LOAD DATA LOCAL INFILE 'PATH/lineitem.tbl' INTO TABLE `LINEITEM` FIELDS TERMINATED BY '|' LINES TERMINATED BY '|\n'; 


ALTER TABLE REGION ADD PRIMARY KEY IF NOT EXISTS(R_REGIONKEY);
ALTER TABLE NATION ADD PRIMARY KEY IF NOT EXISTS (N_NATIONKEY);
ALTER TABLE NATION ADD FOREIGN KEY IF NOT EXISTS NATION_FK1 (N_REGIONKEY) references REGION(R_REGIONKEY);
ALTER TABLE PART   ADD PRIMARY KEY IF NOT EXISTS(P_PARTKEY);
ALTER TABLE SUPPLIER ADD PRIMARY KEY IF NOT EXISTS (S_SUPPKEY);
ALTER TABLE SUPPLIER ADD FOREIGN KEY IF NOT EXISTS SUPPLIER_FK1 (S_NATIONKEY) references NATION(N_NATIONKEY);
ALTER TABLE PARTSUPP ADD PRIMARY KEY IF NOT EXISTS (PS_PARTKEY,PS_SUPPKEY);
ALTER TABLE CUSTOMER ADD PRIMARY KEY IF NOT EXISTS (C_CUSTKEY);
ALTER TABLE CUSTOMER ADD FOREIGN KEY IF NOT EXISTS CUSTOMER_FK1 (C_NATIONKEY) references NATION(N_NATIONKEY);
ALTER TABLE LINEITEM ADD PRIMARY KEY IF NOT EXISTS (L_ORDERKEY,L_LINENUMBER);
ALTER TABLE PARTSUPP ADD FOREIGN KEY IF NOT EXISTS PARTSUPP_FK1 (PS_SUPPKEY) references SUPPLIER(S_SUPPKEY);
ALTER TABLE PARTSUPP ADD FOREIGN KEY IF NOT EXISTS PARTSUPP_FK2 (PS_PARTKEY) references PART(P_PARTKEY);
ALTER TABLE ORDERS   ADD FOREIGN KEY IF NOT EXISTS ORDERS_FK1 (O_CUSTKEY) references CUSTOMER(C_CUSTKEY);
ALTER TABLE LINEITEM ADD FOREIGN KEY IF NOT EXISTS LINEITEM_FK1 (L_ORDERKEY)  references ORDERS(O_ORDERKEY);
ALTER TABLE LINEITEM ADD FOREIGN KEY IF NOT EXISTS LINEITEM_FK2 (L_PARTKEY,L_SUPPKEY) references PARTSUPP(PS_PARTKEY, PS_SUPPKEY);





