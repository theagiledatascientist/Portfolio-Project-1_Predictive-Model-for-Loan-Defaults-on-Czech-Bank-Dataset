SET GLOBAL local_infile = 'ON';
SET GLOBAL innodb_buffer_pool_size=1024*1024*1024;
-- drop database if exists bank_loan;
create database bank_loan;
use bank_loan;
-- -----------------------------
# Create tables and load data
-- -----------------------------
# my notes
-- we will set PKs and FKs later if we need

# Create account table and import data
drop table if exists account;

create table account (
account_id int,
district_id int,
frequency varchar(50),
acc_dt date
);

## some observations on data format in input data
    -- order of input columns is "account_id";"district_id";"frequency";"date"
    -- only frequency enclosed by " quotes
    -- No values are in scientific notation (like 1E+15)
    -- searched for ;; two semicolons together: no nulls from initial search
    -- date is in 930108 yymmdd format
    -- separator is ; semicolon

load data local infile '/Users/bhrig/Desktop/DS Bootcamp/Mid term/bank_loan/account.asc'
into table account
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
(account_id,district_id,@frequency,@acc_dt)
SET frequency = REPLACE(@frequency, '"', ''),
	acc_dt = STR_TO_DATE(REPLACE(@acc_dt, '"', ''), '%y%m%d')
;
-- checks
select count(*) from account;
select count(distinct (account_id)) from account; -- distinct values, account_id is unique
select * from account
where account_id is null or district_id is null or frequency is null or acc_dt is null;
select * from account;

# Create district table and import data
drop table if exists district;
create table district (
district_id int,
A2 varchar(30),
A3 varchar(30),
A4 int,
A5 int,
A6 int,
A7 int,
A8 int,
A9 int,
A10 decimal(10,2),
A11 int,
A12 decimal(10,2),
A13 decimal(10,2),
A14 int,
A15 int,
A16 int
);

## some observations on data format in input data
    -- order of input columns is same
    -- A2 and A3 are enclosed by " quotes
    -- No values are in scientific notation (like 1E+15)
    -- There are two ? marks in A12 and A15
    -- searched for ;; two semicolons together: no nulls from initial search
    -- separator is ; semicolon
    -- no empty strings ""

load data local infile '/Users/bhrig/Desktop/DS Bootcamp/Mid term/bank_loan/district.asc'
into table district
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
(district_id,@A2,@A3,A4,A5,A6,A7,A8,A9,A10,A11,@A12,A13,A14,@A15,A16)
SET A2 = REPLACE(@A2, '"', ''),
    A3 = REPLACE(@A3, '"', ''),
	A12 = NULLIF(@A12, '?'),
    A15 = NULLIF(@A15, '?')
;
-- checks
select count(*) from district;
select count(distinct (district_id)) from district; -- distinct values, district_id is unique
select * from district
where district_id is null;
select * from district;

# Create client_temp table and import raw data
drop table if exists client_temp;
create table client_temp (
client_id int,
vc_birth_date varchar(8),
district_id int
-- gender VARCHAR(20),
-- birth_date date
);

## some observations on data format in input data
    -- order of input columns is "client_id";"birth_number";"district_id"
    -- only birth_number enclosed by " quotes
    -- no gender column, need to derive from date format
    -- No values are in scientific notation (like 1E+15)
    -- searched for ;; two semicolons together: no nulls from initial search
    -- birth_date is in 450204 yymmdd format for men
    -- birth_date is in 706213 yymmd+50dd format for women, that is 50 is added to month, so mm-50 will be positive for women and negative for men
    -- separator is ; semicolon, no empty strings "", no ? marks

# getting the date and gender from it
    -- mm-50 will be positive for women and negative for men
    -- first load as varchar then perform the conversion and get the gender
    -- to get data and find gender:
        -- first subtract, and convert to date

load data local infile '/Users/bhrig/Desktop/DS Bootcamp/Mid term/bank_loan/client.asc'
into table client_temp
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
(client_id,@vc_birth_date,district_id)
SET  vc_birth_date = REPLACE(@vc_birth_date, '"', '')
;

-- date trials ----- internal reference -------------------------
select str_to_date (replace(19706213, '"',''), '%Y%m%d'); -- returns null for women birth_dates
select str_to_date (replace(19450204, '"',''), '%Y%m%d'); -- returns 1945-02-04 for men
select str_to_date (replace(450204, '"',''), '%Y%m%d'); -- is return 2045 for year, another issue, we can append 19 to each column, assuming no one is born before 1900
select str_to_date (replace(19450204, '"',''), '%Y%m%d'); -- then use capital Y and we get the right one

select substr('19706213', 5,2) - '50'; -- returns 12
select substr('19605703', 5,2) - '50'; -- returns 7, we want 07
select substr('19450204', 5,2) - '50'; -- returns -48

select concat('0',substr('19605703', 5,2) - '50');
select substr('1960703', 5,1);
select length('1938221');
select length('19380221');
-- ----------------------------------------------------------------
-- update vc_birthdate with prefix 19 so that years don't come in 2000s when converting, as this is 1999 data, clients cannot be born in 2000s
update client_temp
set vc_birth_date = concat('19',vc_birth_date);
-- creating a temp view for manipulations
drop view if exists view_temp_client;
create view view_temp_client
    as (
        select *,
               case when str_to_date(vc_birth_date, '%Y%m%d') is null
                    then 'Female' else 'Male' end as gender,
               case when str_to_date(vc_birth_date, '%Y%m%d') is null
                    then concat(substr(vc_birth_date, 1,4),  substr(vc_birth_date, 5,2) - '50', substr(vc_birth_date, 7,2))
                    else vc_birth_date end
                    as new_bd_temp
        from client_temp
);
-- now we can can add 0 to months where date length is 7 and then convert that result to date and store in a new table
-- some issue storing into table so saving as csv first and then loading back in
select client_id, gender, birth_date, district_id from (
    select client_id,
           gender,
           str_to_date(new_bd_temp2, '%Y%m%d') as birth_date,
           district_id
    from (
             select *,
                    case
                        when length(new_bd_temp) = 7
                            then concat(substr(new_bd_temp, 1, 4), concat('0', substr(new_bd_temp, 5, 1)),
                                        substr(new_bd_temp, 6, 2))
                        else new_bd_temp end
                        as new_bd_temp2
             from view_temp_client
         ) tmp
)t2
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/new_client.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';

-- now lets create table and load the corrected csv
create table client(
    client_id int,
    gender varchar(20),
    birth_date date,
    district_id int
);
load data local infile '/Users/bhrig/Desktop/DS Bootcamp/Mid term/bank_loan/new_client.csv'
into table client
fields terminated by ','
    enclosed by '"'
lines terminated by '\n';

-- checks on final client table--
select * from client;
select count(*) from client_temp;
select count(distinct (client_id)) from client_temp; -- distinct values, client_id is unique
select * from client
where client_id is null or district_id is null or birth_date is null or gender is null;

# create disposition table and upload data
drop table if exists disposition;
create table disposition(
    disp_id int,
    client_id int,
    account_id int,
    disp_type varchar(20)
);

## some observations on data format in input data
    -- order of input columns is "disp_id";"client_id";"account_id";"type"
    -- only type is enclosed by " quotes
    -- No values are in scientific notation (like 1E+15)
    -- searched for ;; two semicolons together: no nulls from initial search
    -- no empty strings "", no ? marks

load data local infile '/Users/bhrig/Desktop/DS Bootcamp/Mid term/bank_loan/disp.asc'
into table disposition
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
(disp_id,client_id,account_id,@disp_type)
SET  disp_type = REPLACE(@disp_type, '"', '')
;

select * from disposition;
select count(*) from disposition;
select count(distinct (disp_id)) from disposition; -- distinct values, disp_id is unique
select * from disposition
where disp_id is null or client_id is null or account_id is null or disp_type is null;

# create card table and upload data
drop table if exists credit_card;
create table credit_card(
    card_id int,
    disp_id int,
    cc_type varchar(20),
    issued_dt date
);

## some observations on data format in input data
    -- order of input columns is "card_id";"disp_id";"type";"issued"
    -- only type is enclosed by " quotes, date is in 930711 yymmdd h:m:s format but h:m:s is all 0
    -- No values are in scientific notation (like 1E+15)
    -- searched for ;; two semicolons together: no nulls from initial search
    -- no empty strings "", no ? marks

load data local infile '/Users/bhrig/Desktop/DS Bootcamp/Mid term/bank_loan/card.asc'
into table credit_card
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
(card_id, disp_id,@cc_type,@issued_dt)
SET  cc_type = REPLACE(@cc_type, '"', ''),
     issued_dt = STR_TO_DATE(substr(@issued_dt, 1,8), '%y%m%d')
;

select * from credit_card;
select count(*) from credit_card;
select count(distinct (disp_id)) from credit_card; -- distinct values, card_id is unique
select * from credit_card
where disp_id is null or card_id is null or cc_type is null or issued_dt is null;

# create loan table and upload data
drop table if exists loan;
create table loan(
    loan_id int,
    account_id int,
    loan_dt date,
    loan_amount int,
    duration int,
    payments decimal(10,2),
    status varchar(2)
);

## some observations on data format in input data
    -- order of input columns is "loan_id";"account_id";"date";"amount";"duration";"payments";"status"
    -- only status is enclosed by " quotes
    -- date is in 930711 yymmdd format
    -- No values are in scientific notation (like 1E+15)
    -- searched for ;; two semicolons together: no nulls from initial search
    -- no empty strings "", no ? marks

load data local infile '/Users/bhrig/Desktop/DS Bootcamp/Mid term/bank_loan/loan.asc'
into table loan
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
(loan_id, account_id,@loan_dt,loan_amount,duration,payments,@status)
SET  status = REPLACE(@status, '"', ''),
     loan_dt = STR_TO_DATE(replace(@loan_dt,'"',''), '%y%m%d')
;

select * from loan;
select count(*) from loan;
select count(distinct (loan_id)) from loan; -- distinct values, laon_id is unique
select * from loan
where loan_id is null or account_id is null or loan_dt is null or loan_amount is null or duration is null or payments is null or status is null;

# create order table and upload data
drop table if exists orders;
create table orders(
    order_id int,
    account_id int,
    bank_to varchar(10),
    account_to int,
    order_amount decimal(20,2),
    k_symbol varchar(20)
);

## some observations on data format in input data
    -- order of input columns is "order_id";"account_id";"bank_to";"account_to";"amount";"k_symbol"
    -- bank_to, account_to and k_symbol are enclosed by " quotes
    -- date is in 930711 yymmdd format
    -- No values are in scientific notation (like 1E+15)
    -- searched for ;; two semicolons together: no nulls from initial search
    -- empty space strings " " in k_symbol, no ? marks

load data local infile '/Users/bhrig/Desktop/DS Bootcamp/Mid term/bank_loan/order.asc'
into table orders
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
(order_id, account_id,@bank_to,@account_to,order_amount,@k_symbol)
SET  bank_to = REPLACE(@bank_to, '"', ''),
     account_to = REPLACE(@account_to, '"', ''),
     k_symbol = REPLACE(@k_symbol, '"', '')
;
select * from orders where k_symbol like '% %'; # 1379 rows don't have k_symbol, 5092 have the value
-- replacing these with nulls
update orders set k_symbol = null where k_symbol like '% %';

select * from orders;
select count(*) from orders;
select count(distinct (order_id)) from orders; -- distinct values, order_id is unique
select * from orders
where order_id is null or account_id is null or bank_to is null or account_to is null or order_amount is null or k_symbol is null;

# create transactions table and upload data
drop table if exists transactions;
create table transactions(
    trans_id int,
    account_id int,
    trans_dt date,
    trans_type varchar(20),
    operation varchar(20),
    trans_amount DECIMAL(10,2),
    balance DECIMAL(10,2),
    k_symbol_trans varchar(20),
    bank_trans varchar(10),
    partner_account int
);

## some observations on data format in input data
    -- order of input columns is "trans_id";"account_id";"date";"type";"operation";"amount";"balance";"k_symbol";"bank";"account"
    -- trans_type, operation, k_symbol, bank, account are enclosed by " quotes
        -- 637742;2177;930105;"PRIJEM";"PREVOD Z UCTU";5123.00;5923.00;"DUCHOD";"YZ";"62457513"
    -- date is in 930711 yymmdd format
    -- No values are in scientific notation (like 1E+15)
    -- searched for ;; two semicolons together: there are nulls or missing values or empty strings "" in bank, account, operations
    -- empty strings "" in k_symbol, no ? marks

load data local infile '/Users/bhrig/Desktop/DS Bootcamp/Mid term/bank_loan/trans.asc'
into table transactions
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
(trans_id,account_id,@trans_dt,@trans_type,@operation,trans_amount, balance, @k_symbol_trans,@bank_trans,@partner_account)
SET  trans_dt = STR_TO_DATE(replace(@trans_dt,'"',''), '%y%m%d'),
     trans_type = REPLACE(@trans_type, '"', ''),
     operation = REPLACE(@operation, '"', ''),
     k_symbol_trans = REPLACE(@k_symbol_trans, '"', ''),
     bank_trans = REPLACE(@bank_trans, '"', ''),
     partner_account = NULLIF(REPLACE(@partner_account, '"', ''),0)
;

select * from transactions where operation like ''; # 183,114 missing values in operation 2%
select * from transactions where k_symbol_trans like '';
select * from transactions where bank_trans like ''; # 782,812 missing values in bank_trans 8%
select * from transactions where partner_account is null; # 782,812 missing values in partner_account 8%
-- replacing empty strings with nulls
update transactions set operation = null where operation like '';
update transactions set k_symbol_trans = null where k_symbol_trans like '' or k_symbol_trans like ' ';
update transactions set bank_trans = null where bank_trans like '';
select count(*) from transactions where k_symbol_trans is null; # 535314 missing values in k_symbol_trans 5%

select * from transactions where trans_dt like '20%';
select * from transactions;
select * from transactions where trans_id=1113591; # some random checks
select count(*) from transactions; #1,056,320
select count(distinct (trans_id)) from transactions; -- distinct values, trans_id is unique


-- some more cleaning --select * from loan where status = 'A'; replacing \r empty lines
update loan set status = REPLACE(status,'\r','');
update orders set k_symbol = REPLACE(k_symbol,'\r','');
update transactions set k_symbol_trans = REPLACE(k_symbol_trans,'\r','');
update transactions set trans_type = REPLACE(trans_type,'\r','');
update transactions set operation = REPLACE(operation,'\r','');
update credit_card set cc_type = REPLACE(cc_type,'\r','');
update client set gender = REPLACE(gender,'\r','');
-- -------------------------replaced empty lines \r -------------------------------------

