-- database

show databases;
drop database if exists bank_loan;
create database bank_loan;
use bank_loan;

-- tables

set global local_infile = 1;

-- account table
drop table if exists acc;

create table acc(
	 account_id		                    int      NOT NULL,
     district_id                        int,
     frequency                          varchar(20),
     date                               date
);

describe acc;

load data local infile 'C:/Users/sarav/Desktop/ML/bank_loan/bank_loan/account.asc'
into table acc
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
    (account_id, district_id, @frequency, @date)
set date = STR_TO_DATE(REPLACE(@date, '"', ''),'%y%m%d'),
    frequency =REPLACE(@frequency, '"', '');

select * from acc;


-- loan table
drop table if exists loan;
create table loan(
	 loan_id                        int      NOT NULL,
     account_id                     int,
     date                           date,
     amount                         int,
     duration                       int,
     payments                       decimal,
     status                         varchar(3)
);

describe loan;

load data local infile 'C:/Users/sarav/Desktop/ML/bank_loan/bank_loan/loan.asc'
into table loan
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
    (loan_id, account_id, @date, amount, duration, payments, @status)
set date = STR_TO_DATE(REPLACE(@date, '"', ''),'%y%m%d'),
    status = REPLACE(@status, '"', '');

select * from loan;

-- order table
drop table if exists orders;
create table orders(
	 order_id                        int     NOT NULL,
     account_id                      int,
     bank_to                         varchar(20),
     account_to                      int,
     amount                          int,
     k_symbol                        varchar(10)
);

describe orders;

load data local infile 'C:/Users/sarav/Desktop/ML/bank_loan/bank_loan/order.asc'
into table orders
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
(order_id, account_id, @bank_to, @account_to, amount, @k_symbol)
set account_to = REPLACE(@account_to, '"', ''),
bank_to = REPLACE(@bank_to, '"', ''),
k_symbol = REPLACE(@k_symbol, '"', '')
;

select * from orders;

-- trans table
drop table if exists trans;
create table trans(
	 trans_id                        int       NOT NULL,
     account_id                      int,
     date                            date,
     type                            varchar(10),
     operation                       varchar(10),
     amount                          int,
     balance                         int,
     k_symbol                        varchar(10),
     bank                            varchar(10),
     account                         int

);

describe trans;

load data local infile 'C:/Users/sarav/Desktop/ML/bank_loan/bank_loan/trans.asc'
into table trans
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
(trans_id, account_id, date, @type, @operation, amount, balance, @k_symbol, @bank, @account)
set type = REPLACE(@type, '"', ''),
operation = REPLACE(@operation, '"', ''),
k_symbol = REPLACE(@k_symbol, '"', ''),
bank = REPLACE(@bank, '"', ''),
account = REPLACE(@account, '"', '')
;

select * from trans;

-- card table
drop table if exists card;
create table card(
	 card_id                         int        NOT NULL,
     disp_id                         int,
     type                            varchar(10),
     issued                          date
);

describe card;

load data local infile 'C:/Users/sarav/Desktop/ML/bank_loan/bank_loan/card.asc'
into table card
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
(card_id, disp_id, @type, @issued)
set issued = STR_TO_DATE(REPLACE(@issued, '"', ''),'%y%m%d'),
    type = REPLACE(@type, '"', '')
;

select * from card;

-- disp table
drop table if exists disp;

create table disp(
	 disp_id                         int         NOT NULL,
     client_id                       int,
     account_id                      int,
     type                            varchar(10)
);

describe disp;

load data local infile 'C:/Users/sarav/Desktop/ML/bank_loan/bank_loan/disp.asc'
into table disp
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
(disp_id, client_id, account_id, @type)
set type = REPLACE(@type, '"', '')
;

select * from disp;

-- district table
drop table if exists district;

create table district(
	 district_id                     int       NOT NULL,
     A2                              varchar(10),
     A3                              varchar(10),
     A4                              int,
     A5                              int,
     A6                              int,
     A7                              int,
     A8                              int,
     A9                              int,
     A10                             decimal,
     A11                             int,
     A12                             decimal,
     A13                             decimal,
     A14                             int,
     A15                             int,
     A16                             int
);

describe district;

load data local infile 'C:/Users/sarav/Desktop/ML/bank_loan/bank_loan/district.asc'
into table district
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
(district_id, @A2, @A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16)
set A2 = REPLACE(@A2, '"', ''),
    A3 = REPLACE(@A3, '"', '')
;

select * from district;

-- client table
drop table if exists client_info;

create table client_info(
     client_id                       int  NOT NULL,
     birth_number                    int,
     district_id                     int,
     gender                          varchar(3),
     birth_date                      date
);

describe client_info;

load data local infile 'C:/Users/sarav/Desktop/ML/bank_loan/bank_loan/client.asc'
into table client_info
fields terminated by ';'
lines terminated by '\n'
ignore 1 lines
    (client_id, @birth_number, district_id, gender, birth_date)
set birth_number = REPLACE(@birth_number, '"', '')
;

select * from client_info;

-- client_fixed table
drop table if exists client_fixed;

create table client_fixed(
     client_id                       int  NOT NULL,
     birth_number                    date,
     district_id                     int,
     gender                          varchar(1)
);

describe client_fixed;

load data local infile 'C:/Users/sarav/Desktop/ML/bank_loan/bank_loan/client_fixed.csv'
into table client_fixed
fields terminated by ','
lines terminated by '\n'
ignore 1 lines
;

select * from client_fixed;

-- district_fixed table
select A3, count(A3)
from district
where A2 in(
    district_id from acc
        where account_id in(
        select account_id from loan where status = 'D'
    )
)
group by A3

-- Merging Data Tables

-- client_fixed + district
drop table if exists client_district;

create table client_district(
    select cf.client_id, cf.birth_number, cf.gender, district.district_id, district.A2, district.A3, district.A4, district.A5, district.A6, district.A7, district.A8, district.A9, district.A10, district.A11, district.A12, district.A13, district.A14, district.A15, district.A16 from client_fixed cf
        left join district on cf.district_id = district.district_id
    union
    select cf.client_id, cf.birth_number, cf.gender, district.district_id, district.A2, district.A3, district.A4, district.A5, district.A6, district.A7, district.A8, district.A9, district.A10, district.A11, district.A12, district.A13, district.A14, district.A15, district.A16 from client_fixed cf
        right join district on cf.district_id = district.district_id
    );

select * from client_district;

-- acc + orders
drop table if exists acc_orders;

create table acc_orders(
    select acc.district_id, acc.frequency, acc.date, orders.order_id, orders.account_id, orders.bank_to, orders.account_to, orders.amount, orders.k_symbol  from acc
        left join orders on acc.account_id = orders.account_id
   );

select * from acc_orders;

-- loan + orders
drop table if exists loan_orders;

create table loan_orders(
select loan.loan_id, loan.account_id, loan.date, loan.amount, loan.duration, loan.payments, loan.status, orders.order_id, orders.bank_to,orders.account_to, orders.amount as order_amount, orders.k_symbol from loan
left join orders on loan.account_id = orders.account_id
);

select * from loan_orders;

-- disp + card
drop table if exists disp_card;

create table disp_card(
select disp.client_id, disp.account_id, disp.type as disp_type, card.card_id, card.disp_id, card.type, card.issued from disp
left join card on disp.disp_id = card.disp_id
);

select * from loan_orders;

-- disp_card + client_district -> client_district_disp_card

drop table if exists client_district_disp_card;

create table client_district_disp_card(
    select cd.client_id, cd.birth_number, cd.gender, cd.district_id, cd.A2, cd.A3, cd.A4, cd.A5, cd.A6, cd.A7, cd.A8, cd.A9, cd.A10, cd.A11, cd.A12, cd.A13, cd.A14, cd.A15, cd.A16, disp_card.client_id as disp_card_client_id, disp_card.account_id, disp_card.disp_type, disp_card.card_id, disp_card.disp_id, disp_card.type, disp_card.issued from client_district cd
        left join disp_card on cd.client_id = disp_card.client_id
    union
    select cd.client_id, cd.birth_number, cd.gender, cd.district_id, cd.A2, cd.A3, cd.A4, cd.A5, cd.A6, cd.A7, cd.A8, cd.A9, cd.A10, cd.A11, cd.A12, cd.A13, cd.A14, cd.A15, cd.A16, disp_card.client_id as disp_card_client_id, disp_card.account_id, disp_card.disp_type, disp_card.card_id, disp_card.disp_id, disp_card.type, disp_card.issued from client_district cd
        right join disp_card on cd.client_id = disp_card.client_id
    );

select * from client_district_disp_card;


-- fix trans table

# we need to take the trans dates before the loan default
select trans.*,
       loan.date as L_date
from trans
join loan on loan.account_id = trans.account_id;

#get only the transactions before loan default
create table trans_(
    select *
    from (select trans.*,
                 loan.date as L_date
          from trans
                   join loan on loan.account_id = trans.account_id) as t
    where L_date > date
);

select * from trans_


create table trans_type_count(
    select account_id,
           count(case when type = 'PRIJEM' then amount end) as saving,
           count(case when type = 'VYDAJ' then amount end)  as withdrawal,
           count(case when type = 'VYBER' then amount end)  as withdrawal1
    from trans_
    group by account_id
    order by account_id
);


drop table if exists trans_agg;

create table trans_agg (
select account_id,
       count(trans_id) as trans_num,
       max(date) as trans_date,
       sum(case when operation = 'VYBER KARTOU' then 1 else 0 end) as credit_card_withdrawal,
       sum(case when operation = 'VKLAD' then 1 else 0 end) as credit_in_cash,
       sum(case when operation = 'VYBER' then 1 else 0 end) withdrawal_in_cash,
       sum(case when operation = 'PREVOD Z UCTU' then 1 else 0 end) as collection_from_another_bank,
       sum(case when operation = 'PREVOD NA UCET' then 1 else 0 end) as remmitance_to_another_bank,
       avg(case when operation = 'VYBER KARTOU' then amount end) as credit_card_withdrawal_amount,
       avg(case when operation = 'VKLAD' then amount end) as credit_in_cash_amount,
       avg(case when operation = 'VYBER' then amount end) withdrawal_in_cash_amount,
       avg(case when operation = 'PREVOD Z UCTU' then amount end) as collection_from_another_bank_amount,
       avg(case when operation = 'PREVOD NA UCET' then amount end) as remmitance_to_another_bank_amount,
       case when min(balance) < 0 then 1 else 0 end as Negativebalance
from trans_
group by account_id
order by account_id);

select * from trans_agg;


drop table if exists trans_agg_;

create table trans_agg_(
    select trans_agg.*, ttc.saving, ttc.withdrawal, ttc.withdrawal1 from trans_agg
        inner join trans_type_count ttc on trans_agg.account_id = ttc.account_id);

select * from trans_agg_;

# drop table if exists trans_fixed;

# create table trans_fixed(
#     select          account_id,
#                     trans_id,
#                     type,
#                     date,
#                     round(sum(amount),2) as sum,
#                     round(avg(balance),2) as avg_balance,
#                     round(min(balance),2) as min_balance
#     from trans_
#     group by account_id, type
#     order by account_id
# );
#
# select * from trans_fixed;


-- loan_order + transaction --> trans_fixed_loan_order


#     select trans_agg.*, lo.date as loan_date, lo.amount as loan_amount, lo.duration, lo.payments, lo.status, lo.order_id, lo.order_amount, lo.k_symbol as loan_k_symbol from trans_agg
#         right join loan_orders lo on trans_agg.account_id = lo.account_id
#     );

drop table if exists trans_agg_loan_order;

create table trans_agg_loan_order(
    select trans_agg.*, lo.date as loan_date, lo.amount as loan_amount, lo.duration, lo.payments, lo.status as loan_k_symbol from trans_agg
        inner join loan_orders lo on trans_agg.account_id = lo.account_id);
#     union


-- client_district_disp_card + trans_fixed_loan_order --> bank_loan_final

drop table if exists bank_loan_final2;

create table bank_loan_final2(
    select cddc.birth_number,
           cddc.gender,
           cddc.type as client_type,
           cddc.issued,
           cddc.A2,
           cddc.A3,
           cddc.A4,
           cddc.A5,
           cddc.A6,
           cddc.A7,
           cddc.A8,
           cddc.A9,
           cddc.A10,
           cddc.A11,
           cddc.A12,
           cddc.A13,
           cddc.A14,
           cddc.A15,
           cddc.A16,
           talo.*
    from client_district_disp_card cddc
             inner join trans_agg_loan_order talo on cddc.account_id = talo.account_id
#     union
#     select  cddc.client_id,
#            cddc.birth_number,
#            cddc.gender,
#            cddc.account_id,
#            cddc.type as client_type,
#            cddc.issued,
#            cddc.A2,
#            cddc.A3,
#            cddc.A4,
#            cddc.A5,
#            cddc.A6,
#            cddc.A7,
#            cddc.A8,
#            cddc.A9,
#            cddc.A10,
#            cddc.A11,
#            cddc.A12,
#            cddc.A13,
#            cddc.A14,
#            cddc.A15,
#            cddc.A16,
#            tflo.type as transaction_type,
#            tflo.sum,
#            tflo.min_balance,
#            tflo.avg_balance,
#            tflo.loan_date,
#            tflo.loan_amount,
#            tflo.duration,
#            tflo.payments,
#            tflo.status,
#            tflo.order_amount,
#            tflo.loan_k_symbol
#     from client_district_disp_card cddc
#         right join trans_fixed_loan_order tflo on cddc.account_id = tflo.account_id);

select * from bank_loan_final2;

create table bank_loan_final3(
    select count(distinct (client_id)) as num_of_acc_holders, bank_loan_final2.*
    from bank_loan_final2
    group by account_id
);

select * from bank_loan_final3;

alter table bank_loan_final3
    drop client_id;

select * from bank_loan_final3;


drop table if exists bank_loan_final3;

select count(*),
       transaction_type
from bank_loan_final3
group by transaction_type;



