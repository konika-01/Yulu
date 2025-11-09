create database p_blikit;

use p_blinkit;

-- Loading Customers tables using sql query
select * from customers;
truncate customers;

set global local_infile = 1;

load data local infile 'D:/Data Analytics/Utkarsh_IMT/Projects/Blinkit/Dataset/blinkit_customers.csv'
into table blinkit_customers
fields terminated by ',' enclosed by '"'
lines terminated by '\r\n'         
ignore 1 lines;

-- creating stored procedure for select statement

delimiter $$
create procedure t_select_dynamic(tbl_name varchar(255))
begin
	set @sql = concat('select * from `' , tbl_name, '`;');
    prepare stmt from @sql;
    execute stmt;
    deallocate prepare stmt;
end $$
delimiter ;

call t_select_dynamic('blinkit_customers'); 						-- calling the procedure

-- creating stored procedure for truncate statement
drop procedure if exists t_truncate_dynamic;

delimiter $$ 
create procedure t_truncate_dynamic(tbl_name varchar(255))
begin 
	set @sql = concat('truncate `',tbl_name, '`;');
    prepare stmt from @sql;
    execute stmt;
    deallocate prepare stmt;
end $$
delimiter ;

call t_truncate_dynamic('blinkit_customers');

-- Loading other tables
-- -- orders table
call t_select_dynamic('blinkit_orders');
call t_truncate_dynamic('blinkit_orders');


load data local infile 'D:/Data Analytics/Utkarsh_IMT/Projects/Blinkit/Dataset/blinkit_orders.csv'
into table blinkit_orders
fields terminated by ',' enclosed by '"'
lines terminated by '\r\n'         
ignore 1 lines;

-- products
call t_select_dynamic('blinkit_products');
call t_truncate_dynamic('blinkit_products');


load data local infile 'D:/Data Analytics/Utkarsh_IMT/Projects/Blinkit/Dataset/blinkit_products.csv'
into table blinkit_products
fields terminated by ',' enclosed by '"'
lines terminated by '\r\n'         
ignore 1 lines;

-- order items
call t_select_dynamic('blinkit_order_items');
call t_truncate_dynamic('blinkit_order_items');

load data local infile 'D:/Data Analytics/Utkarsh_IMT/Projects/Blinkit/Dataset/blinkit_order_items.csv'
into table blinkit_order_items
fields terminated by ',' enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines;

-- inventory
call t_select_dynamic('blinkit_inventory');
call t_truncate_dynamic('blinkit_inventory');

load data local infile 'D:/Data Analytics/Utkarsh_IMT/Projects/Blinkit/Dataset/blinkit_inventory.csv'
into table blinkit_inventory
fields terminated by ',' enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines;

-- new inventory
call t_select_dynamic('blinkit_inventorynew');
call t_truncate_dynamic('blinkit_inventorynew');

load data local infile 'D:/Data Analytics/Utkarsh_IMT/Projects/Blinkit/Dataset/blinkit_inventoryNew.csv'
into table blinkit_inventorynew
fields terminated by ',' enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines;

-- delivery performance
call t_select_dynamic('blinkit_delivery_performance');
call t_truncate_dynamic('blinkit_delivery_performance');

load data local infile 'D:/Data Analytics/Utkarsh_IMT/Projects/Blinkit/Dataset/blinkit_delivery_performance.csv'
into table blinkit_delivery_performance
fields terminated by ',' enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines;

-- feedback
call t_select_dynamic('blinkit_customer_feedback');
call t_truncate_dynamic('blinkit_customer_feedback');

load data local infile 'D:/Data Analytics/Utkarsh_IMT/Projects/Blinkit/Dataset/blinkit_customer_feedback.csv'
into table blinkit_customer_feedback
fields terminated by ',' enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines;


/*
Data Cleanning Steps followed are
1. changing col name or table name
2. null values
3. duplicates
4. checking data type and data type conversion
5. text values integrity and  segment integrity
6. EDA
*/
 
/*        ---------------- Customers - Data Cleaning and EDA ---------------           */

-- 1. changing table name
rename table blinkit_customers to customers;

-- 2. checking null values
select count(*) as null_values 
from customers 
where
customer_id is null 
or customer_name is null
or email is null
or phone is null
or address is null
or area is null
or pincode is null
or registration_date is null
or customer_segment is null
or total_orders is null
or avg_order_value is null; 
-- no nulls

-- 3. checking dupes
select count(*) as dupe_count from customers
group by customer_id, customer_name, email, phone, address, area, pincode, registration_date,customer_segment,
total_orders,avg_order_value
having dupe_count > 1;
-- no dupes 

-- 4. checking data types follwed by data type conversion as per the requirement
desc customers;
-- registration_date is stored as text

-- converting registration date to date data type
select registration_date                       -- '2024-11-06' → ISO (good format)'
from customers                                 -- '06-11-2024' or '06/11/24' → needs conversion
limit 10;                                      -- 'Nov 6, 2024' → also needs conversion

select str_to_date(registration_date, '%Y-%m-%d') as conv_date
from customers;

update customers
	set registration_date = str_to_date(registration_date, '%Y-%m-%d');

alter table customers
modify registration_date date;    
 
-- 5. checking text values integrity
select 
trim(customer_name),
trim(email),
trim(phone),
trim(address),
trim(area),
trim(pincode),
trim(registration_date),
trim(customer_segment)
from customers;

update customers
set 
customer_name = trim(customer_name),
email = trim(email),
phone = trim(phone),
address = trim(address),
area = trim(area),
pincode = trim(pincode),
registration_date = trim(registration_date),
customer_segment = trim(customer_segment);

-- 6. EDA

select * from customers; #2500 rows

select 	count(distinct customer_id) as unique_id, -- 2500
		count(distinct customer_name) as unique_name, -- 2491
        count(distinct email) as unique_email, -- 2500
        count(distinct phone) unique_phone, -- 2500
        count(distinct address) unique_address, -- 2500
		count(distinct area) unqiue_area, -- 315
        count(distinct pincode) unqiue_pincode, -- 2494
        count(distinct registration_date) unqiue_rdate, -- 589
        count(distinct customer_segment) unique_segment, -- 4
		count(distinct total_orders) unique_orders, -- 20
        count(distinct avg_order_value) unique_aov -- 2487
from customers;

-- checking area more deeply

-- from data area and pincode does not match with address 
-- area and pincode are incorrectly stored

alter table customers
drop column area, 
drop column pincode;

select address from customers;
-- need to extract state and address

-- keeping cust_seg, total_orders, aov for now, will drop after finding a better replacement

-- renaming col name 

select * from customers;

alter table customers
rename column customer_id to cust_id;

alter table customers
rename column customer_name to cust_name,
rename column registration_date to reg_date;

alter table customers 
rename column customer_segment to cust_segment;

select distinct cust_segment from customers;
-- premium, inactive, regular, new

select min(reg_date), max(reg_date) from customers;
-- 2023 - 03 - 16 to 2024 - 11 - 04

select min(total_orders), max(total_orders) from customers;
-- 1 to 20

select * from customers;

/* 
insight -> there are 2500 unique customers already marked as premium, inactive, regular, new having registered 
date between 2023 - 03 - 16 to 2024 - 11 - 04, having total orders ranging from 1 to 20. Cust-segment, total_orders, 
aov are already engineered cols. These cols are kept for now and will be removed once better replacement is found
*/


/*        ---------------- Orders - Data Cleaning and EDA ---------------           */

-- 1. changing table name or col name
alter table blinkit_order rename to orders;

alter table orders
rename column customer_id to cust_id,
rename column promised_delivery_time to scheduled_dl_date_time,
rename column actual_delivery_time to actual_dl_date_time;

-- 2. checking null values
select *  
from orders 
where
order_id is null 
or cust_id is null
or order_date is null
or scheduled_delivery_date is null
or actual_delivery_date is null
or delivery_status is null
or order_total is null
or payment_method is null
or delivery_partner_id is null
or store_id is null
or order_time is null
or scheduled_delivery_time is null
or actual_delivery_time is null;
-- no null values

-- 3. checking dupes
select count(*) as dupe_count from orders
group by order_id, cust_id, order_date, scheduled_delivery_date, actual_delivery_date, delivery_status,
order_total, payment_method, delivery_partner_id, store_id, order_time, scheduled_delivery_time,
actual_delivery_time
having dupe_count >1;
-- no dupes

-- 4. checking data type and date type conversion if needed

desc orders;
-- order date & time - text
-- scheduled_dl_time - text 
-- actual_dl_time - text

select substring_index(order_date,' ',1) as date,            -- substring_index(str, delimiter,count)
substring_index(order_date,' ',-1)  as time                  -- 1 : before delimiter, -1: after delimiter
from orders;

alter table orders add column order_time varchar(60);

start transaction;                                              -- so that transaction can be recalled

update orders
set order_time = substring_index(order_date,' ',-1),
order_date = substring_index(order_date,' ',1);

commit;

select substring_index(scheduled_dl_date_time,' ',1) as s_date,   
substring_index(scheduled_dl_date_time,' ',-1) as s_time,   
substring_index(actual_dl_date_time,' ',1) as a_date,    
substring_index(actual_dl_date_time,' ',-1) as a_time
from orders;
        
alter table orders add column scheduled_delivery_time varchar(60),
add column actual_dlivery_time varchar (60);

start transaction;

update orders
set scheduled_delivery_time = substring_index(scheduled_dl_date_time,' ',-1),
scheduled_dl_date_time = substring_index(scheduled_dl_date_time,' ',1) ,
actual_dlivery_time = substring_index(actual_dl_date_time,' ',-1),
actual_dl_date_time = substring_index(actual_dl_date_time,' ',1);

commit;

alter table orders
rename column scheduled_dl_date_time to scheduled_delivery_date,
rename column actual_dl_date_time to actual_delivery_date;

select str_to_date(order_date,'%Y-%m-%d'),                        -- str_to_date(date,'format_of_date_present')
str_to_date(scheduled_delivery_date,'%Y-%m-%d'),                  -- date_format(date,'format_of_date_output')
str_to_date(actual_delivery_date,'%Y-%m-%d')
from orders;     

start transaction;

update orders
set order_date = str_to_date(order_date,'%Y-%m-%d'),
scheduled_delivery_date = str_to_date(scheduled_delivery_date,'%Y-%m-%d'),
actual_delivery_date = str_to_date(actual_delivery_date,'%Y-%m-%d');

commit;
                                                             
alter table orders
modify column order_date date,
modify column scheduled_delivery_date date,
modify column actual_delivery_date date; 
			
alter table orders
modify column order_time time,
modify column scheduled_delivery_time time,
modify column actual_dlivery_time time;

alter table orders
rename column actual_dlivery_time to actual_delivery_time;

-- 5. checking text values integrity
select trim(delivery_status),
trim(payment_method) from orders;

update orders
set delivery_status = trim(delivery_status),
payment_method = trim(payment_method);

-- 6. EDA

select count(*) from orders; -- 5000 records

select count(distinct order_id), -- 5000
	count(distinct cust_id), -- 2172
	count(distinct delivery_status), -- 3
	count(distinct payment_method), -- 4
	count(distinct delivery_partner_id), -- 5000
	count(distinct store_id)  -- 5000
	from orders;

select distinct delivery_status
from orders;
-- on time, slightly delayed, significantly delayed

select distinct payment_method
from orders;
-- cash, upi, card, wallet

select min(scheduled_delivery_date), max(actual_delivery_date) from orders;
-- 2023 - 03 - 16 5o 2024 -- 11 - 04

select * from orders;

/*
insight -> there are 5000 records under orders, and we have uniques cols as - order_id 5000, cust_id 2172, 
delivery status 3 (already marked as on time, slightly delayed and significantly delayed), 
payment_method 4 (cash, upi, wallet, card), delivery partner id 50000, and store id 5000.
Blinkit has scheduled delivery date ranging from 2023 - 03 - 16 to actual delivery date as 2024 - 11 - 04.
*/

/*        ---------------- Products - Data Cleaning and EDA ---------------           */

-- 1. checking table name
alter table blinkit_products rename to products;

-- 2. checking null values
select * from products 
where 
product_id is null
or product_name is null
or category is null
or brand is null
or price is null
or mrp is null
or margin_percentage is null
or shelf_life_days is null
or min_stock_level is null
or max_stock_level is null;
-- no nulls

-- 3. checking dupe count

select count(*) as dupe_count from products
group by product_id, product_name, category, brand, price, mrp, margin_percentage, shelf_life_days, 
min_stock_level, max_stock_level
having dupe_count >1;
-- no dupes

-- 4. checking data types
desc products;

-- 5. checking text values integrity
select trim(product_name), trim(brand) from products;

update products 
set
product_name = trim(product_name),
brand = trim(brand);

-- 6. EDA

select count(*) from products;
-- 268

select count(distinct product_id),  -- 268
count(distinct product_name), -- 51
count(distinct category), -- 11
count(distinct brand) -- 267
from products;

select distinct product_name from products;

select distinct category from products;

select distinct brand from products;

select * from products 
where brand in 
(select brand
from products
group by brand
having count(brand) >1);

select * from products;

/* 
insight -> there are no null values and dupe records, currently in the products blinkit has unique 268 product 
id,11 unique categories, 51 unique products, and 267 unique brands. Only Jha Group breand has 2 products 
listed (Nuts and Mango Drink), else all other brands has only 1 product listed.
*/

/*        ---------------- Order_items - Data Cleaning and EDA ---------------           */

-- 1. renaming table name
alter table blinkit_order_items rename to order_items;

-- 2. checking null values
select * from order_items
where 
order_id is null
or product_id is null
or quantity is null
or unit_price is null;
-- no null

-- 3. checking dupes
select count(*) as dupe_count
from order_items
group by order_id , product_id, quantity, unit_price
having dupe_count > 1;
-- no dupes

-- 4. checking data types and conversions
desc order_items;

-- 5. checking text values integrity
-- no text col

-- 6. EDA
select count(distinct order_id),   -- 5000
count(distinct product_id), -- 268
count(distinct quantity),    -- 3 
count(distinct unit_price)   -- 267
from order_items;

select distinct quantity from order_items;
-- 1,2,3

select min(unit_price), max(unit_price) from order_items;
-- 12.32 to 995.98

/*
insight -> there are 5000 unique order_id in order_items, 268 unique product_id, 3 distinct quantity (1,2,3), 
and unit price varies greatly ranging from around 12 to around 1k. 
*/

select * from order_items; 

/*        ---------------- Invetory - Data Cleaning and EDA ---------------                   */

-- 1. changing table name
alter table blinkit_inventory rename to inventory;

-- 2. checking null values
select * from inventory 
where 
product_id is null
or date is null
or stock_received is null
or damaged_stock is null;
-- no null

-- 3. check duplicates
select count(*) as dupe_count from inventory
group by product_id, date, stock_received, damaged_stock
having dupe_count > 1;
-- no null

-- 4. checking data type
desc inventory;
-- date - text

select str_to_date(date,'%d-%m-%Y') from inventory;              -- default date format provided -- y - m - d

start transaction;

update inventory 
set 
date = str_to_date(date,'%d-%m-%Y');

commit;

alter table inventory modify column date date;

desc inventory;

-- 5. checking text values
-- no text values

-- 6. EDA
select count(*) from inventory; -- 75172 records

select count(distinct product_id) from inventory; -- 268 unique products 

select min(date), max(date) from inventory; -- 2023-03-17 to 2024-11-05

select min(stock_received),    -- 0
max(stock_received),           -- 4
min(damaged_stock),            -- 0
max(damaged_stock)             -- 2
from inventory;

select * from inventory;

/* 
insight -> 268 product id are restocked between 2023-03-17 to 2024-11-05 and min stock that is received is 0 to
max stock received is 4. similary, min damaged stock received is 0 and max damaged stock received is 2. We have 
75172 data entries for the same.
*/


/*        ---------------- Delivery Performace - Data Cleaning and EDA ---------------           */

-- 1. changing table name
alter table blinkit_delivery_performance rename to Delivery_performance;

alter table Delivery_performance
rename column promised_time to scheduled_time;

-- 2. checking null values
select * from delivery_performance 
where 
order_id is null
or delivery_partner_id is null
or scheduled_time is null
or actual_time is null
or delivery_time_minutes is null
or distance_km is null
or delivery_status is null
or reasons_if_delayed is null;

select reasons_if_delayed from delivery_performance
where trim(reasons_if_delayed) = "";
-- there are empty spaces which are not stored as null

start transaction;

update delivery_performance
set reasons_if_delayed = null
where trim(reasons_if_delayed) = "";

commit;

select count(*) from delivery_performance
where reasons_if_delayed is null;
-- 1905

-- updating null values as other in delivery_performace

start transaction;

update delivery_performance 
set reasons_if_delayed = "other"
where reasons_if_delayed is null;

commit;

-- 3. check duplicates
select * from delivery_performance;

select count(*) as dupe_count from delivery_performance
group by order_id, delivery_partner_id, scheduled_time, actual_time, delivery_time_minutes, distance_km,
delivery_status, reasons_if_delayed
having dupe_count > 1;
-- 2 dupes for 7 rows

-- 4. checking data type
desc delivery_performance;
-- scheduled time - text
-- actual time text
-- delivery_status text
-- reasons if dealyed text


/*        ---------------- Feedback - Data Cleaning and EDA ---------------                      */

-- 1. changing table name
alter table blinkit_customer_Feedback rename to Feedback;

-- 2. checking null values
select count(*) 
where feedback_id is null or trim(feedback_id) = ""
or order_id is null 
or customer_id is null 
or rating is null 
or feedback_text is null or trim(feedback_text) = ""
or feedback_category is null or trim(feedback_category) = ""
or sentiment is null or trim(sentiment) = ""
or feedback_date is null or trim(feedback_date) = ""
from feedback;

use p_blinkit;
select * from feedback;

