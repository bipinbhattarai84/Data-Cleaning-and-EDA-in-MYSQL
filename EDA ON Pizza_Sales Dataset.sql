 show variables like "secure_file_priv";
-- loading just the big data sets and rest two small tables manually for table import wizard
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/order_details.csv" into table order_details 
fields terminated by ','
enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines; 


load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/orders.csv" into table dummy 
fields terminated by ','
enclosed by '"'
lines terminated by '\r\n'
 ignore 1 lines
; 
delete from order_details
where order_details_id >= 1 ;


-- EXPLORATORY DATA ANALYSIS 

-- Retrieve the total number of orders placed.
SELECT COUNT(*) FROM ORDERS ;

-- Calculate the total revenue generated from pizza sales.


select sum(order_details.quantity * pizzas.price) as total_price  from order_details 
join pizzas on 
order_details.pizza_id  = pizzas.pizza_id
;

 -- Identify the highest-priced pizza.
 select pizzas.pizza_id, pizza_types.name, pizzas.price  price from pizzas 
 join pizza_types ON 
 PIZZAS.pizza_type_id = pizza_types.pizza_type_id
 order by price desc 
 limit 5 ;

 select pizza_id, price, rank()over( order by price desc ) rank_by_price from pizzas;

-- Identify the most common pizza size ordered.-------
select  pizzas.size, sum(order_details.quantity) as countt from order_details
join pizzas on 
order_details.pizza_id = pizzas.pizza_id
group by pizzas.size 
order by countt desc ; 

select  pizzas.size, count(order_details_id) from order_details
join pizzas on 
order_details.pizza_id = pizzas.pizza_id 
group by  pizzas.size; 



-- List the top 5 most ordered pizza types along with their quantities.
select pizza_types.name, sum(order_details.quantity) from order_details
join pizzas on 
	order_details.pizza_id = pizzas.pizza_id
join pizza_types on 
pizzas.pizza_type_id = pizza_types.pizza_type_id
    group by  pizza_types.name;

-- Intermediate:
-- Join the necessary tables to find the total quantity of each pizza category ordered.
select pizza_types.category, sum( order_details.quantity) from order_details 
	join pizzas on 
		order_details.pizza_id = pizzas.pizza_id 
	join pizza_types on 
		pizzas.pizza_type_id = pizza_types.pizza_type_id
group by pizza_types.category;



-- Determine the distribution of orders by hour of the day.
select hour(`time`), count(order_id) from orders
group by hour(`time`);

-- Determine the distribution of  orders in quantity by hour of the day.
select date, sum(order_details.quantity) from orders
join order_details on 
	orders.order_id = order_details.order_id
group by date;

with average as (
select `date`, sum(order_details.quantity) as orders from orders
	join  order_details 
    on order_details.order_id= orders.order_id
group by date)
select round(avg(orders), 0) as avg_number_pizza_per_day from average
; 


-- Group the orders by date and calculate the average number of pizzas ordered per day.
select `date`, count(order_id) as orders from orders
group by date;

with average as (
select `date`, count(order_id) as orders from orders
group by date)
select round(avg(orders), 0) as avg_number_pizza_per_day from average
; 

-- Determine the top 3 most ordered pizza types based on revenue.
select pizza_types.name, sum( pizzas.price * order_details.quantity)  from order_details
	join pizzas
    on order_details.pizza_id = pizzas.pizza_id
    join pizza_types
    on pizzas.pizza_type_id = pizza_types.pizza_type_id
group by pizza_types.name
order by 2 desc
limit 3 ;


select pizza_types.name, sum( pizzas.price * order_details.quantity) as total_revenue   from order_details
	join pizzas
    on order_details.pizza_id = pizzas.pizza_id
    join pizza_types
    on pizzas.pizza_type_id = pizza_types.pizza_type_id
group by pizza_types.name
order by 2 desc
limit 3 ;

-- Advanced:
-- Calculate the percentage contribution of each pizza chicken or what type to total revenue.

with grand_total as (
select pizza_types.category as category, round(sum( pizzas.price * order_details.quantity ) , 2 )as total_revenue   from order_details
	join pizzas
    on order_details.pizza_id = pizzas.pizza_id
    join pizza_types
    on pizzas.pizza_type_id = pizza_types.pizza_type_id
group by pizza_types.category
order by 2 desc
), percent as (
select category, total_revenue, sum(total_revenue)over() as gt from grand_total 
) 
select category, total_revenue, round(gt,2) , round((total_revenue/gt)*100,2) as percent_contribution from percent 
order by 4 desc;



-- Determine the top 3 most ordered pizza types based on revenue for each pizza category.

with grand_total as (
select pizza_types.name as name, sum( pizzas.price * order_details.quantity ) as total_revenue   from order_details
	join pizzas
    on order_details.pizza_id = pizzas.pizza_id
    join pizza_types
    on pizzas.pizza_type_id = pizza_types.pizza_type_id
group by pizza_types.name
order by 2 desc
), percent as (
select name, total_revenue, sum(total_revenue)over() as gt from grand_total 
) 
select name, total_revenue, round(gt,2) , round((total_revenue/gt)*100,2) as percent_contribution from percent 
order by 4 desc;

-- Analyze the cumulative revenue generated over time. 

with cumulative as (
select distinct(substring(date,1,7) ) as dat, round(sum( pizzas.price * order_details.quantity )over(partition by substring(date,1,7)),2) as total_revenue   from order_details
	join orders
    on order_details.order_id = orders.order_id
    join pizzas 
    on order_details.pizza_id= pizzas.pizza_id
    )
    select dat , total_revenue, sum(total_revenue)over(order by dat) as cumulative_revenue from cumulative  ;
    
    
    