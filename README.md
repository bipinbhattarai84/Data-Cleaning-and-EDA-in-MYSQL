# Data-Cleaning-
Data cleaning and EDL on World layoff dataset 

SELECT * FROM world_layoffs.layoffs;

-- STARTING WITH DATA CLEANING -- 

--  1 . REMOVAL OF DUPLICATE VALUES 

-- 1 (A) FIRST LOOKING IF THERE ARE ANY DUPLICATE ROWS 
WITH row_num AS (
SELECT * ,row_number()over(partition by company, location, industry, total_laid_off, 
					percentage_laid_off, date, stage, country, funds_raised_millions)as rownumber from layoffs)
Select * from row_num 
where rownumber > 1 ;

-- 1 (B) THERE WERE DUPLICATE ROWS, NOW DELETING THESE ROWS FROM THE TABLE. INORDER TO DO THAT CREATING DUPLICATE TABLE OF LAYOFFS WITH ADDITIONAL ROWNUM COLUMN 


CREATE TABLE `layoffs_staging` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `rownumber` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging 
select *, row_number()over(partition by company, location, industry, total_laid_off, 
					percentage_laid_off, date, stage, country, funds_raised_millions)as rownumber  from layoffs; 
                    
select * from layoffs_staging
where rownumber > 1 ;

delete from layoffs_staging 
where rownumber > 1 ;


-- here the duplicates are deleted and updated on the table . 

-- 2 . STANDARDIZING THE VALUES 

select * from layoffs_staging
where industry like 'Crypto%';

update layoffs_staging 
set industry = 'Crypto' 
where industry like 'Crypto%';

update layoffs_staging
set company= trim(company);

update layoffs_staging
set country = trim(trailing '.' from country); 

-- CHANGING DATE DATATYPE 
SELECT * FROM LAYOFFS_STAGING;

select date, str_to_date(`date`, '%m/%d/%Y') from layoffs_staging;

update layoffs_staging
set `date`= str_to_date(`date`, '%m/%d/%Y');
alter table layoffs_staging
modify column `date` DATE;

delete from layoffs_staging
where total_laid_off is null and percentage_laid_off is null ;

select * from layoffs_staging
where industry is null ;

-- setting all the blank data from industry columns to null 
update layoffs_staging
set industry= null where industry = '';

select * from layoffs_staging
where company in ('Airbnb', 'Carvana', 'Juul');

select A.industry, B.industry from layoffs_staging A  
 join layoffs_staging B 
on A.company = B.company
and A.location = B.location
where  A.industry is null and B.industry is not null;

update layoffs_staging A 
join layoffs_staging B
	on A.company = B.company
		and A.location = B.location 
set A.industry = B.industry 
where A.industry is null and B.industry is not null; 

-- 4. REMOVING ANY COLUMNS 
alter table layoffs_staging 
drop column rownumber;


# EXPLORATORY DATA ANALYSIS 

SELECT * FROM world_layoffs.layoffs_staging;

-- Exploartory Data Analysis 
select max(total_laid_off), max(percentage_laid_off) from layoffs_staging;

-- compnay who had to let all of their employees go in one day

select company, total_laid_off, percentage_laid_off from layoffs_staging 
where percentage_laid_off = 1
order by 1 asc ; 

-- total layoffs by company 
select company, sum(total_laid_off) from layoffs_staging
where total_laid_off is not null
group by company
order by 2 desc; 

-- EXPLORTING DATA ON THE BASIS OF DATES 
	-- IN TERMS OF YEAR AND MONTH 
select company, substring(`date`,1,7) as date, sum(total_laid_off) from layoffs_staging
where total_laid_off is not null AND `date` is not null
group by company, substring(`date`, 1,7)
order by 2 asc;
	
    -- IN TERMS OF YEAR 

select company, substring(`date`,1,4) as date, sum(total_laid_off) from layoffs_staging
where total_laid_off is not null AND `date` is not null
group by company, substring(`date`, 1,4)
order by 3 desc;


select substring(`date`, 1 ,4 ) as dat, sum(total_laid_off)  from layoffs_staging 
where `date` is not null
group by substring(`date`, 1 ,4 ) 
order by 2 desc
;
 
-- laid off on the basis of industry 

select industry, substring(`date`, 1 ,7 ), sum(total_laid_off) from layoffs_staging
where `date` is not null
group by industry, substring(`date`, 1 ,7 )
order by 1 ,2  asc ; 


-- MAX LAID OFF IN ONE PARTICULAR YEAR ON THE BASIS OF INDUSTRY 


with max_laid_off as (
select industry, substring(`date`, 1 ,4 ) as dat, sum(total_laid_off) as laid_total from layoffs_staging
where `date` is not null
group by industry, substring(`date`, 1 ,4 )
order by 2 asc
)
select industry, max(laid_total) from max_laid_off
group by industry
ORDER BY 2 DESC;

SELECT YEAR(`DATE`), SUM(total_laid_off) from layoffs_staging 
group by year(`date`);

-- ROLLING TOTAL FOR EMPLOYEES LAID  in terms of industry and date month specific 
with rollingtotal as (
select  industry, substring(`date`, 1,7) as dat, sum(total_laid_off) as total_laid from layoffs_staging 
group by industry, substring(`date`, 1, 7)
) 
select industry,dat, total_laid, sum(total_laid) over( partition by industry order by  dat ) as rolling_total from rollingtotal
 where dat is not null ;

with rollingtotal as (
select  industry, substring(`date`, 1,4) as dat, sum(total_laid_off) as total_laid from layoffs_staging 
group by industry, substring(`date`, 1, 4)
) 
select industry,dat, total_laid, sum(total_laid) over( partition by industry order by  dat ) as rolling_total from rollingtotal
 where dat is not null ;
 
with rollingtotal as (
select  company, substring(`date`, 1,4) as dat, sum(total_laid_off) as total_laid from layoffs_staging 
group by company, substring(`date`, 1, 4)
) 
select company,dat, total_laid, sum(total_laid) over( partition by company order by  dat asc  ) as rolling_total from rollingtotal
 where dat is not null ;

 -- Rank of company on the basis of total laid off in different years.
 with rank_stage as (
select company, year(`date`) as dat, sum(total_laid_off) as laid_total from layoffs_staging
where total_laid_off is not null and date is not null 
group by company, year(`date`)
),rankstage2 as (
select * , rank()over( partition by dat order by laid_total desc) as rankk from rank_stage
)
select * from rankstage2 
where rankk <5 ;














