select * from coviddeaths ;
select * from covidvaccinations;

-- CHANGING DATA TYPE OF total_deaths from text to int ( found out later when performing EDA at line 45)
alter table coviddeaths 
modify column total_deaths int;
alter table coviddeaths 
modify column new_deaths int;
alter table covidvaccinations
modify column new_vaccinations int;


-- Selecting the datas that we will be going to use 
select location, date, total_cases, new_cases, total_deaths, population from coviddeaths
where continent is not null
order by 1,2;

-- Total cases vs Total deaths
select max(total_cases), max(total_deaths) from coviddeaths;

-- overal death percentage among the covid_cases
select (max(total_deaths)/ max(total_cases))*100 from coviddeaths;

-- death percentage in terms of covid_cases each day location wise 
select location, date, total_cases, new_cases, total_deaths, population,  (total_deaths / total_cases)* 100  as death_percentage from coviddeaths
where location = "australia";

-- TOTAL CASES VS POPULATION 
		-- percentage of pouplatoin affected by covid each day with respect to various countries( location) 

select location, date, population, total_cases, (total_cases/ population)*100 as cases_in_percent from coviddeaths
	where location = "United States";
    
-- COUNTRY HAVING HIGHEST INFECTION RATES 

select location,population, ((max(total_cases)/population)*100 )as infection_rate from coviddeaths
where continent is not null
group by location,population
order by 3 desc;

-- TOP TEN COUNTRIES WITH HIGHEST INFECTION RATE
with infection_rate as (
select location,population, ((max(total_cases)/population)*100 ) as rate_of_infection from coviddeaths
where continent is not null
group by location,population
)
select location, rate_of_infection, rank()over(order by rate_of_infection desc) from infection_rate
limit 10
; 

-- COUNTRIES WITH HIGHEST DEATH COUNT and death rate 
select location, MAX(total_deaths) as Death_count from coviddeaths
where continent is not null
group by location
order by 2 desc; 

select location,population, MAX(total_deaths) as Death_count, ((MAX(total_deaths)/population) *100) as death_rate from coviddeaths
where continent is not null
group by location,population
order by 4 desc; 

-- CONTINENT WISE 
-- this doesnt work beacuse it counts the max death of continents in terms of country (max of asia for qatar or max of asia for nepal and so on )
select continent, MAX(total_deaths) as Death_count from coviddeaths
group by continent
order by 2 desc; 

-- using the new_deaths columns works for the toal deaths in terms of continent
select continent, sum(new_deaths) as Death_count from coviddeaths
group by continent
order by 2 desc; 

-- another way of death count in terms of continent 
select location, MAX(total_deaths) as Death_count from coviddeaths
where continent is null
group by location
order by 2 desc; 

-- GLOBAL DATA CALCULATION 
		-- DEATH PERCENTAGE EACH DAY 
select sum(new_cases) as total_cases, sum(new_deaths), (sum(new_deaths)/SUM(new_cases))*100 as death_percent from coviddeaths 
where continent is not null
 ;

 
		-- JOIN TABLES DEATHS AND VACCINATIONS 

-- LOOKING AT TOAL PEOPLE IN THE WORLD VACCINATED 
	
select dea.continent,dea.location,dea.date,dea.population, vac.new_vaccinations from coviddeaths dea
join covidvaccinations vac
on dea.location = vac.location 
and dea.date = vac.date 
and dea.continent is not null

order by 2,3 ;

-- USING ROLLING TOTAL TO FIND THE VACCINATED POPULATION BY DAY 


select dea.continent,dea.location,dea.date,dea.population, vac.new_vaccinations, 
	sum(vac.new_vaccinations)over( partition by dea.location order by dea.date ) as rolling_total from coviddeaths dea
join covidvaccinations vac
on dea.location = vac.location 
and dea.date = vac.date 
and dea.continent is not null


;

-- USING CTE TABLE TO WORK ON THE ROLLING TOTAL VALUE ( TOTAL POPULATION AND VACCINATION ) 

with rollingpeoplevaccinated as (
select dea.continent,dea.location,dea.date,dea.population, vac.new_vaccinations, 
	sum(vac.new_vaccinations)over( partition by dea.location order by dea.date asc) as rolling_total from coviddeaths dea
join covidvaccinations vac
on dea.location = vac.location 
and dea.date = vac.date 
and dea.continent is not null
)
select continent, location,date,population,new_vaccinations,rolling_total, (rolling_total/population)*100 as vaccinated_rate from rollingpeoplevaccinated
;

-- USING TEMP TABLE ( TOTAL POPULATION AND VACCINATION ) 

CREATE TEMPORARY TABLE perpopulationvaccinated
 ( continent varchar(255),
 location varchar(255),
 `DATE` datetime,
 population bigint,
 new_vaccinations int,
 rolling_total int
 );
 insert into perpopulationvaccinated
 select dea.continent,dea.location,dea.date,dea.population, vac.new_vaccinations, 
	sum(vac.new_vaccinations)over( partition by dea.location order by dea.date asc) as rolling_total from coviddeaths dea
join covidvaccinations vac
on dea.location = vac.location 
and dea.date = vac.date 
and dea.continent is not null
;
 
select continent, location,date,population,new_vaccinations,rolling_total, (rolling_total/population)*100 
		as vaccinated_rate from perpopulationvaccinated;
;


-- CREATING VIEW 

Create view  perpopulationvaccinated  as
select dea.continent,dea.location,dea.date,dea.population, vac.new_vaccinations, 
	sum(vac.new_vaccinations)over( partition by dea.location order by dea.date asc) as rolling_total from coviddeaths dea
join covidvaccinations vac
on dea.location = vac.location 
and dea.date = vac.date 
and dea.continent is not null
;