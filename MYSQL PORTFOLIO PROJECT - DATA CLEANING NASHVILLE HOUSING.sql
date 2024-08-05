-- THE DATASET CONTAINS INFORMATION ABOURT NASHVILLE HOUSING FROM 2013-01-02 TO 2019-12-13
-- FINAL CLEANED TABLE IS n_housingfinal
-- FOLLOWING SKILLS WERE USED : 
			-- LOAD DATA INFILE 
            -- REMOVING DUPLICATES 
            -- WINDOW FUNCTION ROW_NUMBER
            -- REMOVING NULL VALUES 
            -- SELF JOIN 
            -- CTE 
            -- SUBSTRING_INDEX 
            -- ALTER, UPDATE, INSERT, MODIFY 
            -- CASE EXPRESSIONS 
            

-- IMPORTING NASHVILLE HOUSING CSV FILE 
load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/nash_housing.csv' into table n_housing
fields terminated by ','
enclosed by '"' 
lines terminated by '\r\n'
ignore 1 lines;

select max(SaleDate), min(SaleDate) from n_housing
;
select * from n_housingstage
where PropertyAddress is null;

-- CREATE DUPLICATE TO WORK WITH 

CREATE TABLE n_housingstage 
like n_housing;

INSERT INTO n_housingstage 
select * from n_housing;

select * from n_housingstage
where parcelid = '081 02 0 144.00';
select * from n_housingfinal
where parcelid = '081 02 0 144.00';

-- DATA CLEANING -- 

-- B. STANDARDIZING DATE FORMAT 
	-- the date column was in date time format so changing to just date format 
alter table n_housingstage 
modify column SaleDate Date;

-- C.  STANDARDIZING THE DATA 

select distinct(LandUse) from n_housingstage
ORDER BY 1 ASC;

  -- two different names for vacant residential landing found .. fixing that now 
 select * from n_housingstage;
 
select * from n_housingstage
where LandUse like 'VACANT RES%';

SELECT COUNT(LandUse) from n_housingstage
where LandUse = 'VACANT RESIENTIAL LAND';

update n_housingstage 
set LandUse = 'VACANT RESIDENTIAL LAND' 
WHERE LandUse = 'VACANT RESIENTIAL LAND' ;

-- POPULATING THE PROPERTY ADDRESS  

-- PERFORFING SELF JOIN TO POPULATE THE PROPERTY ADDRESS 
select  a.UniqueID,a.parcelid, a.propertyaddress,b.UniqueID, b.parcelid, b.propertyaddress, ifnull(a.propertyaddress,b.propertyaddress) from n_housingstage a 
join n_housingstage b 
on a.ParcelID = b.ParcelID
where a.PropertyAddress is null and 
(b.PropertyAddress is  not null);

update n_housingstage a 
join n_housingstage b 
on a.parcelid = b.parcelid
set a.propertyaddress = b.propertyaddress 
where a.propertyaddress is null 
and ( b.propertyaddress is not null) ;

-- BREAKING OUT ADDRESS INTO INDIVIDUAL COLUMNS ADDRESS CITY STATE for PROPERTY ADDRESS AND OWNER ADDRESS 

select propertyaddress from n_housingstage;
select propertyaddress, substring_index(propertyaddress, ',', 1) as address, 
substring_index(propertyaddress, ',', -1) as city from n_housingstage;

-- CREATING NEW COLUMNS ADDRESS AND CITY 
alter table n_housingstage 
add column propertysplitaddress text, 
add column propertysplitcity text
;  

-- UPDATING COLUMNS ADDRESS AND CITY FROM PROPERTY ADDRESS 
update n_housingstage
set propertysplitaddress = substring_index(propertyaddress, ',', 1);

update n_housingstage 
set propertysplitcity= substring_index(propertyaddress,',',-1);

select * from n_housingstage;

-- CREATING NEW COLUMNS ADDRESS AND CITY - OWNER ADDRESS 
alter table n_housingstage 
add column ownersplitaddress text, 
add column ownersplitcity text,
add column ownersplitstate text
;  

-- UPDATING COLUMNS ADDRESS AND CITY FROM owner address 
select  OwnerAddress, substring_index(substring_index(owneraddress,',',2), ',',-1) from n_housingstage;
update n_housingstage
set ownersplitaddress = substring_index(owneraddress, ',', 1);

update n_housingstage 
set ownersplitcity= substring_index(substring_index(owneraddress,',',2), ',',-1);

update n_housingstage 
set ownersplitstate= substring_index(owneraddress,',',-1);

select * from n_housingstage;

-- CHANGING YES AND NO TO Y AND N FROM soldasvacant 
select soldasvacant,
case when soldasvacant= 'yes' then 'Y' 
	when soldasvacant- 'no' then 'N'
    else soldasvacant
    end 
    from n_housingstage;
update n_housingstage 
	set soldasvacant = case
		when soldasvacant = 'No'  then 'N'
	when soldasvacant = 'yes' then 'Y'
    else soldasvacant
    end
    ;
    
    
-- A. FINDING DUPLICATE ROWS IF THERE IS ANY 
with check_row as (
select *, row_number()over(partition by ParcelID, LandUse, PropertyAddress, SaleDate, SalePrice, LegalReference, 
							SoldAsVacant, OwnerName, OwnerAddress,Acreage, TaxDistrict, LandValue, BuildingValue, TotalValue, YearBuilt, 
							Bedrooms, FullBath, HalfBath) as rownum from n_housingstage
) 
select * from check_row 
where rownum >1;
-- select count(*) from check_row where rownum > 1 ;

--  103 duplicate rows found in the data set 

-- creating new column rownum 
-- creating duplicate table to n_housingstage with extrea rownum column 

CREATE TABLE `n_housingfinal` (
  `UniqueID` text,
  `ParcelID` text,
  `LandUse` text,
  `PropertyAddress` text,
  `SaleDate` date DEFAULT NULL,
  `SalePrice` text,
  `LegalReference` text,
  `SoldAsVacant` text,
  `OwnerName` text,
  `OwnerAddress` text,
  `Acreage` text,
  `TaxDistrict` text,
  `LandValue` text,
  `BuildingValue` text,
  `TotalValue` text,
  `YearBuilt` text,
  `Bedrooms` text,
  `FullBath` text,
  `HalfBath` text,
  `propertysplitaddress` text,
  `propertysplitcity` text,
  `ownersplitaddress` text,
  `ownersplitcity` text,
  `ownersplitstate` text,
  `rownum` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into n_housingfinal
select *, row_number()over(partition by ParcelID, LandUse, PropertyAddress, SaleDate, SalePrice, LegalReference, 
							SoldAsVacant, OwnerName, OwnerAddress,Acreage, TaxDistrict, LandValue, BuildingValue, TotalValue, YearBuilt, 
							Bedrooms, FullBath, HalfBath) from n_housingstage;
                            
delete from n_housingfinal
where rownum > 1 ;

-- checking if the duplicates are deleted
select count(*) from n_housingfinal
where rownum > 1 ;
select * from n_housingstage
where parcelid = '081 02 0 144.00';
select * from n_housingfinal
where parcelid = '081 02 0 144.00';

-- deleting the rownum column from n_housingfinal ( dont' neecd it since we have removed the duplicates) 
alter table n_housingfinal
drop column rownum;

--  THE TABLE BELOW SHOWS THE FINAL CLEANED DATA 
select * from n_housingfinal ;






    
 














