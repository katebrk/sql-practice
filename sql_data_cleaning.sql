

/*

Data Cleaning 

*/



-- All data

SELECT *
FROM [housing].[dbo].[nashville_housing]



-- 1
-- Stardartize date format

-- option 1

SELECT 
	SaleDate
	,CONVERT(DATE, SaleDate)
FROM nashville_housing

UPDATE nashville_housing
SET SaleDate = CONVERT(DATE, SaleDate)


-- option 2

ALTER TABLE nashville_housing
ADD SaleDateConverted DATE;

UPDATE nashville_housing
SET SaleDateConverted = CONVERT(DATE, SaleDate)

SELECT 
	SaleDate
	,SaleDateConverted
FROM nashville_housing



-- 2
-- Populate Property Address Data

-- Some values in [PropertyAddress] column have NULL values; 
-- the same [ParcelID] has the same [PropertyAddress] => 
-- => need to replace NULL with [PropertyAddress] values from the same [ParcelID]


SELECT *
FROM nashville_housing
--WHERE PropertyAddress IS NULL
ORDER BY ParcelID


SELECT 
	a.ParcelID
	,a.PropertyAddress
	,b.ParcelID
	,b.PropertyAddress
	,ISNULL(a.PropertyAddress, b.PropertyAddress) -- populate null values
FROM nashville_housing AS a
JOIN nashville_housing AS b -- self join by the same [ParcelID] and different [UniqueID] to distinguish them
	ON a.ParcelID = b.ParcelID
	AND a.UniqueID <> b.UniqueID
WHERE a.PropertyAddress IS NULL


UPDATE a 
SET PropertyAddress = ISNULL(a.PropertyAddress, b.PropertyAddress) 
FROM nashville_housing AS a
JOIN nashville_housing AS b 
	ON a.ParcelID = b.ParcelID
	AND a.UniqueID <> b.UniqueID
WHERE a.PropertyAddress IS NULL



-- 3 
-- Breaking out address ([PropertyAddress], [OwnerAddress]) into individual columns (address, city, state) 

SELECT *
FROM [housing].[dbo].[nashville_housing]


-- option 1 > transform [PropertyAddress] using SUBSTRINGs

SELECT 
	PropertyAddress
	,SUBSTRING(PropertyAddress, 1, CHARINDEX(',', PropertyAddress)-1) AS Address
	,SUBSTRING(PropertyAddress, CHARINDEX(',', PropertyAddress)+2, LEN(PropertyAddress)) AS City
FROM nashville_housing


ALTER TABLE nashville_housing
ADD PropertySplitAddress NVARCHAR(255);

UPDATE nashville_housing
SET PropertySplitAddress = SUBSTRING(PropertyAddress, 1, CHARINDEX(',', PropertyAddress)-1)


ALTER TABLE nashville_housing
ADD PropertySplitCity NVARCHAR(255);

UPDATE nashville_housing
SET PropertySplitCity = SUBSTRING(PropertyAddress, CHARINDEX(',', PropertyAddress)+2, LEN(PropertyAddress))


-- option 2 (easier) > transform [OwnerAddress] using PARSENAME
-- PARSENAME only useful with . and works from the end of the string

SELECT 
	OwnerAddress
	, PARSENAME(REPLACE(OwnerAddress, ',', '.'), 3) AS OwnerAddress
	, PARSENAME(REPLACE(OwnerAddress, ',', '.'), 2) AS OwnerCity
	, PARSENAME(REPLACE(OwnerAddress, ',', '.'), 1) AS OwnerState
FROM [housing].[dbo].[nashville_housing]


ALTER TABLE nashville_housing
ADD OwnerSplitAddress NVARCHAR(255);

UPDATE nashville_housing
SET OwnerSplitAddress = PARSENAME(REPLACE(OwnerAddress, ',', '.'), 3)


ALTER TABLE nashville_housing
ADD OwnerSplitCity NVARCHAR(255);

UPDATE nashville_housing
SET OwnerSplitCity = PARSENAME(REPLACE(OwnerAddress, ',', '.'), 2)


ALTER TABLE nashville_housing
ADD OwnerSplitState NVARCHAR(255);

UPDATE nashville_housing
SET OwnerSplitState = PARSENAME(REPLACE(OwnerAddress, ',', '.'), 1)



-- 4
-- Transform string values in column

-- [SoldAsVacant] column contains 4 distinct values: Y, N, Yes, No => 
-- => need to change Y and N to Yes and No 

SELECT 
	DISTINCT SoldAsVacant
	, COUNT(SoldAsVacant)
FROM [housing].[dbo].[nashville_housing]
GROUP BY SoldAsVacant
ORDER BY 2


SELECT
	SoldAsVacant
	,CASE WHEN SoldAsVacant = 'Y' THEN 'Yes'
	      WHEN SoldAsVacant = 'N' THEN 'No'
		  ELSE SoldAsVacant END
FROM [housing].[dbo].[nashville_housing]


UPDATE [nashville_housing] 
SET SoldAsVacant = CASE WHEN SoldAsVacant = 'Y' THEN 'Yes'
	      WHEN SoldAsVacant = 'N' THEN 'No'
		  ELSE SoldAsVacant 
		  END


-- 5
-- Remove duplicates
-- Duplicate is the row that has the same ParcelID, PropertyAddress, SaleDate, SalePrice, LegalReference


WITH row_num_CTE AS (
SELECT *
	, ROW_NUMBER() OVER (PARTITION BY ParcelID, PropertyAddress, SaleDate, SalePrice, LegalReference ORDER BY UniqueID) AS unique_row_num
FROM [housing].[dbo].[nashville_housing]
--ORDER BY ParcelID
)

DELETE
FROM row_num_CTE
WHERE unique_row_num > 1



-- 6 
-- Delete unused columns

select *
FROM [housing].[dbo].[nashville_housing]


ALTER TABLE [housing].[dbo].[nashville_housing]
DROP COLUMN LandUse, TaxDistrict

ALTER TABLE [housing].[dbo].[nashville_housing]
DROP COLUMN HalfBath