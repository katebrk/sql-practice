
--select *
--from dbo.view_covid_deaths
--where continent is not null
--order by location, date

--select *
--from view_covid_vaccinations
--order by location, date


-- Select data we need 
SELECT 
	location
	,date
	,total_cases
	,new_cases
	,total_deaths
	,population
FROM dbo.view_covid_deaths
ORDER BY location, date

-- Q1: Total Cases VS Total Deaths by location, date
SELECT
	location
	,date
	,total_cases
	,total_deaths
	,ROUND((total_deaths/total_cases*100), 2) AS 'death_percentage'
FROM dbo.view_covid_deaths
ORDER BY location

-- Q2: Total Cases VS Population by location, date
SELECT
	location
	,date
	,total_cases
	,population
	,ROUND((total_cases/population*100), 4) AS 'infection_percentage'
FROM dbo.view_covid_deaths
ORDER BY location

-- Q3: Countries with Highest Infection Rate to Population
SELECT
	location
	,MAX(population) AS population
	,MAX(total_cases) AS max_total_cases
	,ROUND((MAX(total_cases)/MAX(population)*100), 2) AS 'infection_percentage'
FROM dbo.view_covid_deaths
GROUP BY location
ORDER BY 'infection_percentage, %' DESC

-- Q4.1: Countries with Highest Death Count to Population by Location
SELECT
	location
	,MAX(population) AS population
	,MAX(total_deaths) AS max_total_deaths
	,ROUND((MAX(total_deaths)/MAX(population)*100), 4) AS 'death_percentage'
FROM dbo.view_covid_deaths
GROUP BY location
ORDER BY max_total_deaths DESC

-- Q4.2: Continent with Highest Death Count to Population 
SELECT
	continent
	,MAX(population) AS population
	,MAX(total_deaths) AS max_total_deaths
	,ROUND((MAX(total_deaths)/MAX(population)*100), 4) AS 'death_percentage'
FROM dbo.view_covid_deaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY max_total_deaths DESC

-- Q5.1: Global numbers by Date
SELECT
	date
	,SUM(new_cases) AS total_new_cases
	,SUM(new_deaths) AS total_new_deaths
	,ROUND((SUM(new_deaths)/SUM(new_cases) * 100), 2) AS death_percentage
FROM dbo.view_covid_deaths
WHERE continent IS NOT NULL 
	  AND new_cases <> 0
GROUP BY date
ORDER BY date, total_new_cases

-- Q5.2: Global numbers general
SELECT
	SUM(new_cases) AS total_new_cases
	,SUM(new_deaths) AS total_new_deaths
	,ROUND((SUM(new_deaths)/SUM(new_cases) * 100), 2) AS death_percentage
FROM dbo.view_covid_deaths
WHERE continent IS NOT NULL 
	  AND new_cases <> 0
ORDER BY total_new_cases


-- Q6: Total Population VS Total Vaccination
-- using CTE
WITH pop_vs_vac AS (
SELECT 
	dea.continent
	,dea.location
	,dea.date
	,dea.population AS max_population
	,vac.new_vaccinations AS num_vaccinations
	,SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS cumulative_vaccinations
FROM dbo.view_covid_deaths AS dea
JOIN dbo.view_covid_vaccinations AS vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
-- ORDER BY dea.continent, dea.location
)

SELECT 
	*
	,ROUND((cumulative_vaccinations/max_population) * 100, 4) AS cumulative_percentage
FROM pop_vs_vac


-- Q7: Total Population VS Total Vaccination
-- using temp table
DROP TABLE IF EXISTS #percent_population_vaccinated
CREATE TABLE #percent_population_vaccinated 
(
	continent NVARCHAR(255)
	,location NVARCHAR(255)
	,date DATETIME
	,population FLOAT
	,new_vaccinations FLOAT
	,cumulative_vaccinations FLOAT
)

INSERT INTO #percent_population_vaccinated 
SELECT 
	dea.continent
	,dea.location
	,dea.date
	,dea.population 
	,vac.new_vaccinations 
	,SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS cumulative_vaccinations
FROM dbo.view_covid_deaths AS dea
JOIN dbo.view_covid_vaccinations AS vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL

SELECT 
	*
	,ROUND((cumulative_vaccinations/population) * 100, 4) AS cumulative_percentage
FROM #percent_population_vaccinated


-- Q8: Create view for data viz
CREATE VIEW view_percent_population_vaccinated AS 
SELECT 
	dea.continent
	,dea.location
	,dea.date
	,dea.population 
	,vac.new_vaccinations 
	,SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS cumulative_vaccinations
FROM dbo.view_covid_deaths AS dea
JOIN dbo.view_covid_vaccinations AS vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL