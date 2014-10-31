

------------------------------------------------------------------
------------------------- FKT AUSFÜHREN---------------------------
------------------------------------------------------------------

select * from Cars where 
price <= 400 and Mileage <= 241893 and Horsepower >= 155



--price 400
--mileage = 241893
--horsepower = 155




--Userdefinierte Table-Function zurückgeben
select * from [Skyline_BNL]()



SELECT id FROM cars 
WHERE 
NOT EXISTS(
	SELECT 1 FROM cars h1 WHERE h1.price <= cars.price AND h1.mileage <= cars.mileage AND h1.horsepower >= cars.horsepower 
	AND ( h1.price < cars.price OR h1.mileage < cars.mileage OR h1.horsepower > cars.horsepower) ) 






select * from cars 
where price <= 9800 and mileage <= 46650 and Horsepower >= 200


where id = 42695



9800, 46650, 200



where carid
not in
(
--Ziel 207 records
SELECT id FROM cars WHERE NOT EXISTS(SELECT 1 FROM cars h1 WHERE h1.price <= cars.price AND h1.mileage <= cars.mileage AND h1.horsepower >= cars.horsepower AND ( h1.price < cars.price OR h1.mileage < cars.mileage OR h1.horsepower > cars.horsepower) ) 
)


------------------------------------------------------------------
--------- ASSEMBLY ENTFERNEN -------------------------------------
------------------------------------------------------------------

--Zuerst Funktionen entfernen
--DROP FUNCTION [Skyline_BNL]

DROP PROCEDURE [Getcar]

--Nun Assembly entfernen
DROP ASSEMBLY My_SQLSkyline



------------------------------------------------------------------
---------------------- ASSEMBLY HINZUFÜGEN + FKT ERSTELLEN--------
------------------------------------------------------------------

-- Assembly installieren
CREATE ASSEMBLY My_SQLSkyline FROM 'C:\SQL_CLR\SQLSkyline.dll'
GO

-- Create TableUDF
/*
CREATE FUNCTION [dbo].[Skyline_BNL]()
RETURNS table 
(
    carID INT
) 
AS
EXTERNAL NAME My_SQLSkyline.UserDefinedFunctions.SkylineBNL;
GO
*/


CREATE PROCEDURE GetCar (@Tablename nvarchar(15))
AS EXTERNAL NAME My_SQLSkyline.StoredProcedures.SP_SkylineBNL;



GO

--Ruft einige Attribute in Cars ab
EXEC dbo.GetCar 'cars'
