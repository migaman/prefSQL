/******************************
** File:    01_Install_Assembly
** Name:	Install Assembly
** Desc:	Install the prefSQL assembly and its functions
**			Change the path of the assembly (Variable ASSEMBLY_PATH) before running
** Auth:	Michael
** Date:	05/01/2015
**************************
** Change History
**************************
** PR   Date			Author		Description	
** --   --------		-------		------------------------------------
** 1    01/01/2015      You			sample comment
*******************************/


------------------------------------------------------------------
------------ VARIABLES (CHANGE TO YOUR ENVIRONMENt) --------------
------------------------------------------------------------------

DECLARE @ASSEMBLY_PATH varchar(400);
SET @ASSEMBLY_PATH = 'E:\Doc\Studies\PRJ_Thesis\prefSQL\SQLSkyline\bin\Release\SQLSkyline.dll';

------------------------------------------------------------------
----------- REMOVE ASSEMBLY  -------------------------------------
------------------------------------------------------------------

--Drop Procedures if exists
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'SP_SkylineBNL')
	DROP PROCEDURE SP_SkylineBNL

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'SP_SkylineBNLLevel')
	DROP PROCEDURE SP_SkylineBNLLevel

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'SP_SkylineBNLSort')
	DROP PROCEDURE SP_SkylineBNLSort

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'SP_SkylineBNLSortLevel')
	DROP PROCEDURE SP_SkylineBNLSortLevel

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'SP_SkylineDQ')
	DROP PROCEDURE SP_SkylineDQ

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'SP_SkylineHexagon')
	DROP PROCEDURE SP_SkylineHexagon

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'SP_SkylineHexagonLevel')
	DROP PROCEDURE SP_SkylineHexagonLevel

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'SP_MultipleSkylineBNL')
	DROP PROCEDURE SP_MultipleSkylineBNL


--Drop Assembly
IF EXISTS(SELECT * FROM sys.assemblies WHERE name = 'SQLSkyline')
	DROP ASSEMBLY [SQLSkyline]


------------------------------------------------------------------
---------------------- ADD ASSEMBLY & PROCEDURES------------------
------------------------------------------------------------------

-- Add Assembly
CREATE ASSEMBLY SQLSkyline FROM @ASSEMBLY_PATH
GO

--Create SP for BNL (with Incomparable)
CREATE PROCEDURE SP_SkylineBNL (@Name nvarchar(4000), @Operators nvarchar(200), @NumberOfRecords int, @SortType int)
AS EXTERNAL NAME SQLSkyline.[prefSQL.SQLSkyline.SPSkylineBNL].GetSkyline;
GO
--Create SP for BNL Levelized (without incomparable)
CREATE PROCEDURE SP_SkylineBNLLevel (@Name nvarchar(4000), @Operators nvarchar(200), @NumberOfRecords int, @SortType int)
AS EXTERNAL NAME SQLSkyline.[prefSQL.SQLSkyline.SPSkylineBNLLevel].GetSkyline;
GO
--Create SP for BNLSort (with Incomparable)
CREATE PROCEDURE SP_SkylineBNLSort (@Name nvarchar(4000), @Operators nvarchar(200), @NumberOfRecords int, @SortType int)
AS EXTERNAL NAME SQLSkyline.[prefSQL.SQLSkyline.SPSkylineBNLSort].GetSkyline;
GO
--Create SP for BNLSort Levelized (without incomparable)
CREATE PROCEDURE SP_SkylineBNLSortLevel (@Name nvarchar(4000), @Operators nvarchar(200), @NumberOfRecords int, @SortType int)
AS EXTERNAL NAME SQLSkyline.[prefSQL.SQLSkyline.SPSkylineBNLSortLevel].GetSkyline;
GO
--Create SP for DQ
CREATE PROCEDURE SP_SkylineDQ (@Name nvarchar(4000), @Operators nvarchar(200), @NumberOfRecords int, @SortType int)
AS EXTERNAL NAME SQLSkyline.[prefSQL.SQLSkyline.SPSkylineDQ].GetSkyline;
GO
--Create SP for Hexagon
CREATE PROCEDURE SP_SkylineHexagon (@Name nvarchar(4000), @Operators nvarchar(200), @NumberOfRecords int, @SortType int, @DistinctIncomparable nvarchar(4000), @DistinctLevelIncomparable int)
AS EXTERNAL NAME SQLSkyline.[prefSQL.SQLSkyline.SPSkylineHexagon].GetSkyline;
GO
--Create SP for Hexagon Level
CREATE PROCEDURE SP_SkylineHexagonLevel (@Name nvarchar(4000), @Operators nvarchar(200), @NumberOfRecords int, @SortType int)
AS EXTERNAL NAME SQLSkyline.[prefSQL.SQLSkyline.SPSkylineHexagonLevel].GetSkyline;
GO
--Create SP for MultipleSkyline
CREATE PROCEDURE SP_MultipleSkylineBNL (@Name nvarchar(4000), @Operators nvarchar(200), @NumberOfRecords int, @UpToLevel int)
AS EXTERNAL NAME SQLSkyline.[prefSQL.SQLSkyline.SPMultipleSkylineBNL].GetSkyline;
GO

