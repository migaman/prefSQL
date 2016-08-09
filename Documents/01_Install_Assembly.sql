/******************************
** File:    01_Install_Assembly
** Name:	Install Assembly
** Desc:	Install the prefSQL assembly and its functions
**			Change the path of the assembly (Variable ASSEMBLY_PATH) before running
** Auth:	Michael
** Date:	12/06/2016
**************************
** Change History
**************************
** PR   Date			Author		Description	
** --   --------		-------		------------------------------------
** 1    05/01/2015      Michael		First version
** 2    16/10/2015      Michael		Remove the 4000 character limit for queries
** 3    20/10/2015      Stefan      Add Skyline Sampling procedure
** 4    12/06/2016		Michael		Add Procedure for Ranking
*******************************/


------------------------------------------------------------------
------------ VARIABLES (CHANGE TO YOUR ENVIRONMENt) --------------
------------------------------------------------------------------

--DECLARE @ASSEMBLY_PATH varchar(400);
--SET @ASSEMBLY_PATH = 'E:\Projekte\prefSQL\SQLSkyline\bin\Release\SQLSkyline.dll';


DECLARE @ASSEMBLY_PATH varchar(400);
SET @ASSEMBLY_PATH = '$(assemblyFile)'


------------------------------------------------------------------
----------- REMOVE ASSEMBLY  -------------------------------------
------------------------------------------------------------------

--Drop Procedures if exists (this are old procedure names, before 21/06/2016)
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

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'SP_MultipleSkylineBNLLevel')
	DROP PROCEDURE SP_MultipleSkylineBNLLevel

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'SP_SkylineSampling')
    DROP PROCEDURE SP_SkylineSampling

--Drop Procedures if exists
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'prefSQL_SkylineBNL')
	DROP PROCEDURE prefSQL_SkylineBNL

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'prefSQL_SkylineBNLLevel')
	DROP PROCEDURE prefSQL_SkylineBNLLevel

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'prefSQL_SkylineBNLSort')
	DROP PROCEDURE prefSQL_SkylineBNLSort

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'prefSQL_SkylineBNLSortLevel')
	DROP PROCEDURE prefSQL_SkylineBNLSortLevel

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'prefSQL_SkylineDQ')
	DROP PROCEDURE prefSQL_SkylineDQ

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'prefSQL_SkylineHexagon')
	DROP PROCEDURE prefSQL_SkylineHexagon

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'prefSQL_SkylineHexagonLevel')
	DROP PROCEDURE prefSQL_SkylineHexagonLevel

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'prefSQL_MultipleSkylineBNL')
	DROP PROCEDURE prefSQL_MultipleSkylineBNL

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'prefSQL_MultipleSkylineBNLLevel')
	DROP PROCEDURE prefSQL_MultipleSkylineBNLLevel

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'prefSQL_SkylineSampling')
    DROP PROCEDURE prefSQL_SkylineSampling

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'PC' AND name = 'prefSQL_Ranking')
    DROP PROCEDURE prefSQL_Ranking
	
	
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
CREATE PROCEDURE prefSQL_SkylineBNL (@Name nvarchar(MAX), @Operators nvarchar(200), @NumberOfRecords int, @SortType int)
AS EXTERNAL NAME SQLSkyline.[prefSQL.SQLSkyline.SPSkylineBNL].GetSkyline;
GO
--Create SP for BNL Levelized (without incomparable)
CREATE PROCEDURE prefSQL_SkylineBNLLevel (@Name nvarchar(MAX), @Operators nvarchar(200), @NumberOfRecords int, @SortType int)
AS EXTERNAL NAME SQLSkyline.[prefSQL.SQLSkyline.SPSkylineBNLLevel].GetSkyline;
GO
--Create SP for BNLSort (with Incomparable)
CREATE PROCEDURE prefSQL_SkylineBNLSort (@Name nvarchar(MAX), @Operators nvarchar(200), @NumberOfRecords int, @SortType int)
AS EXTERNAL NAME SQLSkyline.[prefSQL.SQLSkyline.SPSkylineBNLSort].GetSkyline;
GO
--Create SP for BNLSort Levelized (without incomparable)
CREATE PROCEDURE prefSQL_SkylineBNLSortLevel (@Name nvarchar(MAX), @Operators nvarchar(200), @NumberOfRecords int, @SortType int)
AS EXTERNAL NAME SQLSkyline.[prefSQL.SQLSkyline.SPSkylineBNLSortLevel].GetSkyline;
GO
--Create SP for DQ
CREATE PROCEDURE prefSQL_SkylineDQ (@Name nvarchar(MAX), @Operators nvarchar(200), @NumberOfRecords int, @SortType int)
AS EXTERNAL NAME SQLSkyline.[prefSQL.SQLSkyline.SPSkylineDQ].GetSkyline;
GO
--Create SP for Hexagon
CREATE PROCEDURE prefSQL_SkylineHexagon (@Name nvarchar(MAX), @Operators nvarchar(200), @NumberOfRecords int, @SortType int, @DistinctIncomparable nvarchar(MAX), @DistinctLevelIncomparable int)
AS EXTERNAL NAME SQLSkyline.[prefSQL.SQLSkyline.SPSkylineHexagon].GetSkyline;
GO
--Create SP for Hexagon Level
CREATE PROCEDURE prefSQL_SkylineHexagonLevel (@Name nvarchar(MAX), @Operators nvarchar(200), @NumberOfRecords int, @SortType int)
AS EXTERNAL NAME SQLSkyline.[prefSQL.SQLSkyline.SPSkylineHexagonLevel].GetSkyline;
GO
--Create SP for MultipleSkyline
CREATE PROCEDURE prefSQL_MultipleSkylineBNL (@Name nvarchar(MAX), @Operators nvarchar(200), @NumberOfRecords int,  @SortType int, @UpToLevel int)
AS EXTERNAL NAME SQLSkyline.[prefSQL.SQLSkyline.SPMultipleSkylineBNL].GetSkyline;
GO
--Create SP for MultipleSkylineLevel
CREATE PROCEDURE prefSQL_MultipleSkylineBNLLevel (@Name nvarchar(MAX), @Operators nvarchar(200), @NumberOfRecords int,  @SortType int, @UpToLevel int)
AS EXTERNAL NAME SQLSkyline.[prefSQL.SQLSkyline.SPMultipleSkylineBNLLevel].GetSkyline;
GO
--Create SP for SkylineSampling
CREATE PROCEDURE prefSQL_SkylineSampling (@Name nvarchar(MAX), @Operators nvarchar(200), @NumberOfRecords int, @SortType int, @Count int, @Dimension int, @Algorithm nvarchar(200), @HasIncomparable bit)
AS EXTERNAL NAME SQLSkyline.[prefSQL.SQLSkyline.SkylineSampling.SPSkylineSampling].GetSkyline;
GO

--Create SP for Ranking
CREATE PROCEDURE prefSQL_Ranking (@Name nvarchar(MAX), @SelectExtremas nvarchar(MAX),  @NumberOfRecords int, @RankingWeights nvarchar(MAX), @RankingExpressions nvarchar(MAX), @ShowInternalAttributes bit, @ColumnNames nvarchar(MAX))
AS EXTERNAL NAME SQLSkyline.[prefSQL.SQLSkyline.SPRanking].GetRanking;
GO


