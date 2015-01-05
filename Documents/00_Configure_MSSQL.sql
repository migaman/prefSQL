/******************************
** File:    00_Configure_MSSQL
** Name:	Configure MSSQL Instance
** Desc:	Configure MSSQL Instance for using CLR Assemblies
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
------------------ ENABLE ASSEMBLY SUPPORT -----------------------
------------------------------------------------------------------

--CLR aktivieren
EXEC sp_configure 'show advanced options' , '1'
GO
RECONFIGURE
GO
EXEC sp_configure 'clr enabled' , '1'
GO
RECONFIGURE
GO
EXEC sp_configure 'show advanced options' , '0';
GO

