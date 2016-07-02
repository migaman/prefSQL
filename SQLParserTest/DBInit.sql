--Placeholder [UnitTestDB] will be replaced during unit test with timestamp
CREATE DATABASE [UnitTestDB]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'[UnitTestDB]', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL11.MSSQLSERVER\MSSQL\DATA\[UnitTestDB].mdf' , SIZE = 31360KB , MAXSIZE = UNLIMITED, FILEGROWTH = 51200KB )
 LOG ON 
( NAME = N'[UnitTestDB]_Log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL11.MSSQLSERVER\MSSQL\DATA\[UnitTestDB]_Log.ldf' , SIZE = 26624KB , MAXSIZE = 2048GB , FILEGROWTH = 51200KB )
COLLATE SQL_Latin1_General_CP1_CI_AS
GO
ALTER DATABASE [UnitTestDB] SET COMPATIBILITY_LEVEL = 100
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [UnitTestDB].[dbo].[sp_fulltext_database] @action = 'disable'
end
GO
ALTER DATABASE [UnitTestDB] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [UnitTestDB] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [UnitTestDB] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [UnitTestDB] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [UnitTestDB] SET ARITHABORT OFF 
GO
ALTER DATABASE [UnitTestDB] SET AUTO_CLOSE OFF 
GO
ALTER DATABASE [UnitTestDB] SET AUTO_CREATE_STATISTICS ON 
GO
ALTER DATABASE [UnitTestDB] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [UnitTestDB] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [UnitTestDB] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [UnitTestDB] SET CURSOR_DEFAULT  GLOBAL 
GO
ALTER DATABASE [UnitTestDB] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [UnitTestDB] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [UnitTestDB] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [UnitTestDB] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [UnitTestDB] SET  DISABLE_BROKER 
GO
ALTER DATABASE [UnitTestDB] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [UnitTestDB] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO
ALTER DATABASE [UnitTestDB] SET TRUSTWORTHY OFF 
GO
ALTER DATABASE [UnitTestDB] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO
ALTER DATABASE [UnitTestDB] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [UnitTestDB] SET READ_COMMITTED_SNAPSHOT OFF 
GO
ALTER DATABASE [UnitTestDB] SET HONOR_BROKER_PRIORITY OFF 
GO
ALTER DATABASE [UnitTestDB] SET RECOVERY FULL 
GO
ALTER DATABASE [UnitTestDB] SET  MULTI_USER 
GO
ALTER DATABASE [UnitTestDB] SET PAGE_VERIFY CHECKSUM  
GO
ALTER DATABASE [UnitTestDB] SET DB_CHAINING OFF 
GO
ALTER DATABASE [UnitTestDB] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO
ALTER DATABASE [UnitTestDB] SET TARGET_RECOVERY_TIME = 0 SECONDS 
GO
USE [UnitTestDB]
GO


-----------------------------------------------------------------------
-----------------------------------------------------------------------
----------------T A B L E S--------------------------------------------
-----------------------------------------------------------------------
-----------------------------------------------------------------------


CREATE TABLE [dbo].[Bodies](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Name] [varchar](30) NULL,
 CONSTRAINT [PK_Bodies] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
CREATE TABLE [dbo].[Cars](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Make_Id] [int] NOT NULL,
	[Model_Id] [int] NOT NULL,
	[Title] [varchar](100) NULL,
	[Price] [int] NOT NULL,
	[Mileage] [int] NOT NULL,
	[Horsepower] [int] NOT NULL,
	[EngineSize] [int] NOT NULL,
	[Condition_Id] [int] NOT NULL,
	[Body_Id] [int] NOT NULL,
	[Fuel_Id] [int] NOT NULL,
	[Transmission_Id] [int] NOT NULL,
	[Drive_Id] [int] NOT NULL,
	[Color_Id] [int] NOT NULL,
	[Registration] [date] NOT NULL,
	[Pollution_Id] [int] NULL,
	[Efficiency_Id] [int] NULL,
	[Consumption] [int] NOT NULL,
	[Doors] [int] NOT NULL,
	[Seats] [int] NOT NULL,
	[Cylinders] [int] NOT NULL,
	[Gears] [int] NOT NULL,
	[Reference] [varchar](40) NULL,
	[RegistrationNumeric] [int] NULL,
 CONSTRAINT [PK_Cars] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
CREATE TABLE [dbo].[Colors](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Name] [varchar](20) NULL,
 CONSTRAINT [PK_Colors] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
CREATE TABLE [dbo].[Conditions](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Name] [varchar](20) NULL,
 CONSTRAINT [PK_Conditions] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
CREATE TABLE [dbo].[Drives](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Name] [varchar](20) NULL,
 CONSTRAINT [PK_Drives] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
CREATE TABLE [dbo].[Efficiencies](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Name] [varchar](20) NULL,
 CONSTRAINT [PK_Efficiencies] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
CREATE TABLE [dbo].[Fuels](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Name] [varchar](20) NULL,
 CONSTRAINT [PK_Fuels] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
CREATE TABLE [dbo].[Makes](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Name] [varchar](20) NULL,
 CONSTRAINT [PK_Makes] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
CREATE TABLE [dbo].[Models](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Name] [varchar](20) NULL,
 CONSTRAINT [PK_Models] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
CREATE TABLE [dbo].[Pollutions](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Name] [varchar](20) NULL,
 CONSTRAINT [PK_Pollutions] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
CREATE TABLE [dbo].[Transmissions](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Name] [varchar](20) NULL,
 CONSTRAINT [PK_Transmissions] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
ALTER TABLE [dbo].[Cars]  WITH NOCHECK ADD  CONSTRAINT [FK_Cars_Bodies_Body_Id] FOREIGN KEY([Body_Id])
REFERENCES [dbo].[Bodies] ([Id])
GO
ALTER TABLE [dbo].[Cars] CHECK CONSTRAINT [FK_Cars_Bodies_Body_Id]
GO
ALTER TABLE [dbo].[Cars]  WITH NOCHECK ADD  CONSTRAINT [FK_Cars_Colors_Color_Id] FOREIGN KEY([Color_Id])
REFERENCES [dbo].[Colors] ([Id])
GO
ALTER TABLE [dbo].[Cars] CHECK CONSTRAINT [FK_Cars_Colors_Color_Id]
GO
ALTER TABLE [dbo].[Cars]  WITH NOCHECK ADD  CONSTRAINT [FK_Cars_Conditions_Condition_Id] FOREIGN KEY([Condition_Id])
REFERENCES [dbo].[Conditions] ([Id])
GO
ALTER TABLE [dbo].[Cars] CHECK CONSTRAINT [FK_Cars_Conditions_Condition_Id]
GO
ALTER TABLE [dbo].[Cars]  WITH NOCHECK ADD  CONSTRAINT [FK_Cars_Drives_Drive_Id] FOREIGN KEY([Drive_Id])
REFERENCES [dbo].[Drives] ([Id])
GO
ALTER TABLE [dbo].[Cars] CHECK CONSTRAINT [FK_Cars_Drives_Drive_Id]
GO
ALTER TABLE [dbo].[Cars]  WITH NOCHECK ADD  CONSTRAINT [FK_Cars_Efficiencies_Efficiency_Id] FOREIGN KEY([Efficiency_Id])
REFERENCES [dbo].[Efficiencies] ([Id])
GO
ALTER TABLE [dbo].[Cars] CHECK CONSTRAINT [FK_Cars_Efficiencies_Efficiency_Id]
GO
ALTER TABLE [dbo].[Cars]  WITH NOCHECK ADD  CONSTRAINT [FK_Cars_Fuels_Fuel_Id] FOREIGN KEY([Fuel_Id])
REFERENCES [dbo].[Fuels] ([Id])
GO
ALTER TABLE [dbo].[Cars] CHECK CONSTRAINT [FK_Cars_Fuels_Fuel_Id]
GO
ALTER TABLE [dbo].[Cars]  WITH NOCHECK ADD  CONSTRAINT [FK_Cars_Makes_Make_Id] FOREIGN KEY([Make_Id])
REFERENCES [dbo].[Makes] ([Id])
GO
ALTER TABLE [dbo].[Cars] CHECK CONSTRAINT [FK_Cars_Makes_Make_Id]
GO
ALTER TABLE [dbo].[Cars]  WITH NOCHECK ADD  CONSTRAINT [FK_Cars_Models_Model_Id] FOREIGN KEY([Model_Id])
REFERENCES [dbo].[Models] ([Id])
GO
ALTER TABLE [dbo].[Cars] CHECK CONSTRAINT [FK_Cars_Models_Model_Id]
GO
ALTER TABLE [dbo].[Cars]  WITH NOCHECK ADD  CONSTRAINT [FK_Cars_Pollutions_Pollution_Id] FOREIGN KEY([Pollution_Id])
REFERENCES [dbo].[Pollutions] ([Id])
GO
ALTER TABLE [dbo].[Cars] CHECK CONSTRAINT [FK_Cars_Pollutions_Pollution_Id]
GO
ALTER TABLE [dbo].[Cars]  WITH NOCHECK ADD  CONSTRAINT [FK_Cars_Transmissions_Transmission_Id] FOREIGN KEY([Transmission_Id])
REFERENCES [dbo].[Transmissions] ([Id])
GO
ALTER TABLE [dbo].[Cars] CHECK CONSTRAINT [FK_Cars_Transmissions_Transmission_Id]
GO


-----------------------------------------------------------------------
-----------------------------------------------------------------------
------------------V I E W S--------------------------------------------
-----------------------------------------------------------------------
-----------------------------------------------------------------------

 
-----------------------------------------------------------------------
-----------------------------------------------------------------------
-------------S T O R E D  P R O C E D U R E S--------------------------
-----------------------------------------------------------------------
-----------------------------------------------------------------------

-----------------------------------------------------------------------
-----------------------------------------------------------------------
------------U S E R  D E F I N E D  F U N C T I O N S------------------
-----------------------------------------------------------------------
-----------------------------------------------------------------------


-----------------------------------------------------------------------
-----------------------------------------------------------------------
--------------I N D E X E S--------------------------------------------
-----------------------------------------------------------------------
-----------------------------------------------------------------------




-----------------------------------------------------------------------
-----------------------------------------------------------------------
---------------------D A T A--------------------------------------------
-----------------------------------------------------------------------
-----------------------------------------------------------------------

SET IDENTITY_INSERT [dbo].[Conditions] ON 

INSERT [dbo].[Conditions] ([Id], [Name]) VALUES (1, N'new')
INSERT [dbo].[Conditions] ([Id], [Name]) VALUES (2, N'occasion')
INSERT [dbo].[Conditions] ([Id], [Name]) VALUES (3, N'oldtimer')
INSERT [dbo].[Conditions] ([Id], [Name]) VALUES (4, N'demonstration car')
SET IDENTITY_INSERT [dbo].[Conditions] OFF
SET IDENTITY_INSERT [dbo].[Bodies] ON 

INSERT [dbo].[Bodies] ([Id], [Name]) VALUES (1, N'bus')
INSERT [dbo].[Bodies] ([Id], [Name]) VALUES (2, N'cabriolet')
INSERT [dbo].[Bodies] ([Id], [Name]) VALUES (3, N'coupé')
INSERT [dbo].[Bodies] ([Id], [Name]) VALUES (4, N'van')
INSERT [dbo].[Bodies] ([Id], [Name]) VALUES (5, N'compact car')
INSERT [dbo].[Bodies] ([Id], [Name]) VALUES (6, N'estate car')
INSERT [dbo].[Bodies] ([Id], [Name]) VALUES (7, N'minivan')
INSERT [dbo].[Bodies] ([Id], [Name]) VALUES (8, N'limousine')
INSERT [dbo].[Bodies] ([Id], [Name]) VALUES (9, N'pick-up')
INSERT [dbo].[Bodies] ([Id], [Name]) VALUES (10, N'scooter')
INSERT [dbo].[Bodies] ([Id], [Name]) VALUES (11, N'suv')
SET IDENTITY_INSERT [dbo].[Bodies] OFF
SET IDENTITY_INSERT [dbo].[Fuels] ON 

INSERT [dbo].[Fuels] ([Id], [Name]) VALUES (1, N'petrol')
INSERT [dbo].[Fuels] ([Id], [Name]) VALUES (2, N'bioethanol')
INSERT [dbo].[Fuels] ([Id], [Name]) VALUES (3, N'diesel')
INSERT [dbo].[Fuels] ([Id], [Name]) VALUES (4, N'electro')
INSERT [dbo].[Fuels] ([Id], [Name]) VALUES (5, N'gas')
INSERT [dbo].[Fuels] ([Id], [Name]) VALUES (6, N'hybrid')
SET IDENTITY_INSERT [dbo].[Fuels] OFF
SET IDENTITY_INSERT [dbo].[Transmissions] ON 

INSERT [dbo].[Transmissions] ([Id], [Name]) VALUES (1, N'automatic')
INSERT [dbo].[Transmissions] ([Id], [Name]) VALUES (2, N'manual')
SET IDENTITY_INSERT [dbo].[Transmissions] OFF
SET IDENTITY_INSERT [dbo].[Drives] ON 

INSERT [dbo].[Drives] ([Id], [Name]) VALUES (1, N'all wheel')
INSERT [dbo].[Drives] ([Id], [Name]) VALUES (2, N'rear wheel')
INSERT [dbo].[Drives] ([Id], [Name]) VALUES (3, N'front wheel')
SET IDENTITY_INSERT [dbo].[Drives] OFF
SET IDENTITY_INSERT [dbo].[Colors] ON 

INSERT [dbo].[Colors] ([Id], [Name]) VALUES (1, N'anthracite')
INSERT [dbo].[Colors] ([Id], [Name]) VALUES (2, N'beige')
INSERT [dbo].[Colors] ([Id], [Name]) VALUES (3, N'blue')
INSERT [dbo].[Colors] ([Id], [Name]) VALUES (4, N'bordeaux')
INSERT [dbo].[Colors] ([Id], [Name]) VALUES (5, N'brown')
INSERT [dbo].[Colors] ([Id], [Name]) VALUES (6, N'yellow')
INSERT [dbo].[Colors] ([Id], [Name]) VALUES (7, N'gold')
INSERT [dbo].[Colors] ([Id], [Name]) VALUES (8, N'gray')
INSERT [dbo].[Colors] ([Id], [Name]) VALUES (9, N'green')
INSERT [dbo].[Colors] ([Id], [Name]) VALUES (10, N'orange')
INSERT [dbo].[Colors] ([Id], [Name]) VALUES (11, N'pink')
INSERT [dbo].[Colors] ([Id], [Name]) VALUES (12, N'red')
INSERT [dbo].[Colors] ([Id], [Name]) VALUES (13, N'black')
INSERT [dbo].[Colors] ([Id], [Name]) VALUES (14, N'silver')
INSERT [dbo].[Colors] ([Id], [Name]) VALUES (15, N'turquoise')
INSERT [dbo].[Colors] ([Id], [Name]) VALUES (16, N'violet')
INSERT [dbo].[Colors] ([Id], [Name]) VALUES (17, N'white')
SET IDENTITY_INSERT [dbo].[Colors] OFF
SET IDENTITY_INSERT [dbo].[Pollutions] ON 

INSERT [dbo].[Pollutions] ([Id], [Name]) VALUES (1, N'Euro 1')
INSERT [dbo].[Pollutions] ([Id], [Name]) VALUES (2, N'Euro 2')
INSERT [dbo].[Pollutions] ([Id], [Name]) VALUES (3, N'Euro 3')
INSERT [dbo].[Pollutions] ([Id], [Name]) VALUES (4, N'Euro 4')
INSERT [dbo].[Pollutions] ([Id], [Name]) VALUES (5, N'Euro 5')
INSERT [dbo].[Pollutions] ([Id], [Name]) VALUES (6, N'Euro 6')
SET IDENTITY_INSERT [dbo].[Pollutions] OFF
SET IDENTITY_INSERT [dbo].[Efficiencies] ON 

INSERT [dbo].[Efficiencies] ([Id], [Name]) VALUES (1, N'A')
INSERT [dbo].[Efficiencies] ([Id], [Name]) VALUES (2, N'B')
INSERT [dbo].[Efficiencies] ([Id], [Name]) VALUES (3, N'C')
INSERT [dbo].[Efficiencies] ([Id], [Name]) VALUES (4, N'D')
INSERT [dbo].[Efficiencies] ([Id], [Name]) VALUES (5, N'E')
INSERT [dbo].[Efficiencies] ([Id], [Name]) VALUES (6, N'F')
INSERT [dbo].[Efficiencies] ([Id], [Name]) VALUES (7, N'G')
SET IDENTITY_INSERT [dbo].[Efficiencies] OFF


SET IDENTITY_INSERT [dbo].[Makes] ON 
INSERT [dbo].[Makes] ([Id], [Name]) VALUES (1, N'ALFA ROMEO')
SET IDENTITY_INSERT [dbo].[Makes] OFF

SET IDENTITY_INSERT [dbo].[Models] ON 
INSERT [dbo].[Models] ([Id], [Name]) VALUES (1, N'1007')
SET IDENTITY_INSERT [dbo].[Models] OFF


SET IDENTITY_INSERT [dbo].[Cars] ON
INSERT [dbo].[Cars] ([Id], [Make_Id], [Model_Id], [Title], [Price], [Mileage], [Horsepower], [EngineSize], [Condition_Id], [Body_Id], [Fuel_Id], [Transmission_Id], [Drive_Id], [Color_Id], [Registration], [Pollution_Id], [Efficiency_Id], [Consumption], [Doors], [Seats], [Cylinders], [Gears], [Reference], [RegistrationNumeric]) VALUES (1, 1, 1, 'car01', 44800, 27600, 320, 4973, 2, 2, 1, 1, 2, 1, CAST(N'1996-10-01' AS Date), 2, NULL, 143, 2, 2, 8, 5, NULL, 35337)
INSERT [dbo].[Cars] ([Id], [Make_Id], [Model_Id], [Title], [Price], [Mileage], [Horsepower], [EngineSize], [Condition_Id], [Body_Id], [Fuel_Id], [Transmission_Id], [Drive_Id], [Color_Id], [Registration], [Pollution_Id], [Efficiency_Id], [Consumption], [Doors], [Seats], [Cylinders], [Gears], [Reference], [RegistrationNumeric]) VALUES (2, 1, 1, 'car02', 69800, 12900, 394, 5987, 2, 2, 1, 1, 2, 3, CAST(N'1996-05-01' AS Date), 2, NULL, 155, 2, 2, 12, 5, NULL, 35184)
INSERT [dbo].[Cars] ([Id], [Make_Id], [Model_Id], [Title], [Price], [Mileage], [Horsepower], [EngineSize], [Condition_Id], [Body_Id], [Fuel_Id], [Transmission_Id], [Drive_Id], [Color_Id], [Registration], [Pollution_Id], [Efficiency_Id], [Consumption], [Doors], [Seats], [Cylinders], [Gears], [Reference], [RegistrationNumeric]) VALUES (3, 1, 1, 'car03', 22800, 18300, 286, 4398, 2, 8, 1, 1, 2, 1, CAST(N'1999-05-01' AS Date), 2, NULL, 133, 4, 5, 8, 5, NULL, 36279)
INSERT [dbo].[Cars] ([Id], [Make_Id], [Model_Id], [Title], [Price], [Mileage], [Horsepower], [EngineSize], [Condition_Id], [Body_Id], [Fuel_Id], [Transmission_Id], [Drive_Id], [Color_Id], [Registration], [Pollution_Id], [Efficiency_Id], [Consumption], [Doors], [Seats], [Cylinders], [Gears], [Reference], [RegistrationNumeric]) VALUES (4, 1, 1, 'car04', 44800, 25100, 224, 3199, 2, 2, 1, 1, 2, 14, CAST(N'2000-04-01' AS Date), 3, 7, 115, 2, 2, 6, 5, NULL, 36615)
INSERT [dbo].[Cars] ([Id], [Make_Id], [Model_Id], [Title], [Price], [Mileage], [Horsepower], [EngineSize], [Condition_Id], [Body_Id], [Fuel_Id], [Transmission_Id], [Drive_Id], [Color_Id], [Registration], [Pollution_Id], [Efficiency_Id], [Consumption], [Doors], [Seats], [Cylinders], [Gears], [Reference], [RegistrationNumeric]) VALUES (5, 1, 1, 'car05', 199800, 300, 462, 3600, 1, 3, 1, 2, 2, 13, CAST(N'2004-01-01' AS Date), 3, 7, 129, 2, 4, 6, 6, NULL, 37985)
INSERT [dbo].[Cars] ([Id], [Make_Id], [Model_Id], [Title], [Price], [Mileage], [Horsepower], [EngineSize], [Condition_Id], [Body_Id], [Fuel_Id], [Transmission_Id], [Drive_Id], [Color_Id], [Registration], [Pollution_Id], [Efficiency_Id], [Consumption], [Doors], [Seats], [Cylinders], [Gears], [Reference], [RegistrationNumeric]) VALUES (6, 1, 1, 'car06', 14800, 63000, 286, 4398, 2, 8, 1, 1, 2, 9, CAST(N'1998-02-01' AS Date), 2, NULL, 130, 4, 5, 8, 5, NULL, 35825)
INSERT [dbo].[Cars] ([Id], [Make_Id], [Model_Id], [Title], [Price], [Mileage], [Horsepower], [EngineSize], [Condition_Id], [Body_Id], [Fuel_Id], [Transmission_Id], [Drive_Id], [Color_Id], [Registration], [Pollution_Id], [Efficiency_Id], [Consumption], [Doors], [Seats], [Cylinders], [Gears], [Reference], [RegistrationNumeric]) VALUES (7, 1, 1, 'car07', 109800, 150, 354, 3506, 1, 3, 1, 2, 2, 3, CAST(N'2002-06-01' AS Date), NULL, 7, 157, 2, 2, 8, 5, NULL, 37406)
INSERT [dbo].[Cars] ([Id], [Make_Id], [Model_Id], [Title], [Price], [Mileage], [Horsepower], [EngineSize], [Condition_Id], [Body_Id], [Fuel_Id], [Transmission_Id], [Drive_Id], [Color_Id], [Registration], [Pollution_Id], [Efficiency_Id], [Consumption], [Doors], [Seats], [Cylinders], [Gears], [Reference], [RegistrationNumeric]) VALUES (8, 1, 1, 'car08', 34800, 27800, 299, 4966, 2, 3, 1, 1, 2, 14, CAST(N'2001-01-01' AS Date), 4, 7, 124, 2, 4, 8, 5, NULL, 36890)
INSERT [dbo].[Cars] ([Id], [Make_Id], [Model_Id], [Title], [Price], [Mileage], [Horsepower], [EngineSize], [Condition_Id], [Body_Id], [Fuel_Id], [Transmission_Id], [Drive_Id], [Color_Id], [Registration], [Pollution_Id], [Efficiency_Id], [Consumption], [Doors], [Seats], [Cylinders], [Gears], [Reference], [RegistrationNumeric]) VALUES (9, 1, 1, 'car09', 29900, 39600, 306, 4966, 2, 3, 1, 1, 2, 4, CAST(N'2000-01-01' AS Date), 4, 7, 124, 2, 4, 8, 7, NULL, 36524)
SET IDENTITY_INSERT [dbo].[Cars] OFF

--------------------------------------------------------------------------
--------------------------------------------------------------------------
---------------------------- Activate Readmode for DB --------------------
--------------------------------------------------------------------------
--------------------------------------------------------------------------


--USE [master]
GO
ALTER DATABASE [UnitTestDB] SET  READ_WRITE 
GO
