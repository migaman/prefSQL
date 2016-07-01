prefSQL
=======
prefSQL is a framework for preference-driven querying of relational databases. It allows to query for preferences (soft criteria) instead of knockout criteria.
The main components are a parser and an algorithm library. 

The parser library translates the prefSQL language to standard SQL.
The algorithm library adds additional operators and functions like a skyline operator to the project.
The algorithm library is written for integration into the MS SQL Server Common Language Runtime (CLR). 
However, it can also be used standalone, for example to query against another relational database like MySQL.
 
The Documents folder contains information on how to install and use the framework.


Continuous Integration/Deployment
=======
###### - AppVeyor [![Build status](https://ci.appveyor.com/api/projects/status/458h4u5v0qbh2tr7?svg=true)](https://ci.appveyor.com/project/migaman/prefsql)

