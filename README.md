# [prefSQL] (https://abiz.ch/ecom)

[![Build Status](https://ci.appveyor.com/api/projects/status/458h4u5v0qbh2tr7?svg=true)](https://ci.appveyor.com/project/migaman/prefsql)

prefSQL is a framework for preference-driven querying of relational databases. It allows to query for preferences (soft criteria) instead of knockout criteria.
To get started, check out <https://abiz.ch/ecom>!

### What's included
The main components are a parser and an algorithm library. 

The parser library translates the prefSQL language to standard SQL.
The algorithm library adds additional operators and functions like a skyline operator to the project.
The algorithm library is written for integration into the MS SQL Server Common Language Runtime (CLR). 
However, it can also be used standalone, for example to query against another relational database like MySQL.
 

## Documentation

The Documents folder, included in this repo in the root directory, contains information on how to install and use the framework.


## Performance

Overview of the Skyline algorithms using real data.

| Algorithm     | Dimensions    | Records  | Skyline size   | Avg. time [ms]| Min.Time [ms]| Max. Time [ms]|
|---------------|--------------:| --------:|---------------:|--------------:|-------------:|--------------:|
| BNL           | 7             | 55208    | 0 				| 0 		    | 0 	  	   | 0    		   |
| D&Q           | 7             | 55208    | 0 				| 0 		    | 0 		   | 0 			   |
| Hexagon       | 7             | 55208    | 0 				| 0 		    | 0 		   | 0 			   |
| Native SQL    | 7             | 55208    | 0 				| 0 		    | 0 		   | 0 			   |


## Data

You can find CSV data files in the Data folder.
You can find MS SQL database in the SQLParserTest folder.

## License
Code released under [the BSD license](https://github.com/migaman/prefSQL/blob/master/LICENSE.txt).
