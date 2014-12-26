﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using prefSQL.SQLParser;
using System.IO;
using System.Diagnostics;

namespace Utility
{
    class Performance
    {
        private const string path = "E:\\Doc\\Studies\\PRJ_Thesis\\15 Performance\\";

        public void GeneratePerformanceQueries(SQLCommon.Algorithm algorithmType, bool withIncomparable, bool withLeveling, bool includeDate)
        {
            //Add more columns
           string[] columns;
            if (includeDate == true)
            {
                columns = new string[] { "cars.price", "cars.mileage", "cars.horsepower", "cars.enginesize", "cars.registration", "cars.consumption", "cars.doors", "colors.name", "fuels.name", "bodies.name", "cars.title", "makes.name", "conditions.name" };
            }
            else
            {
                columns = new string[] { "cars.price", "cars.mileage", "cars.horsepower", "cars.enginesize", "cars.consumption", "cars.doors", "colors.name", "fuels.name", "bodies.name", "cars.title", "makes.name", "conditions.name" };
            }
            
            
            //Use the correct line, depending on how incomparable items should be compared
            string[] preferences;
            if (withIncomparable == true)
            {
                preferences = new string[] { "cars.price LOW", "cars.mileage LOW", "cars.horsepower HIGH", "cars.enginesize HIGH", "cars.registration HIGHDATE", "cars.consumption LOW", "cars.doors HIGH", "colors.name ('rot' == 'blau' >> OTHERS INCOMPARABLE >> 'grau')", "fuels.name ('Benzin' >> OTHERS INCOMPARABLE >> 'Diesel')", "bodies.name ('Kleinwagen' >> 'Bus' >> 'Kombi' >> 'Roller' >> OTHERS INCOMPARABLE >> 'Pick-Up')", "cars.title ('MERCEDES-BENZ SL 600' >> OTHERS INCOMPARABLE)", "makes.name ('ASTON MARTIN' >> 'VW' == 'Audi' >> OTHERS INCOMPARABLE >> 'FERRARI')", "conditions.name ('Neu' >> OTHERS INCOMPARABLE)" };
            }
            else
            {
                if (withLeveling == true && includeDate == true)
                {
                    preferences = new string[] { "cars.price LOW 100000", "cars.mileage LOW 100000", "cars.horsepower HIGH 100", "cars.enginesize HIGH 1000", "cars.registration HIGHDATE", "cars.consumption LOW 5", "cars.doors HIGH", "colors.name ('rot' == 'blau' >> OTHERS EQUAL >> 'grau')", "fuels.name ('Benzin' >> OTHERS EQUAL >> 'Diesel')", "bodies.name ('Kleinwagen' >> 'Bus' >> 'Kombi' >> 'Roller' >> OTHERS EQUAL >> 'Pick-Up')", "cars.title ('MERCEDES-BENZ SL 600' >> OTHERS EQUAL)", "makes.name ('ASTON MARTIN' >> 'VW' == 'Audi' >> OTHERS EQUAL >> 'FERRARI')", "conditions.name ('Neu' >> OTHERS EQUAL)" };
                }
                else if (withLeveling == true)
                {
                    preferences = new string[] { "cars.price LOW 100000", "cars.mileage LOW 100000", "cars.horsepower HIGH 100", "cars.enginesize HIGH 1000", "cars.consumption LOW 5", "cars.doors HIGH", "colors.name ('rot' == 'blau' >> OTHERS EQUAL >> 'grau')", "fuels.name ('Benzin' >> OTHERS EQUAL >> 'Diesel')", "bodies.name ('Kleinwagen' >> 'Bus' >> 'Kombi' >> 'Roller' >> OTHERS EQUAL >> 'Pick-Up')", "cars.title ('MERCEDES-BENZ SL 600' >> OTHERS EQUAL)", "makes.name ('ASTON MARTIN' >> 'VW' == 'Audi' >> OTHERS EQUAL >> 'FERRARI')", "conditions.name ('Neu' >> OTHERS EQUAL)" };
                }
                else if(includeDate == true)
                {
                    preferences = new string[] { "cars.price LOW", "cars.mileage LOW", "cars.horsepower HIGH", "cars.enginesize HIGH", "cars.registration HIGHDATE", "cars.consumption LOW", "cars.doors HIGH", "colors.name ('rot' == 'blau' >> OTHERS EQUAL >> 'grau')", "fuels.name ('Benzin' >> OTHERS EQUAL >> 'Diesel')", "bodies.name ('Kleinwagen' >> 'Bus' >> 'Kombi' >> 'Roller' >> OTHERS EQUAL >> 'Pick-Up')", "cars.title ('MERCEDES-BENZ SL 600' >> OTHERS EQUAL)", "makes.name ('ASTON MARTIN' >> 'VW' == 'Audi' >> OTHERS EQUAL >> 'FERRARI')", "conditions.name ('Neu' >> OTHERS EQUAL)" };
                }
                else
                {
                    preferences = new string[] { "cars.price LOW", "cars.mileage LOW", "cars.horsepower HIGH", "cars.enginesize HIGH", "cars.consumption LOW", "cars.doors HIGH", "colors.name ('rot' == 'blau' >> OTHERS EQUAL >> 'grau')", "fuels.name ('Benzin' >> OTHERS EQUAL >> 'Diesel')", "bodies.name ('Kleinwagen' >> 'Bus' >> 'Kombi' >> 'Roller' >> OTHERS EQUAL >> 'Pick-Up')", "cars.title ('MERCEDES-BENZ SL 600' >> OTHERS EQUAL)", "makes.name ('ASTON MARTIN' >> 'VW' == 'Audi' >> OTHERS EQUAL >> 'FERRARI')", "conditions.name ('Neu' >> OTHERS EQUAL)" };
                }
            }

            
            string[] sizes = { "small", "medium", "large", "superlarge" };
            StringBuilder sb = new StringBuilder();

            for (int i = columns.GetUpperBound(0); i >= 0; i--)
            {
                //SELECT FROM
                string strSQL = "SELECT " + string.Join(",", columns) + " FROM cars ";
                int countJoins = 0;

                //Add Joins
                if (strSQL.IndexOf("colors") > 0)
                {
                    strSQL += "LEFT OUTER JOIN colors ON cars.color_id = colors.ID ";
                    countJoins++;
                }
                if (strSQL.IndexOf("fuels") > 0)
                {
                    strSQL += "LEFT OUTER JOIN fuels ON cars.fuel_id = fuels.ID ";
                    countJoins++;
                }
                if (strSQL.IndexOf("bodies") > 0)
                {
                    strSQL += "LEFT OUTER JOIN bodies ON cars.body_id = bodies.ID ";
                    countJoins++;
                }
                if (strSQL.IndexOf("makes") > 0)
                {
                    strSQL += "LEFT OUTER JOIN makes ON cars.make_id = makes.ID ";
                    countJoins++;
                }
                if (strSQL.IndexOf("conditions") > 0)
                {
                    strSQL += "LEFT OUTER JOIN conditions ON cars.condition_id = conditions.ID ";
                    countJoins++;
                }


                //ADD Preferences
                strSQL += "SKYLINE OF " + string.Join(", ", preferences);


                //Convert to real SQL
                SQLCommon parser = new SQLCommon();
                parser.SkylineType = algorithmType; // SQLCommon.Algorithm.NativeSQL;
                strSQL = parser.parsePreferenceSQL(strSQL);


                //Format for each of the customer profiles
                sb.AppendLine("PRINT '----- -------------------------------------------------------- ------'");
                sb.AppendLine("PRINT '----- " + (i + 1) + " dimensions, " + (countJoins) + " join(s) ------'");
                sb.AppendLine("PRINT '----- -------------------------------------------------------- ------'");
                foreach (string size in sizes)
                {
                    sb.AppendLine("GO"); //we need this in order the profiler shows each query in a new line
                    sb.AppendLine(strSQL.Replace("cars", "cars_" + size));

                }

                sb.AppendLine("");
                sb.AppendLine("");
                sb.AppendLine("");

                //Remove current column
                columns = columns.Where(w => w != columns[i]).ToArray();
                preferences = preferences.Where(w => w != preferences[i]).ToArray();
            }

            //Write in file
            string strFileName = "";
            string strIncomparable = "Level";
            if( withIncomparable == true)
            {
                strIncomparable = "Incomparable";
            }
            if(algorithmType == SQLCommon.Algorithm.NativeSQL) 
            {
                strFileName = path + "Performance_Native_" + strIncomparable + ".sql";
            }
            else if(algorithmType == SQLCommon.Algorithm.Hexagon)
            {
                strFileName = path + "Performance_Hexagon_" + strIncomparable + ".sql";
            }
            else if(algorithmType == SQLCommon.Algorithm.BNLSort)
            {
                strFileName = path + "Performance_BNLSort_" + strIncomparable + ".sql";
            }
            else
            {
                strFileName = path + "Performance_BNL_" + strIncomparable + ".sql";
            }
            StreamWriter outfile = new StreamWriter(strFileName);
            outfile.Write(sb.ToString());
            outfile.Close();
            Debug.WriteLine("THE END!!");

        }

    }
}
