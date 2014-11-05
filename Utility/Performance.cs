using System;
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
        public void GeneratePerformanceQueries(SQLCommon.Algorithm algorithmType)
        {
            //Add more columns
            String[] columns = { "cars.price", "cars.mileage", "cars.horsepower", "cars.enginesize", "cars.registration", "cars.consumption", "cars.doors", "colors.name", "fuels.name", "bodies.name", "cars.title", "makes.name", "conditions.name" };
            //Use the correct line, depending on how incomparable items should be compared
            //String[] preferences = { "LOW cars.price", "LOW cars.mileage", "HIGH cars.horsepower", "HIGH cars.enginesize", "HIGH cars.registration", "LOW cars.consumption", "HIGH cars.doors", "HIGH colors.name {'rot' == 'blau' >> OTHERS >> 'grau'}", "HIGH fuels.name {'Benzin' >> OTHERS >> 'Diesel'}", "HIGH bodies.name {'Kleinwagen' >> 'Bus' >> 'Kombi' >> 'Roller' >> OTHERS >> 'Pick-Up'}", "HIGH cars.title {'MERCEDES-BENZ SL 600' >> OTHERS}", "HIGH makes.name {'ASTON MARTIN' >> 'VW' == 'Audi' >> OTHERS >> 'FERRARI'}", "HIGH conditions.name {'Neu' >> OTHERS}" };
            String[] preferences = { "LOW cars.price", "LOW cars.mileage", "HIGH cars.horsepower", "HIGH cars.enginesize", "HIGH cars.registration", "LOW cars.consumption", "HIGH cars.doors", "HIGH colors.name {'rot' == 'blau' >> OTHERSEQUAL >> 'grau'}", "HIGH fuels.name {'Benzin' >> OTHERSEQUAL >> 'Diesel'}", "HIGH bodies.name {'Kleinwagen' >> 'Bus' >> 'Kombi' >> 'Roller' >> OTHERSEQUAL >> 'Pick-Up'}", "HIGH cars.title {'MERCEDES-BENZ SL 600' >> OTHERSEQUAL}", "HIGH makes.name {'ASTON MARTIN' >> 'VW' == 'Audi' >> OTHERSEQUAL >> 'FERRARI'}", "HIGH conditions.name {'Neu' >> OTHERSEQUAL}" };
            String[] sizes = { "small", "medium", "large", "superlarge" };
            

            StringBuilder sb = new StringBuilder();
            sb.AppendLine("set statistics time on");


            for (int i = columns.GetUpperBound(0); i >= 1; i--)
            {
                //SELECT FROM
                String strSQL = "SELECT " + string.Join(",", columns) + " FROM cars ";
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
                strSQL += "PREFERENCE " + string.Join(" AND ", preferences);


                //Convert to real SQL
                SQLCommon parser = new SQLCommon();
                parser.SkylineType = algorithmType; // SQLCommon.Algorithm.NativeSQL;
                strSQL = parser.parsePreferenceSQL(strSQL);


                //Format for each of the customer profiles
                sb.AppendLine("PRINT '----- -------------------------------------------------------- ------'");
                sb.AppendLine("PRINT '----- " + (i + 1) + " dimensions, " + (countJoins) + " join(s) ------'");
                sb.AppendLine("PRINT '----- -------------------------------------------------------- ------'");
                foreach (String size in sizes)
                {
                    sb.AppendLine("GO"); //we need this in order the profiler shows each query in a new line
                    sb.AppendLine(strSQL.Replace("cars", "cars_" + size));

                }


                //strPerformanceQuery += "\n\n\n";
                sb.AppendLine("");
                sb.AppendLine("");
                sb.AppendLine("");
                //Debug.WriteLine();




                //Remove current column
                columns = columns.Where(w => w != columns[i]).ToArray();
                preferences = preferences.Where(w => w != preferences[i]).ToArray();
            }
            //
            sb.AppendLine("set statistics time off");

            //Write in file
            String strFileName = "";
            if(algorithmType == SQLCommon.Algorithm.NativeSQL) 
            {
                strFileName = "E:\\Doc\\Studies\\PRJ_Thesis\\10 Arcmedia Profiles\\Performance_Native_Auto.sql";
            }
            else
            {
                strFileName = "E:\\Doc\\Studies\\PRJ_Thesis\\10 Arcmedia Profiles\\Performance_BNL_Auto.sql";
            }
            StreamWriter outfile = new StreamWriter(strFileName);
            outfile.Write(sb.ToString());
            outfile.Close();
            Debug.WriteLine("THE END!!");

        }

    }
}
