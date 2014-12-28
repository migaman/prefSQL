using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using prefSQL.SQLParser;
using System.IO;
using System.Diagnostics;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

namespace Utility
{
    class Performance
    {
        private const string path = "E:\\Doc\\Studies\\PRJ_Thesis\\19 Performance Level\\";
        private const string cnnStringLocalhost = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
        
        public void GeneratePerformanceQueries(SQLCommon.Algorithm algorithmType, bool withIncomparable, bool withLeveling, bool includeDate, bool doExecute)
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
                if (withLeveling == true)
                {
                    preferences = new string[] { "cars.price LOW 60000", "cars.mileage LOW 60000", "cars.horsepower HIGH 80", "cars.enginesize HIGH 1000", "cars.consumption LOW 30", "colors.name ('rot' >> 'blau' >> OTHERS EQUAL >> 'grau')", "fuels.name ('Benzin' >> OTHERS EQUAL >> 'Diesel')", "bodies.name ('Kleinwagen' >> 'Bus' >> 'Kombi' >> 'Roller' >> OTHERS EQUAL >> 'Pick-Up')", "makes.name ('ASTON MARTIN' >> 'VW' == 'Audi' >> OTHERS EQUAL >> 'FERRARI')", "conditions.name ('Neu' >> OTHERS EQUAL)" };
                    columns = new string[] { "cars.price", "cars.mileage", "cars.horsepower", "cars.enginesize", "cars.consumption", "colors.name", "fuels.name", "bodies.name", "makes.name", "conditions.name" };
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

                if (doExecute == true)
                {
                    Stopwatch sw = new Stopwatch();
                    sw.Start();
                    try
                    {
                        /*
                        Program prg = new Program();
                        prg.executeDb(strSQL, SQLCommon.Algorithm.BNL);
                        */
                        //Native SQL
                        SqlConnection connection = null;
                        connection = new SqlConnection(cnnStringLocalhost);
                        
                        connection.Open();

                        SqlDataAdapter dap = new SqlDataAdapter(strSQL, connection);
                        dap.SelectCommand.CommandTimeout = 0;
                        DataTable dt = new DataTable();
                        dap.Fill(dt);

                        sw.Stop();

                        System.Diagnostics.Debug.WriteLine(dt.Rows.Count);
                        
                        sb.AppendLine(i + 1 + ";" + dt.Rows.Count + ";" + sw.ElapsedMilliseconds);
                        
                    }
                    catch (Exception e)
                    {
                        Debug.WriteLine(e.Message);
                    }
                }
                else
                {
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
                }
                //Remove current column
                columns = columns.Where(w => w != columns[i]).ToArray();
                preferences = preferences.Where(w => w != preferences[i]).ToArray();
            }

            //Write in file
            string strFileName = "";
            string strIncomparable = "Level";
            string strFiletype = "";
            if( withIncomparable == true)
            {
                strIncomparable = "Incomparable";
            }
            if(doExecute == true)
            {
                strFiletype = ".csv";
            }
            else
            {
                strFiletype = ".sql";
            }
            if(algorithmType == SQLCommon.Algorithm.NativeSQL) 
            {
                strFileName = path + "Performance_Native_" + strIncomparable + strFiletype;
            }
            else if(algorithmType == SQLCommon.Algorithm.Hexagon)
            {
                strFileName = path + "Performance_Hexagon_" + strIncomparable + strFiletype;
            }
            else if(algorithmType == SQLCommon.Algorithm.BNLSort)
            {
                strFileName = path + "Performance_BNLSort_" + strIncomparable + strFiletype;
            }
            else
            {
                strFileName = path + "Performance_BNL_" + strIncomparable + strFiletype;
            }
            StreamWriter outfile = new StreamWriter(strFileName);
            outfile.Write(sb.ToString());
            outfile.Close();
            Debug.WriteLine("THE END!!");

        }

    }
}
