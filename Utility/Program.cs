using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using prefSQL.SQLParser;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Threading;

namespace Utility
{
    class Program
    {
        private const string cnnStringLocalhost = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";

        static void Main(string[] args)
        {
            /*
            Performance p = new Performance();
            p.GeneratePerformanceQueries(SQLCommon.Algorithm.NativeSQL, true, Performance.PreferenceSet.Mya);
             * */
            /*p.GeneratePerformanceQueries(prefSQL.SQLParser.SQLCommon.Algorithm.BNL,             false, true, false, true);
            p.GeneratePerformanceQueries(prefSQL.SQLParser.SQLCommon.Algorithm.BNLLevel,        false, true, false, true);
            p.GeneratePerformanceQueries(prefSQL.SQLParser.SQLCommon.Algorithm.BNLSort,         false, true, false, true);*/
            //p.GeneratePerformanceQueries(prefSQL.SQLParser.SQLCommon.Algorithm.BNLSortLevel,    false, true, false, true);
            //p.GeneratePerformanceQueries(prefSQL.SQLParser.SQLCommon.Algorithm.Hexagon,         false, true, false, true);
            //p.GeneratePerformanceQueries(prefSQL.SQLParser.SQLCommon.Algorithm.NativeSQL,       false, true, false, true);
            

            /*
            DominanceGraph graph = new DominanceGraph();
            graph.run();
            */

            
            
            Program prg = new Program();
            prg.Run();
            
            
            /*
            FrmSQLParser form = new FrmSQLParser();
            form.Show();
            */
        }



        public void Run()
        {
            try
            {
                //Ablauf aktuell
                /*
                    1.Pareto Front
                    2.Filter auf die Kritik (z.B. Preis < 50000')
                    3.Pareto Front aufgrund der Präferenzen
                    4.Similarity berechnen
                */

                //string strPrefSQL = "SELECT cars.id, cars.title, colors.name, fuels.name FROM cars " +
                //string strPrefSQL = "SELECT cars.id, cars.title, cars.price, colors.name, mileage FROM cars " +
                string strPrefSQL = "SELECT t1.id, t1.title, colors.name AS Colour, bodies.name AS Body, conditions.name AS Condition FROM cars t1 " +
                    //string strPrefSQL = "SELECT cars.id, cars.Price, cars.mileage FROM cars " +
                    //string strPrefSQL = "SELECT cars.id, cars.title, cars.price, cars.mileage, cars.horsepower, cars.enginesize, cars.registration, cars.consumption, cars.doors, colors.name, fuels.name FROM cars " +
                    //string strPrefSQL = "SELECT cars.id, cars.title, colors.name AS colourname, fuels.name AS fuelname, cars.price FROM cars " +
                    //string strPrefSQL = "SELECT id FROM cars " +
                    "LEFT OUTER JOIN colors ON t1.color_id = colors.ID " +
                    "LEFT OUTER JOIN bodies ON t1.body_id = bodies.ID " +
                    "LEFT OUTER JOIN conditions ON t1.condition_id = conditions.id " +
                    "LEFT OUTER JOIN Transmissions ON t1.transmission_id = Transmissions.id " +
                    "LEFT OUTER JOIN Fuels ON t1.fuel_id = Fuels.id " +
                    "LEFT OUTER JOIN Drives ON t1.drive_id = Drives.id " +
                    "LEFT OUTER JOIN Pollutions ON t1.pollution_id = Pollutions.id " +
                    "LEFT OUTER JOIN Efficiencies ON t1.efficiency_id = Efficiencies.id " +
                    "LEFT OUTER JOIN Makes ON t1.make_id = Makes.id " +
                    "LEFT OUTER JOIN Models ON t1.model_id = Models.id " +

                    //"LEFT OUTER JOIN fuels ON cars.fuel_id = fuels.ID " +
                    //"WHERE t1.id NOT IN (54521, 25612, 46268, 668, 47392, 1012, 22350, 55205, 51017) " +
                    "SKYLINE OF t1.price LOW, t1.mileage LOW ";
                    //"SKYLINE OF HIGH t2.name {'schwarz' >> OTHERS} AND LOW t1.price AND HIGH t1.horsepower";
                    //"SKYLINE OF HIGH colors.name {'rot' == 'blau' >> OTHERS >> 'grau'} AND HIGH cars.registration";
                    //"SKYLINE OF t1.price LOW AND t1.title ('MERCEDES-BENZ SL 600' >> OTHERS EQUAL) ORDER BY t1.price, t1.mileage ";

                //"SKYLINE OF t1.horsepower HIGH, t1.price LOW 10000, t1.mileage LOW 10000, t2.name ('schwarz' >> 'rot' >> OTHERS EQUAL), t1.title ('MERCEDES-BENZ SL 600' >> OTHERS EQUAL)";
                    //"SKYLINE OF t1.price LOW, t1.mileage LOW, t2.name ('pink' >> 'rot' == 'schwarz' >> 'beige' == 'gelb' >> OTHERS EQUAL), t1.consumption LOW 50, t1.enginesize HIGH 1000 " +
                    //"SKYLINE OF t1.price LOW 60000, t1.horsepower HIGH 80";
                    //"SKYLINE OF t1.price LOW, t1.horsepower HIGH, t1.registration HIGHDATE, t1.consumption LOW, t1.mileage LOW, t1.enginesize HIGH ";
                //"SKYLINE OF Fuels.name ('Benzin' >> OTHERS EQUAL), Makes.name ('FISKER' >> OTHERS EQUAL)   " +
                //", bodies.name ('Roller' >> OTHERS EQUAL), models.name ('123' >> OTHERS EQUAL) "; 
                //EXEC dbo.SP_SkylineBNLSort 'SELECT  CASE WHEN Fuels.name = ''Benzin'' THEN 0 ELSE 100 END AS SkylineAttribute0, CASE WHEN conditions.name = ''Vorführmodell'' THEN 0 WHEN conditions.name = ''dd'' THEN 100 END AS SkylineAttribute1 , t1.id, t1.title, colors.name AS Colour, bodies.name AS Body, conditions.name AS Condition FROM cars t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID LEFT OUTER JOIN bodies ON t1.body_id = bodies.ID LEFT OUTER JOIN conditions ON t1.condition_id = conditions.id LEFT OUTER JOIN Transmissions ON t1.transmission_id = Transmissions.id LEFT OUTER JOIN Fuels ON t1.fuel_id = Fuels.id LEFT OUTER JOIN Drives ON t1.drive_id = Drives.id LEFT OUTER JOIN Pollutions ON t1.pollution_id = Pollutions.id LEFT OUTER JOIN Efficiencies ON t1.efficiency_id = Efficiencies.id LEFT OUTER JOIN Makes ON t1.make_id = Makes.id ORDER BY CASE WHEN Fuels.name = ''Benzin'' THEN 0 ELSE 100 END ASC, CASE WHEN conditions.name = ''Vorführmodell'' THEN 0 WHEN conditions.name = ''dd'' THEN 100 END ASC', 'LOW;LOW', 'false'
                    //t2.name ('schwarz' >> 'pink' >> 'blau' >> 'rot' >> OTHERS EQUAL), bodies.name ('Limousine' >> 'Roller' >> 'Coupé' >> OTHERS EQUAL) " +
                //", drives.name ('Vorderradantrieb' >> OTHERS EQUAL), Transmissions.name ('Schaltgetriebe' >> 'Automat')  " +
                //", Pollutions.name ('Euro 5' >> OTHERS EQUAL), bodies.name ('Limousine' >> OTHERS EQUAL) ";
                /*", colors.name ('schwarz' >> OTHERS EQUAL), efficiencies.name ('G' >> OTHERS EQUAL) " +
                //", makes.name ('VW' >> OTHERS EQUAL), t1.price LOW ";
                */
                //"SKYLINE OF LOW t1.price PRIORITIZE LOW t1.mileage";
                //"SKYLINE OF LOW t1.price PRIORITIZE LOW t1.mileage PRIORITIZE HIGH t2.name {OTHERS >> 'pink'}";
                //"SKYLINE OF cars.price AROUND 10000 ";
                //"SKYLINE OF HIGH colors.name {'rot' >> OTHERS EQUAL} AND cars.price AROUND 10000";
                //"SKYLINE OF cars.price AROUND 10000 AND HIGH colors.name {'rot' >> OTHERS EQUAL}";
                //"SKYLINE OF LOW cars.price AND HIGH colors.name {'rot' >> OTHERS EQUAL} ";
                //"SKYLINE OF LOW cars.price AND LOW cars.mileage AND HIGH cars.horsepower AND HIGH cars.enginesize AND HIGH cars.registration AND LOW cars.consumption AND HIGH cars.doors AND HIGH colors.name {'rot' == 'blau' >> OTHERS >> 'grau'} AND HIGH fuels.name {'Benzin' >> OTHERS >> 'Diesel'}";
                //"SKYLINE OF LOW cars.price AND LOW cars.mileage AND HIGH fuels.name {'Benzin' >> OTHERS >> 'Diesel'}";
                //"SKYLINE OF LOW cars.price AND HIGH colors.name {'rot' >> OTHERS}";
                //"SKYLINE OF LOW cars.price AND HIGH colors.name {'pink' >> 'rot' == 'schwarz'}";
                //"SKYLINE OF LOW cars.price AND HIGH colors.name {'pink' >> {'rot', 'schwarz'} >> 'beige' >> OTHERS}";
                //"ORDER BY SUM_RANK() ";
                //"SKYLINE OF HIGH colors.name {'gelb' >> OTHERS >> 'grau'} AND HIGH fuels.name {'Benzin' >> OTHERS >> 'Diesel'} AND LOW cars.price ";
                //"SKYLINE OF colors.name DISFAVOUR 'rot' ";
                //"SKYLINE OF cars.location AROUND (47.0484, 8.32629) ";
                Debug.WriteLine(strPrefSQL);


                Debug.WriteLine("--------------------------------------------");


                SQLCommon parser = new SQLCommon();
                //parser.SkylineType = SQLCommon.Algorithm.NativeSQL;
                //parser.SkylineType = SQLCommon.Algorithm.BNL;
                //parser.SkylineType = SQLCommon.Algorithm.BNLLevel;
                //parser.SkylineType = SQLCommon.Algorithm.BNLSort;
                //parser.SkylineType = SQLCommon.Algorithm.BNLSortLevel;
                parser.SkylineType = SQLCommon.Algorithm.Hexagon;
                //parser.OrderType = SQLCommon.Ordering.RankingBestOf;
                //parser.SkylineType = SQLCommon.Algorithm.Tree;
                //parser.ShowSkylineAttributes = true;

                string strSQL = parser.parsePreferenceSQL(strPrefSQL);

                Debug.WriteLine(strSQL);


                executeDb(strSQL, parser.SkylineType);
                



                Debug.WriteLine("------------------------------------------\nDONE");


            }
            catch (Exception ex)
            {
                Debug.WriteLine("ERROR: " + ex);
            }

            Environment.Exit(0);
        }




        public void executeDb(String strSQL, SQLCommon.Algorithm algorithm)
        {
            string str1 = "";
            string str2 = "";
            string str3 = "";

            if (algorithm != SQLCommon.Algorithm.NativeSQL)
            {



                //First parameter
                int iPosStart = strSQL.IndexOf("'") + 1;
                int iPosMiddle = iPosStart;
                bool bEnd = false;
                while (bEnd == false)
                {
                    iPosMiddle = iPosMiddle + strSQL.Substring(iPosMiddle).IndexOf("'") + 1;
                    if (!strSQL.Substring(iPosMiddle, 1).Equals("'"))
                    {
                        bEnd = true;
                    }
                    else
                    {
                        iPosMiddle++;
                    }
                    //Prüfen ob es kein doppeltes Hochkomma ist
                }
                iPosMiddle += 3;

                //Second parameter
                int iPosEnd = iPosMiddle;
                bEnd = false;
                while (bEnd == false)
                {
                    iPosEnd = iPosEnd + strSQL.Substring(iPosEnd).IndexOf("'") + 1;
                    if (iPosEnd == strSQL.Length)
                        break; //Kein 3.Parameter
                    if (!strSQL.Substring(iPosEnd, 1).Equals("'"))
                    {
                        bEnd = true;
                    }
                    else
                    {
                        iPosEnd++;
                    }
                    //Prüfen ob es kein doppeltes Hochkomma ist
                }
                iPosEnd += 3;


                str1 = strSQL.Substring(iPosStart, iPosMiddle - iPosStart - 4);
                str2 = strSQL.Substring(iPosMiddle, iPosEnd - iPosMiddle - 4);
                str3 = "";
                if (iPosEnd < strSQL.Length - 10)
                {
                    //str3 = strSQL.Substring(iPosEnd).TrimEnd('\'');
                    str3 = strSQL.Substring(iPosEnd, strSQL.Length - iPosEnd - 10).TrimEnd('\'');
                }
                str1 = str1.Replace("''", "'").Trim('\'');
                str2 = str2.Replace("''", "'").Trim('\'');
                str3 = str3.Replace("''", "'").Trim('\'');

            }

            Stopwatch sw = new Stopwatch();

            sw.Start();

            try
            {
                System.Data.SqlTypes.SqlString strSQL1 = str1;
                System.Data.SqlTypes.SqlString strSQL2 = str2;
                System.Data.SqlTypes.SqlString strSQL3 = str3;
                if (algorithm == SQLCommon.Algorithm.BNL)
                {
                    prefSQL.SQLSkyline.SP_SkylineBNL.getSkyline(str1, str2, true);
                }
                else if (algorithm == SQLCommon.Algorithm.BNLLevel)
                {
                    prefSQL.SQLSkyline.SP_SkylineBNLLevel.getSkyline(str1, str2, true);
                }
                else if (algorithm == SQLCommon.Algorithm.BNLSort)
                {
                    prefSQL.SQLSkyline.SP_SkylineBNLSort.getSkyline(str1, str2, true);
                }
                else if (algorithm == SQLCommon.Algorithm.BNLSortLevel)
                {
                    prefSQL.SQLSkyline.SP_SkylineBNLSortLevel.getSkyline(str1, str2, true);
                }
                else if (algorithm == SQLCommon.Algorithm.Hexagon)
                {
                    //Hexagon algorithm neads a higher stack (much recursions). Therefore start it with a new thread

                    //Default stack size is 1MB (1024000) --> Double it to 2MB
                    Thread T = new Thread(() => prefSQL.SQLSkyline.SP_SkylineHexagon.getSkyline(str1, str2, str3, true), 2048000);
                    T.Start();

                    //Join method to block the current thread  until the object's thread terminates.
                    T.Join();

                    //prefSQL.SQLSkyline.SP_SkylineHexagon.getSkyline(str1, str2, str3, true);
                }
                else if(algorithm == SQLCommon.Algorithm.Tree)
                {
                    prefSQL.SQLSkyline.SP_SkylineTree.getSkyline(str1, str2, true);
                }
                else if (algorithm == SQLCommon.Algorithm.NativeSQL)
                {
                    //Native SQL
                    SqlConnection connection = null;
                    connection = new SqlConnection(cnnStringLocalhost);

                    connection.Open();

                    SqlDataAdapter dap = new SqlDataAdapter(strSQL, connection);
                    DataTable dt = new DataTable();
                    dap.Fill(dt);

                    System.Diagnostics.Debug.WriteLine(dt.Rows.Count);
                }
                //prefSQL.SQLSkyline.SP_SkylineBNLLevel.getSkylineBNLLevel(str1, str2, true);
                
            }
            catch (Exception e)
            {
                Debug.WriteLine(e.Message);
            }



            sw.Stop();

            Console.WriteLine("Elapsed={0}", sw.Elapsed);
        }
    }
}
