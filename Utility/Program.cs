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


namespace Utility
{
    class Program
    {
        private const string cnnStringLocalhost = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";

        static void Main(string[] args)
        {


            /*
            Performance p = new Performance();
            p.GeneratePerformanceQueries(prefSQL.SQLParser.SQLCommon.Algorithm.Hexagon, false, true);
            */

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
                string strPrefSQL = "SELECT t1.id, t1.title, t1.price, t1.mileage, t1.enginesize FROM cars_superlarge t1 " +
                    //string strPrefSQL = "SELECT cars.id, cars.Price, cars.mileage FROM cars " +
                    //string strPrefSQL = "SELECT cars.id, cars.title, cars.price, cars.mileage, cars.horsepower, cars.enginesize, cars.registration, cars.consumption, cars.doors, colors.name, fuels.name FROM cars " +
                    //string strPrefSQL = "SELECT cars.id, cars.title, colors.name AS colourname, fuels.name AS fuelname, cars.price FROM cars " +
                    //string strPrefSQL = "SELECT id FROM cars " +
                    "LEFT OUTER JOIN colors t2 ON t1.color_id = t2.ID " +
                    //"LEFT OUTER JOIN bodies ON cars.body_id = bodies.ID " +
                    //"LEFT OUTER JOIN fuels ON cars.fuel_id = fuels.ID " +
                //"WHERE t1.id NOT IN (54521, 25612, 46268, 668, 47392, 1012, 22350, 55205, 51017) " +
                    //"SKYLINE OF LOW cars.price AND colors.name FAVOUR 'rot'";
                    //"SKYLINE OF HIGH t2.name {'schwarz' >> OTHERS} AND LOW t1.price AND HIGH t1.horsepower";
                    //"SKYLINE OF HIGH colors.name {'rot' == 'blau' >> OTHERS >> 'grau'} AND HIGH cars.registration";
                //"SKYLINE OF t1.price LOW AND t1.title ('MERCEDES-BENZ SL 600' >> OTHERS EQUAL) ORDER BY t1.price, t1.mileage ";

                //"SKYLINE OF t1.horsepower HIGH, t1.price LOW 10000, t1.mileage LOW 10000, t2.name ('schwarz' >> 'rot' >> OTHERS EQUAL), t1.title ('MERCEDES-BENZ SL 600' >> OTHERS EQUAL)";
                "SKYLINE OF t1.price LOW 10000, t1.mileage LOW 10000, t2.name ('pink' >> 'rot' == 'schwarz' >> 'beige' == 'gelb' >> OTHERS EQUAL) " +
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
                "ORDER BY t1.price ";
                //"SKYLINE OF HIGH colors.name {'gelb' >> OTHERS >> 'grau'} AND HIGH fuels.name {'Benzin' >> OTHERS >> 'Diesel'} AND LOW cars.price ";
                //"SKYLINE OF colors.name DISFAVOUR 'rot' ";
                //"SKYLINE OF cars.location AROUND (47.0484, 8.32629) ";
                Debug.WriteLine(strPrefSQL);


                Debug.WriteLine("--------------------------------------------");


                SQLCommon parser = new SQLCommon();
                //parser.SkylineType = SQLCommon.Algorithm.NativeSQL;
                //parser.SkylineType = SQLCommon.Algorithm.BNL;
                //parser.SkylineType = SQLCommon.Algorithm.Hexagon;
                //parser.OrderType = SQLCommon.Ordering.RankingBestOf;
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




        private void executeDb(String strSQL, SQLCommon.Algorithm algorithm)
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


            string str1 = strSQL.Substring(iPosStart, iPosMiddle - iPosStart - 4);
            string str2 = strSQL.Substring(iPosMiddle, iPosEnd - iPosMiddle - 4);
            string str3 = "";
            if (iPosEnd < strSQL.Length-10)
            {
                //str3 = strSQL.Substring(iPosEnd).TrimEnd('\'');
                str3 = strSQL.Substring(iPosEnd, strSQL.Length-iPosEnd-10).TrimEnd('\'');
            }
            str1 = str1.Replace("''", "'").Trim('\'');
            str2 = str2.Replace("''", "'").Trim('\'');
            str3 = str3.Replace("''", "'").Trim('\'');



            Stopwatch sw = new Stopwatch();

            sw.Start();

            try
            {
                System.Data.SqlTypes.SqlString strSQL1 = str1;
                System.Data.SqlTypes.SqlString strSQL2 = str2;
                System.Data.SqlTypes.SqlString strSQL3 = str3;
                if (algorithm == SQLCommon.Algorithm.BNL)
                {
                    prefSQL.SQLSkyline.SP_SkylineBNL.getSkylineBNL(str1, str2, true);
                }
                else if (algorithm == SQLCommon.Algorithm.Hexagon)
                {
                    prefSQL.SQLSkyline.SP_SkylineHexagon.getSkylineHexagon(str1, str2, str3, true);
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
