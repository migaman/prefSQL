using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using prefSQL.SQLParser;

namespace Utility
{
    class Program
    {
        static void Main(string[] args)
        {

            /*
            Performance p = new Performance();
            p.GeneratePerformanceQueries(prefSQL.SQLParser.SQLCommon.Algorithm.NativeSQL, false);
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
            
            
            
            
            
            //COMPARABLE
            /*
            string str1 = "SELECT cars_large.id, cars_large.price, cars_large.mileage, cars_large.horsepower, cars_large.consumption, CASE WHEN colors.name = 'rot' THEN 0 WHEN colors.name = 'blau' THEN 0 WHEN colors.name = 'grau' THEN 200 ELSE 100 END, cars_large.price AS SkylineAttribute1, cars_large.mileage AS SkylineAttribute2 FROM cars_large LEFT OUTER JOIN colors ON cars_large.color_id = colors.ID ORDER BY price ASC, mileage ASC, horsepower DESC, consumption ASC, CASE WHEN colors.name = 'rot' THEN 0 WHEN colors.name = 'blau' THEN 0 WHEN colors.name = 'grau' THEN 200 ELSE 100 END ASC";
            string str2 = "LOW;LOW;HIGH;LOW;LOW";
            string str3 = "SELECT cars_large.price,cars_large.mileage,cars_large.horsepower,cars_large.consumption,colors.name FROM cars_large LEFT OUTER JOIN colors ON cars_large.color_id = colors.ID";
            string str4 = "cars_large";
            */
            
            /*
            string str1 = "SELECT cars_large.price AS Skyline1, cars_large.mileage AS Skyline2, cars_large.price,cars_large.mileage,cars_large.horsepower,cars_large.consumption FROM cars_large LEFT OUTER JOIN colors ON cars_large.color_id = colors.ID " +
                            "ORDER BY price ASC, mileage ASC";
            string str2 = "LOW;LOW";
            */
            /*string strPrefSQL = "SELECT t1.id FROM cars t1 LEFT OUTER JOIN colors t2 ON t1.color_id = t2.ID " +
                //"SKYLINE OF LOW t1.price AND LOW t1.mileage AND LOW t1.consumption AND HIGH t1.enginesize AND HIGHDATE t1.registration AND HIGH t1.horsepower";
                "SKYLINE OF LOW t1.price AND LOW t1.mileage";
            SQLCommon parser = new SQLCommon();
            parser.SkylineType = SQLCommon.Algorithm.BNL;
            string strSQL = parser.parsePreferenceSQL(strPrefSQL);
            string str1 = strSQL.Substring(strSQL.IndexOf("'"), strSQL.IndexOf(", 'LOW")-strSQL.IndexOf("'"));
            string str2 = strSQL.Substring(strSQL.IndexOf(", 'LOW")+3);
            str1 = str1.Replace("''", "'").Trim('\'');
            str2 = str2.Replace("''", "'").Trim('\'');


            Stopwatch sw = new Stopwatch();

            sw.Start();

            try
            {
                SkylineDQ.SP_SkylineDQ(str1, str2);
                //SkylineBNL.SP_SkylineBNL(str1, str2);
            }
            catch(Exception e)
            {
                Debug.WriteLine(e.Message);
            }
            
            
            
            sw.Stop();
            
            Console.WriteLine("Elapsed={0}", sw.Elapsed);
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
                //string strPrefSQL = "SELECT t1.id, t1.title, t1.price, t1.mileage FROM cars t1 " +
                string  strPrefSQL = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID SKYLINE OF cars.price LOW, colors.name ('pink' >> {'rot', 'schwarz'} >> 'beige' == 'gelb') ORDER BY cars.price ASC, colors.name('pink'>>'beige')";
                    //string strPrefSQL = "SELECT cars.id, cars.Price, cars.mileage FROM cars " +
                    //string strPrefSQL = "SELECT cars.id, cars.title, cars.price, cars.mileage, cars.horsepower, cars.enginesize, cars.registration, cars.consumption, cars.doors, colors.name, fuels.name FROM cars " +
                    //string strPrefSQL = "SELECT cars.id, cars.title, colors.name AS colourname, fuels.name AS fuelname, cars.price FROM cars " +
                    //string strPrefSQL = "SELECT id FROM cars " +
                    //"LEFT OUTER JOIN colors t2 ON t1.color_id = t2.ID " +
                    //"LEFT OUTER JOIN bodies ON cars.body_id = bodies.ID " +
                    //"LEFT OUTER JOIN fuels ON cars.fuel_id = fuels.ID " +
                //"WHERE t1.id NOT IN (54521, 25612, 46268, 668, 47392, 1012, 22350, 55205, 51017) " +
                    //"SKYLINE OF LOW cars.price AND colors.name FAVOUR 'rot'";
                    //"SKYLINE OF HIGH t2.name {'schwarz' >> OTHERS} AND LOW t1.price AND HIGH t1.horsepower";
                    //"SKYLINE OF HIGH colors.name {'rot' == 'blau' >> OTHERS >> 'grau'} AND HIGH cars.registration";
                //"SKYLINE OF t1.price LOW AND t1.title ('MERCEDES-BENZ SL 600' >> OTHERS EQUAL) ORDER BY t1.price, t1.mileage ";
                //"SKYLINE OF HIGH t2.name {'rot' >> OTHERS} AND LOW t1.price";
                //"SKYLINE OF t1.price LOW, t1.mileage LOW";
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

                //"SKYLINE OF HIGH colors.name {'gelb' >> OTHERS >> 'grau'} AND HIGH fuels.name {'Benzin' >> OTHERS >> 'Diesel'} AND LOW cars.price ";
                //"SKYLINE OF colors.name DISFAVOUR 'rot' ";
                //"SKYLINE OF cars.location AROUND (47.0484, 8.32629) ";
                Debug.WriteLine(strPrefSQL);


                Debug.WriteLine("--------------------------------------------");


                SQLCommon parser = new SQLCommon();
                //parser.SkylineType = SQLCommon.Algorithm.BNL;
                //parser.OrderType = SQLCommon.Ordering.RankingBestOf;
                //parser.ShowSkylineAttributes = true;

                string strSQL = parser.parsePreferenceSQL(strPrefSQL);

                Debug.WriteLine(strSQL);





                Debug.WriteLine("------------------------------------------\nDONE");


            }
            catch (Exception ex)
            {
                Debug.WriteLine("ERROR: " + ex);
            }

            Environment.Exit(0);
        }
    }
}
