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

            
            Performance p = new Performance();
            p.GeneratePerformanceQueries(prefSQL.SQLParser.SQLCommon.Algorithm.NativeSQL, false);
            

            /*
            DominanceGraph graph = new DominanceGraph();
            graph.run();
            */

            /*
            Program prg = new Program();
            prg.Run();
            */

            /*
            FrmSQLParser form = new FrmSQLParser();
            form.Show();
            */
            //Test SkylineBNL Algorithm
            /*string str1 = "SELECT cars.id , CASE WHEN colors.name = 'schwarz' THEN 0 ELSE 100 END, colors.name, cars.price FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID ORDER BY CASE WHEN colors.name = 'schwarz' THEN 0 ELSE 100 END ASC, price ASC";
            string str2 = ";LOW_INCOMPARABLE;INCOMPARABLE;LOW";
            string str3 = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID";
            string str4 = "cars";*/
            //SkylineBNL.SP_SkylineBNL(str1, str2, str3, str4);

            /*
            Stopwatch sw = new Stopwatch();

            sw.Start();
            */
            /*
            string str1 = "SELECT cars_large.id , cars_large.price, cars_large.mileage, cars_large.horsepower, cars_large.enginesize, cars_large.registration, cars_large.consumption, cars_large.doors, CASE WHEN colors.name = 'rot' THEN 0 WHEN colors.name = 'blau' THEN 0 WHEN colors.name = 'grau' THEN 200 ELSE 100 END, CASE WHEN colors.name = 'rot' THEN '' WHEN colors.name = 'blau' THEN '' WHEN colors.name = 'grau' THEN ''ELSE colors.name END, CASE WHEN fuels.name = 'Benzin' THEN 0 WHEN fuels.name = 'Diesel' THEN 200 ELSE 100 END, CASE WHEN fuels.name = 'Benzin' THEN '' WHEN fuels.name = 'Diesel' THEN ''ELSE fuels.name END, CASE WHEN bodies.name = 'Kleinwagen' THEN 0 WHEN bodies.name = 'Bus' THEN 100 WHEN bodies.name = 'Kombi' THEN 200 WHEN bodies.name = 'Roller' THEN 300 WHEN bodies.name = 'Pick-Up' THEN 500 ELSE 400 END, CASE WHEN bodies.name = 'Kleinwagen' THEN '' WHEN bodies.name = 'Bus' THEN '' WHEN bodies.name = 'Kombi' THEN '' WHEN bodies.name = 'Roller' THEN '' WHEN bodies.name = 'Pick-Up' THEN ''ELSE bodies.name END, CASE WHEN cars_large.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 100 END, CASE WHEN cars_large.title = 'MERCEDES-BENZ SL 600' THEN ''ELSE cars_large.title END, CASE WHEN makes.name = 'ASTON MARTIN' THEN 0 WHEN makes.name = 'VW' THEN 100 WHEN makes.name = 'Audi' THEN 100 WHEN makes.name = 'FERRARI' THEN 300 ELSE 200 END, CASE WHEN makes.name = 'ASTON MARTIN' THEN '' WHEN makes.name = 'VW' THEN '' WHEN makes.name = 'Audi' THEN '' WHEN makes.name = 'FERRARI' THEN ''ELSE makes.name END, CASE WHEN conditions.name = 'Neu' THEN 0 ELSE 100 END, CASE WHEN conditions.name = 'Neu' THEN ''ELSE conditions.name END FROM cars_large LEFT OUTER JOIN colors ON cars_large.color_id = colors.ID LEFT OUTER JOIN fuels ON cars_large.fuel_id = fuels.ID LEFT OUTER JOIN bodies ON cars_large.body_id = bodies.ID LEFT OUTER JOIN makes ON cars_large.make_id = makes.ID LEFT OUTER JOIN conditions ON cars_large.condition_id = conditions.ID ORDER BY price ASC, mileage ASC, horsepower DESC, enginesize DESC, registration DESC, consumption ASC, doors DESC, CASE WHEN colors.name = 'rot' THEN 0 WHEN colors.name = 'blau' THEN 0 WHEN colors.name = 'grau' THEN 200 ELSE 100 END ASC, CASE WHEN fuels.name = 'Benzin' THEN 0 WHEN fuels.name = 'Diesel' THEN 200 ELSE 100 END ASC, CASE WHEN bodies.name = 'Kleinwagen' THEN 0 WHEN bodies.name = 'Bus' THEN 100 WHEN bodies.name = 'Kombi' THEN 200 WHEN bodies.name = 'Roller' THEN 300 WHEN bodies.name = 'Pick-Up' THEN 500 ELSE 400 END ASC, CASE WHEN cars_large.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 100 END ASC, CASE WHEN makes.name = 'ASTON MARTIN' THEN 0 WHEN makes.name = 'VW' THEN 100 WHEN makes.name = 'Audi' THEN 100 WHEN makes.name = 'FERRARI' THEN 300 ELSE 200 END ASC, CASE WHEN conditions.name = 'Neu' THEN 0 ELSE 100 END ASC";
            string str2 = ";LOW;LOW;HIGH;HIGH;HIGH;LOW;HIGH;LOW;INCOMPARABLE;LOW;INCOMPARABLE;LOW;INCOMPARABLE;LOW;INCOMPARABLE;LOW;INCOMPARABLE;LOW;INCOMPARABLE";
            string str3 = "SELECT cars_large.price,cars_large.mileage,cars_large.horsepower,cars_large.enginesize,cars_large.registration,cars_large.consumption,cars_large.doors,colors.name,fuels.name,bodies.name,cars_large.title,makes.name,conditions.name FROM cars_large LEFT OUTER JOIN colors ON cars_large.color_id = colors.ID LEFT OUTER JOIN fuels ON cars_large.fuel_id = fuels.ID LEFT OUTER JOIN bodies ON cars_large.body_id = bodies.ID LEFT OUTER JOIN makes ON cars_large.make_id = makes.ID LEFT OUTER JOIN conditions ON cars_large.condition_id = conditions.ID ";
            string str4 = "cars_large";
            */
            
            /*
            string str1 = "SELECT cars_large.id , cars_large.price, cars_large.mileage, cars_large.horsepower, cars_large.enginesize, cars_large.registration, cars_large.consumption, cars_large.doors, CASE WHEN colors.name = 'rot' THEN 0 WHEN colors.name = 'blau' THEN 0 WHEN colors.name = 'grau' THEN 200 ELSE 100 END, CASE WHEN colors.name = 'rot' THEN '' WHEN colors.name = 'blau' THEN '' WHEN colors.name = 'grau' THEN ''ELSE colors.name END, CASE WHEN fuels.name = 'Benzin' THEN 0 WHEN fuels.name = 'Diesel' THEN 200 ELSE 100 END, CASE WHEN fuels.name = 'Benzin' THEN '' WHEN fuels.name = 'Diesel' THEN ''ELSE fuels.name END, CASE WHEN bodies.name = 'Kleinwagen' THEN 0 WHEN bodies.name = 'Bus' THEN 100 WHEN bodies.name = 'Kombi' THEN 200 WHEN bodies.name = 'Roller' THEN 300 WHEN bodies.name = 'Pick-Up' THEN 500 ELSE 400 END, CASE WHEN bodies.name = 'Kleinwagen' THEN '' WHEN bodies.name = 'Bus' THEN '' WHEN bodies.name = 'Kombi' THEN '' WHEN bodies.name = 'Roller' THEN '' WHEN bodies.name = 'Pick-Up' THEN ''ELSE bodies.name END, CASE WHEN cars_large.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 100 END, CASE WHEN cars_large.title = 'MERCEDES-BENZ SL 600' THEN ''ELSE cars_large.title END, CASE WHEN makes.name = 'ASTON MARTIN' THEN 0 WHEN makes.name = 'VW' THEN 100 WHEN makes.name = 'Audi' THEN 100 WHEN makes.name = 'FERRARI' THEN 300 ELSE 200 END, CASE WHEN makes.name = 'ASTON MARTIN' THEN '' WHEN makes.name = 'VW' THEN '' WHEN makes.name = 'Audi' THEN '' WHEN makes.name = 'FERRARI' THEN ''ELSE makes.name END, CASE WHEN conditions.name = 'Neu' THEN 0 ELSE 100 END, CASE WHEN conditions.name = 'Neu' THEN ''ELSE conditions.name END FROM cars_large LEFT OUTER JOIN colors ON cars_large.color_id = colors.ID LEFT OUTER JOIN fuels ON cars_large.fuel_id = fuels.ID LEFT OUTER JOIN bodies ON cars_large.body_id = bodies.ID LEFT OUTER JOIN makes ON cars_large.make_id = makes.ID LEFT OUTER JOIN conditions ON cars_large.condition_id = conditions.ID ORDER BY price ASC, mileage ASC, horsepower DESC, enginesize DESC, registration DESC, consumption ASC, doors DESC, CASE WHEN colors.name = 'rot' THEN 0 WHEN colors.name = 'blau' THEN 0 WHEN colors.name = 'grau' THEN 200 ELSE 100 END ASC, CASE WHEN fuels.name = 'Benzin' THEN 0 WHEN fuels.name = 'Diesel' THEN 200 ELSE 100 END ASC, CASE WHEN bodies.name = 'Kleinwagen' THEN 0 WHEN bodies.name = 'Bus' THEN 100 WHEN bodies.name = 'Kombi' THEN 200 WHEN bodies.name = 'Roller' THEN 300 WHEN bodies.name = 'Pick-Up' THEN 500 ELSE 400 END ASC, CASE WHEN cars_large.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 100 END ASC, CASE WHEN makes.name = 'ASTON MARTIN' THEN 0 WHEN makes.name = 'VW' THEN 100 WHEN makes.name = 'Audi' THEN 100 WHEN makes.name = 'FERRARI' THEN 300 ELSE 200 END ASC, CASE WHEN conditions.name = 'Neu' THEN 0 ELSE 100 END ASC";
            string str2 = ";LOW;LOW;HIGH;HIGH;HIGH;LOW;HIGH;LOW;INCOMPARABLE;LOW;INCOMPARABLE;LOW;INCOMPARABLE;LOW;INCOMPARABLE;LOW;INCOMPARABLE;LOW;INCOMPARABLE";
            string str3 = "SELECT cars_large.price,cars_large.mileage,cars_large.horsepower,cars_large.enginesize,cars_large.registration,cars_large.consumption,cars_large.doors,colors.name,fuels.name,bodies.name,cars_large.title,makes.name,conditions.name FROM cars_large LEFT OUTER JOIN colors ON cars_large.color_id = colors.ID LEFT OUTER JOIN fuels ON cars_large.fuel_id = fuels.ID LEFT OUTER JOIN bodies ON cars_large.body_id = bodies.ID LEFT OUTER JOIN makes ON cars_large.make_id = makes.ID LEFT OUTER JOIN conditions ON cars_large.condition_id = conditions.ID ";
            string str4 = "cars_large";
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
            /*
            string str1 = "SELECT  CASE WHEN t1.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 100 END AS SkylineAttribute0, CASE WHEN t1.title = 'MERCEDES-BENZ SL 600' THEN ''ELSE t1.title END, t1.price AS SkylineAttribute1 , t1.id, t1.title, t1.price, t1.mileage FROM cars t1 ORDER BY CASE WHEN t1.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 100 END ASC, price ASC";
            string str2 = "LOW;INCOMPARABLE;LOW";
            
            try
            {
                SkylineBNL.SP_SkylineBNL(str1, str2);
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
                string strPrefSQL = "SELECT t1.id, t1.title, t1.price, t1.mileage, name FROM cars t1 " +
                    //string strPrefSQL = "SELECT cars.id, cars.Price, cars.mileage FROM cars " +
                    //string strPrefSQL = "SELECT cars.id, cars.title, cars.price, cars.mileage, cars.horsepower, cars.enginesize, cars.registration, cars.consumption, cars.doors, colors.name, fuels.name FROM cars " +
                    //string strPrefSQL = "SELECT cars.id, cars.title, colors.name AS colourname, fuels.name AS fuelname, cars.price FROM cars " +
                    //string strPrefSQL = "SELECT id FROM cars " +
                    "LEFT OUTER JOIN colors t2 ON t1.color_id = t2.ID " +
                    //"LEFT OUTER JOIN bodies ON cars.body_id = bodies.ID " +
                    //"LEFT OUTER JOIN fuels ON cars.fuel_id = fuels.ID " +
                //"WHERE t1.id NOT IN (54521, 25612, 46268, 668, 47392, 1012, 22350, 55205, 51017) " +
                //"PREFERENCE LOW cars.price AND colors.name FAVOUR 'rot'";
                //"PREFERENCE HIGH t2.name {'schwarz' >> OTHERS} AND LOW t1.price AND HIGH t1.horsepower";
                //"PREFERENCE HIGH colors.name {'rot' == 'blau' >> OTHERS >> 'grau'} AND HIGH cars.registration";
                //"PREFERENCE HIGH t1.title {'MERCEDES-BENZ SL 600' >> OTHERS} AND LOW t1.price";
                //"PREFERENCE HIGH t2.name {'rot' >> OTHERS} AND LOW t1.price";
                "PREFERENCE LOW t1.price AND LOW t1.mileage ";
                //"PREFERENCE LOW t1.price PRIORITIZE LOW t1.mileage";
                //"PREFERENCE LOW t1.price PRIORITIZE LOW t1.mileage PRIORITIZE HIGH t2.name {OTHERS >> 'pink'}";
                //"PREFERENCE cars.price AROUND 10000 ";
                //"PREFERENCE HIGH colors.name {'rot' >> OTHERSEQUAL} AND cars.price AROUND 10000";
                //"PREFERENCE cars.price AROUND 10000 AND HIGH colors.name {'rot' >> OTHERSEQUAL}";
                //"PREFERENCE LOW cars.price AND HIGH colors.name {'rot' >> OTHERSEQUAL} ";
                //"PREFERENCE LOW cars.price AND LOW cars.mileage AND HIGH cars.horsepower AND HIGH cars.enginesize AND HIGH cars.registration AND LOW cars.consumption AND HIGH cars.doors AND HIGH colors.name {'rot' == 'blau' >> OTHERS >> 'grau'} AND HIGH fuels.name {'Benzin' >> OTHERS >> 'Diesel'}";
                //"PREFERENCE LOW cars.price AND LOW cars.mileage AND HIGH fuels.name {'Benzin' >> OTHERS >> 'Diesel'}";
                //"PREFERENCE LOW cars.price AND HIGH colors.name {'rot' >> OTHERS}";
                //"PREFERENCE LOW cars.price AND HIGH colors.name {'pink' >> 'rot' == 'schwarz'}";
                //"PREFERENCE LOW cars.price AND HIGH colors.name {'pink' >> {'rot', 'schwarz'} >> 'beige' >> OTHERS}";

                //"PREFERENCE HIGH colors.name {'gelb' >> OTHERS >> 'grau'} AND HIGH fuels.name {'Benzin' >> OTHERS >> 'Diesel'} AND LOW cars.price ";
                //"PREFERENCE colors.name DISFAVOUR 'rot' ";
                //"PREFERENCE cars.location AROUND (47.0484, 8.32629) ";
                Debug.WriteLine(strPrefSQL);


                Debug.WriteLine("--------------------------------------------");


                SQLCommon parser = new SQLCommon();
                parser.SkylineType = SQLCommon.Algorithm.BNL;
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
