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
            /*Demo d = new Demo();
            d.generateDemoQueries();*/

            Performance p = new Performance();
            p.GeneratePerformanceQueries(prefSQL.SQLParser.SQLCommon.Algorithm.BNL);

            /*Program prg = new Program();
            prg.Run();*/


            //Test SkylineBNL Algorithm
            /*String str1 = "SELECT cars.id , CASE WHEN colors.name = 'schwarz' THEN 0 ELSE 100 END, colors.name, cars.price FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID ORDER BY CASE WHEN colors.name = 'schwarz' THEN 0 ELSE 100 END ASC, price ASC";
            String str2 = ";LOW_INCOMPARABLE;INCOMPARABLE;LOW";
            String str3 = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID";
            String str4 = "cars";*/
            //SkylineBNL.SP_SkylineBNL(str1, str2, str3, str4);
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

                //String strPrefSQL = "SELECT cars.id, cars.title, colors.name, fuels.name FROM cars " +
                //String strPrefSQL = "SELECT cars.id, cars.title, cars.price, colors.name, mileage FROM cars " +
                String strPrefSQL = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars " +
                    //String strPrefSQL = "SELECT cars.id, cars.Price, cars.mileage FROM cars " +
                    //String strPrefSQL = "SELECT cars.id, cars.title, cars.price, cars.mileage, cars.horsepower, cars.enginesize, cars.registration, cars.consumption, cars.doors, colors.name, fuels.name FROM cars " +
                    //String strPrefSQL = "SELECT cars.id, cars.title, colors.name AS colourname, fuels.name AS fuelname, cars.price FROM cars " +
                    //String strPrefSQL = "SELECT id FROM cars " +
                    "LEFT OUTER JOIN colors ON cars.color_id = colors.ID " +
                    //"LEFT OUTER JOIN bodies ON cars.body_id = bodies.ID " +
                    //"LEFT OUTER JOIN fuels ON cars.fuel_id = fuels.ID " +
                    //"WHERE cars.horsepower > 10 AND cars.price < 10000 " +
                //"PREFERENCE LOW cars.price AND colors.name FAVOUR 'rot'";
                "PREFERENCE HIGH colors.name {'schwarz' >> OTHERS} AND LOW cars.price";
                //"PREFERENCE HIGH cars.title {'MERCEDES-BENZ SL 600' >> OTHERS} AND LOW cars.price";
                //"PREFERENCE HIGH colors.name {'rot' >> OTHERS} AND LOW cars.price";
                //"PREFERENCE HIGH cars.price AND Low cars.mileage ";
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
                //parser.SkylineType = SQLCommon.Algorithm.NativeSQL;
                parser.SkylineType = SQLCommon.Algorithm.BNL;

                String strSQL = parser.parsePreferenceSQL(strPrefSQL);

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
