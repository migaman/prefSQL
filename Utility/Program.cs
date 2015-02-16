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
using System.Data.Common;

namespace Utility
{
    class Program
    {
        private const string cnnStringLocalhost = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
        private const string driver = "System.Data.SqlClient";

        static void Main(string[] args)
        {
            /*
            Performance p = new Performance();
            p.GeneratePerformanceQueries(SQLCommon.Algorithm.NativeSQL, true, Performance.PreferenceSet.Mya);
            */

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
                string strPrefSQL = "SELECT t1.id, t1.title, t1.price, t1.mileage, t1.enginesize FROM cars_small t1 " +
                    //string strPrefSQL = "SELECT cars.id, cars.Price, cars.mileage FROM cars " +
                    //string strPrefSQL = "SELECT cars.id, cars.title, cars.price, cars.mileage, cars.horsepower, cars.enginesize, cars.registration, cars.consumption, cars.doors, colors.name, fuels.name FROM cars " +
                    //string strPrefSQL = "SELECT cars.id, cars.title, colors.name AS colourname, fuels.name AS fuelname, cars.price FROM cars " +
                    //string strPrefSQL = "SELECT id FROM cars " +
                    "LEFT OUTER JOIN colors ON t1.color_id = colors.ID " +
                    /*"LEFT OUTER JOIN bodies ON t1.body_id = bodies.ID " +
                    "LEFT OUTER JOIN conditions ON t1.condition_id = conditions.id " +
                    "LEFT OUTER JOIN Transmissions ON t1.transmission_id = Transmissions.id " +
                    "LEFT OUTER JOIN Fuels ON t1.fuel_id = Fuels.id " +
                    "LEFT OUTER JOIN Drives ON t1.drive_id = Drives.id " +
                    "LEFT OUTER JOIN Pollutions ON t1.pollution_id = Pollutions.id " +
                    "LEFT OUTER JOIN Efficiencies ON t1.efficiency_id = Efficiencies.id " +
                    "LEFT OUTER JOIN Makes ON t1.make_id = Makes.id " +
                    "LEFT OUTER JOIN Models ON t1.model_id = Models.id " +*/
                    //"WHERE (t1.price < 16000) " +
                    //"WHERE (t1.price < 4000) " + 
                    "SKYLINE OF t1.price LOW, t1.mileage LOW ";
                    //"SKYLINE OF t1.price LOW, colors.name ({'blau', 'silber', 'rot', 'schwarz', 'gelb'} >> OTHERS INCOMPARABLE) " +
                    //"SKYLINE OF t1.price LOW 3000, t1.mileage LOW 20000, t1.horsepower HIGH 20, t1.enginesize HIGH 1000";
                    //", t1.consumption LOW 10, t1.registration HIGHDATE 525600" +
                    //", t1.doors HIGH, t1.seats HIGH 2, t1.cylinders HIGH, t1.gears HIGH ";
                    
                    
                    //"SKYLINE OF t1.price LOW AND t1.title ('MERCEDES-BENZ SL 600' >> OTHERS EQUAL) ORDER BY t1.price, t1.mileage ";

                //"SKYLINE OF t1.horsepower HIGH, t1.price LOW 10000, t1.mileage LOW 10000, t2.name ('schwarz' >> 'rot' >> OTHERS EQUAL), t1.title ('MERCEDES-BENZ SL 600' >> OTHERS EQUAL)";
                    //"SKYLINE OF t1.price LOW, t1.mileage LOW, t2.name ('pink' >> 'rot' == 'schwarz' >> 'beige' == 'gelb' >> OTHERS EQUAL), t1.consumption LOW 50, t1.enginesize HIGH 1000 " +
                    //"SKYLINE OF t1.price LOW 60000, t1.horsepower HIGH 80";
                    //"SKYLINE OF t1.price LOW, t1.horsepower HIGH, t1.registration HIGHDATE, t1.consumption LOW, t1.mileage LOW, t1.enginesize HIGH ";
                //"SKYLINE OF Fuels.name ('Benzin' >> OTHERS EQUAL), Makes.name ('FISKER' >> OTHERS EQUAL)   " +
                //", bodies.name ('Roller' >> OTHERS EQUAL), models.name ('123' >> OTHERS EQUAL) "; 
                

                //"ORDER BY t1.title ";


                strPrefSQL = "SELECT t1.id, t1.title, t1.price, colors.name FROM cars_small t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID WHERE t1.price < 9000 SKYLINE OF t1.price LOW, colors.name ('schwarz' >> OTHERS INCOMPARABLE >> 'grau')";
                //strPrefSQL = "SELECT t1.id, t1.title, t1.price, colors.name FROM cars_small t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID WHERE colors.name IN ('schwarz', 'blau', 'silber', 'rot', 'grau') SKYLINE OF t1.price LOW, colors.name ('schwarz' >> {'blau', 'silber', 'rot'} >> 'grau')";
                Debug.WriteLine(strPrefSQL);
                Debug.WriteLine("--------------------------------------------");

                SQLCommon parser = new SQLCommon();
                parser.SkylineType = SQLCommon.Algorithm.NativeSQL;
                //parser.SkylineType = SQLCommon.Algorithm.BNL;
                //parser.SkylineType = SQLCommon.Algorithm.BNLSort;
                parser.SkylineType = SQLCommon.Algorithm.Hexagon;
                //parser.SkylineType = SQLCommon.Algorithm.MultipleBNL;
                //parser.SkylineType = SQLCommon.Algorithm.DQ;
                //parser.ShowSkylineAttributes = true;
                parser.SkylineUpToLevel = 1;
                

                //string strSQL = parser.parsePreferenceSQL(strPrefSQL);
                //Debug.WriteLine(strSQL);

                DataTable dt = parser.parseAndExeutePrefSQL(cnnStringLocalhost, driver, strPrefSQL, parser.SkylineType, parser.SkylineUpToLevel);
                System.Diagnostics.Debug.WriteLine(dt.Rows.Count);



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
