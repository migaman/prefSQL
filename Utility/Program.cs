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
using System.Windows.Forms;
using prefSQL.SQLSkyline;

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
            
            
            
            //Application.Run(new FrmSQLParser());
            
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
                string strPrefSQL = "SELECT t1.id, t1.title, t1.price, t1.mileage FROM cars_small t1 " +
                    //string strPrefSQL = "SELECT cars.id, cars.Price, cars.mileage FROM cars " +
                    //string strPrefSQL = "SELECT cars.id, cars.title, cars.price, cars.mileage, cars.horsepower, cars.enginesize, cars.registration, cars.consumption, cars.doors, colors.name, fuels.name FROM cars " +
                    //string strPrefSQL = "SELECT cars.id, cars.title, colors.name AS colourname, fuels.name AS fuelname, cars.price FROM cars " +
                    //string strPrefSQL = "SELECT id FROM cars " +
                    //"LEFT OUTER JOIN colors ON t1.color_id = colors.ID " +
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
                    //"SKYLINE OF t1.price LOW, t1.mileage LOW ";
                    //"SKYLINE OF t1.price AROUND 10000, colors.name ('pink' >> 'rot' >> 'schwarz' >> OTHERS EQUAL) " +
                    //"SKYLINE OF t1.price LOW, t1.mileage LOW, t1.horsepower HIGH, t1.doors HIGH, t1.enginesize HIGH, t1.consumption LOW, t1.seats HIGH, t1.cylinders HIGH " +
                    //", colors.name ('pink' >> 'rot' >> 'schwarz' >> OTHERS EQUAL) ";

                    "SKYLINE OF t1.price LOW, t1.mileage LOW";

                    //"SKYLINE OF t1.price LOW 3000, t1.mileage LOW 20000, t1.horsepower HIGH 20, t1.enginesize HIGH 1000";
                    //", t1.consumption LOW 10, t1.registration HIGHDATE 525600" +
                    //", t1.doors HIGH, t1.seats HIGH 2, t1.cylinders HIGH, t1.gears HIGH ";
                    //"ORDER BY BEST_RANK() ";


                //strPrefSQL = "SELECT c.id, c.price, b.name FROM cars_small c LEFT OUTER JOIN bodies b ON c.body_id = b.ID SKYLINE OF c.price LOW, b.name ('Bus' >> 'Kleinwagen')";
                //strPrefSQL = "SELECT c.id, c.price FROM cars_small c LEFT OUTER JOIN colors cc ON c.color_id = cc.id RANKING OF c.price LOW 0.5, cc.name ('braun' >> 'grün') 0.5";
                //strPrefSQL = "SELECT c.id, c.price FROM cars_small c LEFT OUTER JOIN colors cc ON c.color_id = cc.id SKYLINE OF c.horsepower HIGH, cc.name ('rot' >> 'blau' >> 'gelb')";

                Debug.WriteLine(strPrefSQL);
                Debug.WriteLine("--------------------------------------------");

                SQLCommon parser = new SQLCommon();
                //parser.SkylineType = new SkylineSQL();
                //parser.SkylineType = new SkylineBNL();
                //parser.SkylineType = new SkylineBNLSort();
                parser.SkylineType = new SkylineHexagon();
                //parser.SkylineType = new MultipleSkylineBNL();
                //parser.SkylineType = new SkylineDQ();
                //parser.ShowSkylineAttributes = true;
                //parser.SkylineUpToLevel = 1;

                //string strSQL = parser.parsePreferenceSQL(strPrefSQL);
                //Debug.WriteLine(strSQL);

                DataTable dt = parser.parseAndExecutePrefSQL(cnnStringLocalhost, driver, strPrefSQL);
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
