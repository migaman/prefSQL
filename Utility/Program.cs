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
        

        static void Main(string[] args)
        {
            Program prg = new Program();
            //prg.measurePerformance();
            prg.Run();


            /*
            DominanceGraph graph = new DominanceGraph();
            graph.run();
            */
            
            //Application.Run(new FrmSQLParser());
            
        }

        private void measurePerformance()
        {
            Performance p = new Performance();


            p.GenerateScript = false;

            //p.UseCLR = true;
            p.UseCLR = false;
            p.Trials = 1;           //Amount of trials for each single sql preference statement
            p.Dimensions = 7;       //Up to x dimensions
            p.RandomDraws = 50;    //Amount of draws (x times randomly choose a some preferences)
            
            //p.TableSize = Performance.Size.Small;
            //p.TableSize = Performance.Size.Medium;
            //p.TableSize = Performance.Size.Large;
            p.TableSize = Performance.Size.Superlarge;

            //p.Set = Performance.PreferenceSet.ArchiveComparable;
            //p.Set = Performance.PreferenceSet.ArchiveIncomparable;
            //p.Set = Performance.PreferenceSet.Jon;
            //p.Set = Performance.PreferenceSet.Mya;
            //p.Set = Performance.PreferenceSet.Barra;
            //p.Set = Performance.PreferenceSet.Shuffle;
            //p.Set = Performance.PreferenceSet.Combination;
            p.Set = Performance.PreferenceSet.CombinationNumeric;
            //p.Set = Performance.PreferenceSet.CombinationCategoric;
            //p.Set = Performance.PreferenceSet.Correlation;
            //p.Set = Performance.PreferenceSet.AntiCorrelation;
            //p.Set = Performance.PreferenceSet.Independent;
            //p.Set = Performance.PreferenceSet.CombinationMinCardinality;

            //p.Strategy = null; //all algorithms should be tested
            //p.Strategy = new SkylineSQL();
            //p.Strategy = new SkylineBNL();
            //p.Strategy = new SkylineBNLSort();
            p.Strategy = new SkylineDQ();
            //p.Strategy = new SkylineHexagon();
            //p.Strategy = new SkylineDecisionTree();
            

            p.generatePerformanceQueries();
        }


        public void Run()
        {
            try
            {
                //Playground --> Test here your queries
                string strPrefSQL = "";
                strPrefSQL = "SELECT c.id, c.price, b.name          FROM cars_small c   LEFT OUTER JOIN bodies b ON c.body_id = b.ID SKYLINE OF c.price LOW, b.name ('Bus' >> 'Kleinwagen')";
                strPrefSQL = "SELECT c.id, c.price                  FROM cars_small c   LEFT OUTER JOIN colors cc ON c.color_id = cc.id RANKING OF c.price LOW 0.5, cc.name ('brown' >> 'green') 0.5";
                strPrefSQL = "SELECT c.id, c.price                  FROM cars_small c   LEFT OUTER JOIN colors cc ON c.color_id = cc.id SKYLINE OF c.horsepower HIGH, cc.name ('red' >> 'blue' >> 'yellow')";
                strPrefSQL = "SELECT t1.id, t1.title, t1.price      FROM cars_small t1  LEFT OUTER JOIN colors ON t1.color_id = colors.ID SKYLINE OF t1.price LOW, colors.name ('red' >> 'blue' >> OTHERS INCOMPARABLE)";
                strPrefSQL = "SELECT t1.id                          FROM cars t1        RANKING OF t1.price HIGH 0.5, t1.mileage HIGH 0.5, t1.horsepower LOW 0.5, t1.enginesize LOW 0.5, t1.consumption HIGH 0.5, t1.doors LOW 0.5, t1.cylinders LOW 0.5";
                strPrefSQL = "SELECT t1.id, t1.title, t1.price      FROM cars t1        LEFT OUTER JOIN colors ON t1.color_id = colors.ID SKYLINE OF t1.price LOW, t1.mileage LOW, t1.horsepower HIGH, t1.enginesize HIGH, t1.doors HIGH, t1.consumption LOW, t1.cylinders HIGH, colors.name ('red' >> 'blue' >> 'yellow' >> OTHERS INCOMPARABLE)";
                strPrefSQL = "SELECT t1.id, t1.title, t1.price      FROM cars t1        SKYLINE OF t1.price LOW, t1.mileage LOW ORDER BY BEST_RANK()";
                strPrefSQL = "SELECT t1.id                          FROM cars t1        SKYLINE OF t1.price LOW, t1.mileage LOW";
                strPrefSQL = "SELECT cars.id, cars.consumption      FROM cars           SKYLINE OF cars.consumption LOW, cars.enginesize HIGH, cars.price LOW";
                strPrefSQL = "SELECT *                              FROM cars           SKYLINE OF cars.registrationnumeric HIGH, cars.mileage LOW, cars.horsepower HIGH 100 EQUAL";
                strPrefSQL = "SELECT cars.id, cars.horsepower       FROM cars           SKYLINE OF cars.horsepower HIGH, cars.mileage LOW";
                strPrefSQL = "SELECT t1.id, t1.title, t1.price      FROM cars t1        LEFT OUTER JOIN colors ON t1.color_id = colors.ID SKYLINE OF t1.price LOW, t1.mileage LOW, t1.horsepower HIGH, t1.enginesize HIGH, t1.doors HIGH, t1.consumption LOW, t1.cylinders HIGH";
                strPrefSQL = "SELECT t1.id, t1.title, t1.price, t1.mileage, t1.enginesize           FROM cars t1        SKYLINE OF t1.price LOW, t1.mileage LOW, t1.enginesize HIGH ORDER BY SUM_RANK()";
                strPrefSQL = "SELECT t1.id                          FROM cars t1        LEFT OUTER JOIN colors ON t1.color_id = colors.ID SKYLINE OF t1.price LOW, t1.mileage LOW, t1.horsepower HIGH, t1.enginesize HIGH, t1.doors HIGH, t1.consumption LOW, t1.cylinders HIGH";

                //strPrefSQL = "SELECT t1.id, t1.title, t1.price      FROM cars t1        SKYLINE OF t1.price LOW, t1.mileage LOW ORDER BY SUM_RANK()";
                
                Debug.WriteLine(strPrefSQL);
                SQLCommon parser = new SQLCommon();


                //Choose here your algorithm
                //parser.SkylineType = new SkylineSQL();
                //parser.SkylineType = new SkylineBNL();
                parser.SkylineType = new SkylineBNLSort();
                //parser.SkylineType = new SkylineHexagon();
                //parser.SkylineType = new MultipleSkylineBNL();
                //parser.SkylineType = new SkylineDQ();
                
                //Some other available properties
                //parser.ShowSkylineAttributes = true;
                //parser.SkylineUpToLevel = 1;

                
                //First parse only (to get the parsed string for CLR)
                string strSQL = parser.parsePreferenceSQL(strPrefSQL);
                Debug.WriteLine(strSQL);

                //Now parse and execute
                Stopwatch sw = new Stopwatch();
                sw.Start();
                DataTable dt = parser.parseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, strPrefSQL);
                sw.Stop();

                
                Debug.WriteLine("\n------------------------------------------\nSTATISTIC\n------------------------------------------");
                System.Diagnostics.Debug.WriteLine("         skyline size:" + dt.Rows.Count.ToString().PadLeft(6));
                System.Diagnostics.Debug.WriteLine("algo  time elapsed ms:" + parser.TimeInMilliseconds.ToString().PadLeft(6));
                System.Diagnostics.Debug.WriteLine("total time elapsed ms:" + sw.ElapsedMilliseconds.ToString().PadLeft(6));
            }
            catch (Exception ex)
            {
                Debug.WriteLine("ERROR: " + ex);
            }

            Environment.Exit(0);
        }

    }
}
