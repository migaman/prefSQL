using System;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Text;
using prefSQL.SQLParser;
using prefSQL.SQLSkyline;

namespace Utility
{
    class Program
    {


        static void Main(string[] args)
        {






            Program prg = new Program();
            //prg.PerformanceTestBNL();
            prg.MeasurePerformance();
            //prg.Run();


            /*
            DominanceGraph graph = new DominanceGraph();
            graph.run();
            */

            //Application.Run(new FrmSQLParser());

        }



        private void MeasurePerformance()
        {
            Performance p = new Performance();


            p.GenerateScript = false;

            //p.UseCLR = true;
            p.UseCLR = false;
            p.Trials = 1;           //Amount of trials for each single sql preference statement

            p.MinDimensions = 17;   //Up from x dimensions
            p.MaxDimensions = 17;   //Up to x dimensions
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
            //p.Set = Performance.PreferenceSet.All;
            //p.Set = Performance.PreferenceSet.Numeric;
            //p.Set = Performance.PreferenceSet.Categoric;
            //p.Set = Performance.PreferenceSet.MinCardinality;
            //p.Set = Performance.PreferenceSet.LowCardinality;
            //p.Set = Performance.PreferenceSet.HighCardinality;
            p.Set = Performance.PreferenceSet.LowAndHighCardinality;
            //p.Set = Performance.PreferenceSet.ForRandom10;
            //p.Set = Performance.PreferenceSet.ForRandom17;

            p.Mode = Performance.PreferenceChooseMode.Combination;
            //p.Mode = Performance.PreferenceChooseMode.Shuffle;
            //p.Mode = Performance.PreferenceChooseMode.Correlation;
            //p.Mode = Performance.PreferenceChooseMode.AntiCorrelation;
            //p.Mode = Performance.PreferenceChooseMode.Independent;

            //p.Strategy = null; //all algorithms should be tested
            //p.Strategy = new SkylineSQL();
            //p.Strategy = new SkylineBNL();
            p.Strategy = new SkylineBNLSort();
            //p.Strategy = new SkylineDQ();
            //p.Strategy = new SkylineHexagon();
            //p.Strategy = new SkylineDecisionTree();

            p.ExcessiveTests = false;
            p.Sampling = true;
            p.SamplingSubsetsCount = 15;
            p.SamplingSubsetDimension = 3;
            p.SamplingSamplesCount = 20;

            p.GeneratePerformanceQueries();
        }


        public void Run()
        {


            try
            {
                //Playground --> Test here your queries
                //string strPrefSQL = "SELECT c.id, c.price, b.name          FROM cars_small c   LEFT OUTER JOIN bodies b ON c.body_id = b.ID SKYLINE OF c.price LOW, b.name ('Bus' >> 'Kleinwagen')";
                //string strPrefSQL = "SELECT c.id, c.price                  FROM cars_small c   LEFT OUTER JOIN colors cc ON c.color_id = cc.id RANKING OF c.price LOW 0.5, cc.name ('brown' >> 'green') 0.5";
                //string strPrefSQL = "SELECT c.id, c.price                  FROM cars_small c   LEFT OUTER JOIN colors cc ON c.color_id = cc.id SKYLINE OF c.horsepower HIGH, cc.name ('red' >> 'blue' >> 'yellow')";
                //string strPrefSQL = "SELECT t1.id, t1.title, t1.price      FROM cars_small t1  LEFT OUTER JOIN colors ON t1.color_id = colors.ID SKYLINE OF t1.price LOW, colors.name ('red' >> 'blue' >> OTHERS INCOMPARABLE)";
                //string strPrefSQL = "SELECT t1.id                          FROM cars t1        RANKING OF t1.price HIGH 0.5, t1.mileage HIGH 0.5, t1.horsepower LOW 0.5, t1.enginesize LOW 0.5, t1.consumption HIGH 0.5, t1.doors LOW 0.5, t1.cylinders LOW 0.5";
                //string strPrefSQL = "SELECT t1.id, t1.title, t1.price      FROM cars t1        LEFT OUTER JOIN colors ON t1.color_id = colors.ID SKYLINE OF t1.price LOW, t1.mileage LOW, t1.horsepower HIGH, t1.enginesize HIGH, t1.doors HIGH, t1.consumption LOW, t1.cylinders HIGH, colors.name ('red' >> 'blue' >> 'yellow' >> OTHERS INCOMPARABLE)";
                //string strPrefSQL = "SELECT t1.id, t1.title, t1.price      FROM cars t1        SKYLINE OF t1.price LOW, t1.mileage LOW ORDER BY BEST_RANK()";
                //string strPrefSQL = "SELECT t1.id                          FROM cars t1        SKYLINE OF t1.price LOW, t1.mileage LOW";
                //string strPrefSQL = "SELECT cars.id, cars.consumption      FROM cars           SKYLINE OF cars.consumption LOW, cars.enginesize HIGH, cars.price LOW";
                //string strPrefSQL = "SELECT *                              FROM cars           SKYLINE OF cars.registrationnumeric HIGH, cars.mileage LOW, cars.horsepower HIGH 100 EQUAL";
                //string strPrefSQL = "SELECT cars.id, cars.horsepower       FROM cars           SKYLINE OF cars.horsepower HIGH, cars.mileage LOW";
                //string strPrefSQL = "SELECT t1.id, t1.title, t1.price      FROM cars t1        LEFT OUTER JOIN colors ON t1.color_id = colors.ID SKYLINE OF t1.price LOW, t1.mileage LOW, t1.horsepower HIGH, t1.enginesize HIGH, t1.doors HIGH, t1.consumption LOW, t1.cylinders HIGH";
                //string strPrefSQL = "SELECT t1.id, t1.title, t1.price, t1.mileage, t1.enginesize           FROM cars t1        SKYLINE OF t1.price LOW, t1.mileage LOW, t1.enginesize HIGH ORDER BY SUM_RANK()";
                //string strPrefSQL = "SELECT t1.id                          FROM cars t1  SKYLINE OF t1.price LOW, t1.mileage LOW, t1.horsepower HIGH, t1.enginesize HIGH, t1.doors HIGH, t1.consumption LOW, t1.cylinders HIGH";
                //string strPrefSQL = "SELECT t1.id                          FROM cars_norm t1   SKYLINE OF t1.price LOW, t1.mileage LOW, t1.horsepower LOW, t1.enginesize LOW, t1.doors LOW, t1.consumption LOW, t1.cylinders LOW";
                //string strPrefSQL = "SELECT t1.id, t1.title, t1.price      FROM cars t1        SKYLINE OF t1.price LOW, t1.mileage LOW ORDER BY BEST_RANK()";
                //string strPrefSQL = "SELECT t1.id AS ID, t1.title, t1.price FROM cars_small t1 SKYLINE OF t1.price LOW, t1.title ('hans' >> OTHERS INCOMPARABLE)";
                //string strPrefSQL = "SELECT t1.id, t1.title, t1.price, t1.mileage, colors.name FROM cars_small t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID WHERE t1.price < 10000 SKYLINE OF t1.price LOW, colors.name ('red' >> 'blue' >> OTHERS INCOMPARABLE)";
                //string strPrefSQL = "SELECT t1.id, t1.title, t1.price, t1.mileage, colors.name FROM cars_small t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID WHERE t1.price < 10000 SKYLINE OF t1.price LOW, colors.name (OTHERS INCOMPARABLE >> 'blue' >> 'red')";
                //string strPrefSQL = "SELECT * FROM cars_small cs SKYLINE OF cs.price LOW, cs.mileage LOW SAMPLE BY RANDOM_SUBSETS COUNT 2 DIMENSION 1";
                //string strPrefSQL = "SELECT t1.id                          FROM cars_small t1  SKYLINE OF t1.price LOW";
                //string strPrefSQL = "SELECT * FROM cars_small cs SKYLINE OF cs.price LOW, cs.mileage LOW";
                string strPrefSQL = "SELECT t1.id, t1.price, t1.mileage FROM cars_small t1 SKYLINE OF t1.price LOW, t1.mileage LOW";

                Debug.WriteLine(strPrefSQL);
                SQLCommon parser = new SQLCommon();


                //Choose here your algorithm
                //parser.SkylineType = new SkylineSQL();
                //parser.SkylineType = new SkylineBNL();
                //parser.SkylineType = new SkylineBNLSort();
                //parser.SkylineType = new SkylineHexagon();
                //parser.SkylineType = new SkylineDQ();
                parser.SkylineType = new MultipleSkylineBNL();


                //Some other available properties
                //parser.ShowSkylineAttributes = true;
                //parser.SkylineUpToLevel = 1;


                //First parse only (to get the parsed string for CLR)
                Debug.WriteLine(parser.ParsePreferenceSQL(strPrefSQL));

                //Now parse and execute
                Stopwatch sw = new Stopwatch();
                sw.Start();
                DataTable dt = parser.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, strPrefSQL);
                sw.Stop();


                StringBuilder sb = new StringBuilder();
                sb.AppendLine("------------------------------------------");
                sb.AppendLine("STATISTIC");
                sb.AppendLine("------------------------------------------");
                sb.AppendLine("         skyline size:" + dt.Rows.Count.ToString().PadLeft(6));
                sb.AppendLine("algo  time elapsed ms:" + parser.TimeInMilliseconds.ToString().PadLeft(6));
                sb.AppendLine("total time elapsed ms:" + sw.ElapsedMilliseconds.ToString().PadLeft(6));
                Debug.Write(sb);

            }
            catch (Exception ex)
            {
                Debug.WriteLine("ERROR: " + ex);
            }

            Environment.Exit(0);
        }






        private void PerformanceTestBNL()
        {

            try
            {
                string strPrefSQL = "SELECT t1.id                          FROM cars t1  SKYLINE OF t1.price LOW, t1.mileage LOW, t1.horsepower HIGH, t1.enginesize HIGH, t1.doors HIGH, t1.consumption LOW, t1.cylinders HIGH";
                Debug.WriteLine(strPrefSQL);
                SQLCommon parser = new SQLCommon();
                parser.SkylineType = new SkylineBNLSort();

                //Now parse and execute
                Stopwatch sw = new Stopwatch();



                /*
                 * Variante bisher 
                */
                sw.Start();
                parser.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, strPrefSQL);
                sw.Stop();
                long elapsedTime1 = sw.ElapsedMilliseconds;
                long elpasedTimeAlgo1 = parser.TimeInMilliseconds;


                //Variante BNL Test (Logik von CLOFI, MoveToHead)
                BNLTest test1 = new BNLTest();
                string str1 = "SELECT  CAST(t1.price as decimal(10,9)) AS SkylineAttribute0, CAST(t1.mileage as decimal(10,9)) AS SkylineAttribute1, CAST(t1.horsepower as decimal(10,9)) AS SkylineAttribute2, CAST(t1.enginesize as decimal(10,9)) AS SkylineAttribute3, CAST(t1.doors as decimal(10,9)) AS SkylineAttribute4, CAST(t1.consumption as decimal(10,9)) AS SkylineAttribute5, CAST(t1.cylinders as decimal(10,9)) AS SkylineAttribute6 , t1.id                          FROM cars_norm t1   ORDER BY t1.price, t1.mileage, t1.horsepower, t1.enginesize, t1.doors, t1.consumption, t1.cylinders";
                string str2 = "LOW;LOW;LOW;LOW;LOW;LOW;LOW";
                string str3 = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
                string str4 = "System.Data.SqlClient";
                sw.Start();
                test1.GetSkylineTable(str1, str2, 0, true, str3, str4);
                sw.Stop();
                long elapsedTime2 = sw.ElapsedMilliseconds;
                long elpasedTimeAlgo2 = test1.TimeInMs;

                // Variante BNL Test (Meine Logik aber mit float statt object, einige andere optimierungen)
                BNLTest2 test2 = new BNLTest2();
                str1 = "SELECT  CAST(t1.price as decimal(10,2)) AS SkylineAttribute0, CAST(t1.mileage as decimal(10,2)) AS SkylineAttribute1, CAST(t1.horsepower as decimal(10,2))*-1 AS SkylineAttribute2, CAST(t1.enginesize as decimal(10,2))*-1 AS SkylineAttribute3, CAST(t1.doors as decimal(10,2))*-1 AS SkylineAttribute4, CAST(t1.consumption as decimal(10,2)) AS SkylineAttribute5, CAST(t1.cylinders as decimal(10,2))*-1 AS SkylineAttribute6 , t1.id                          FROM cars t1   ORDER BY t1.price, t1.mileage, t1.horsepower*-1, t1.enginesize*-1, t1.doors*-1, t1.consumption, t1.cylinders*-1";
                str2 = "LOW;LOW;LOW;LOW;LOW;LOW;LOW";
                str3 = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
                str4 = "System.Data.SqlClient";
                sw.Start();
                test2.GetSkylineTable(str1, str2, 0, true, str3, str4);
                sw.Stop();
                long elapsedTime3 = sw.ElapsedMilliseconds;
                long elpasedTimeAlgo3 = test2.TimeInMs;

                // Variante BNL Test (Logik von CLOFI, MoveToHead, aber mit meiner vergleichsklasse)
                BNLTest3 test3 = new BNLTest3();
                str1 = "SELECT  CAST(t1.price as decimal(10,9)) AS SkylineAttribute0, CAST(t1.mileage as decimal(10,9)) AS SkylineAttribute1, CAST(t1.horsepower as decimal(10,9)) AS SkylineAttribute2, CAST(t1.enginesize as decimal(10,9)) AS SkylineAttribute3, CAST(t1.doors as decimal(10,9)) AS SkylineAttribute4, CAST(t1.consumption as decimal(10,9)) AS SkylineAttribute5, CAST(t1.cylinders as decimal(10,9)) AS SkylineAttribute6 , t1.id                          FROM cars_norm t1   ORDER BY t1.price, t1.mileage, t1.horsepower, t1.enginesize, t1.doors, t1.consumption, t1.cylinders";
                str2 = "LOW;LOW;LOW;LOW;LOW;LOW;LOW";
                str3 = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
                str4 = "System.Data.SqlClient";
                sw.Start();
                test3.GetSkylineTable(str1, str2, 0, true, str3, str4);
                sw.Stop();
                long elapsedTime4 = sw.ElapsedMilliseconds;
                long elpasedTimeAlgo4 = test3.TimeInMs;




                StringBuilder sb = new StringBuilder();
                sb.AppendLine("------------------------------------------");
                sb.AppendLine("STATISTIC");
                sb.AppendLine("------------------------------------------");
                sb.AppendLine("Current algo  time elapsed ms:" + elpasedTimeAlgo1);
                sb.AppendLine("Current total time elapsed ms:" + elapsedTime1);
                sb.AppendLine("ClofiMovetoFront algo  time elapsed ms:" + elpasedTimeAlgo2);
                sb.AppendLine("ClofiMovetoFront total time elapsed ms:" + elapsedTime2);
                sb.AppendLine("MeFloat algo  time elapsed ms:" + elpasedTimeAlgo3);
                sb.AppendLine("MeFloat total time elapsed ms:" + elapsedTime3);
                sb.AppendLine("ClofiMe algo  time elapsed ms:" + elpasedTimeAlgo4);
                sb.AppendLine("ClofiMe total time elapsed ms:" + elapsedTime4);

                Debug.Write(sb);


                //create filename
                string strFileName = "E:\\Doc\\Studies\\PRJ_Thesis\\70 Release\\Performance_2.txt";
                StreamWriter outfile = new StreamWriter(strFileName, true);
                outfile.Write(sb.ToString());
                outfile.Close();
            }
            catch (Exception ex)
            {
                Debug.WriteLine("ERROR: " + ex);
            }

        }

    }
}
