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
            //prg.MeasurePerformance();
            prg.Run();

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
            p.UseNormalizedValues = false;
            //p.UseNormalizedValues = true;

            p.WindowSort = SQLCommon.Ordering.AttributePosition;
            //p.WindowSort = SQLCommon.Ordering.EntropyFunction;

            //p.WindowHandling = 0; //Do not move           start with last tuple in window.   
            //p.WindowHandling = 1; //Do not move           start with first tuple in window.  
            p.WindowHandling = 2; //Move To End             start with last tuple in window.   
            //p.WindowHandling = 3; //Move To Beginning     start with first tuple in window.  

            //p.UseCLR = true;
            p.UseCLR = false;
            p.Trials = 1;           //Amount of trials for each single sql preference statement

            p.MinDimensions = 7;   //Up from x dimensions
            p.MaxDimensions = 7;   //Up to x dimensions
            p.RandomDraws = 10;    //Amount of draws (x times randomly choose a some preferences)

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
            p.Set = Performance.PreferenceSet.Numeric;
            //p.Set = Performance.PreferenceSet.Categoric;
            //p.Set = Performance.PreferenceSet.MinCardinality;
            //p.Set = Performance.PreferenceSet.CategoricIncomparable;
            //p.Set = Performance.PreferenceSet.NumericIncomparable;

            p.Mode = Performance.PreferenceChooseMode.Combination;
            //p.Mode = Performance.PreferenceChooseMode.SameOrder;
            //p.Mode = Performance.PreferenceChooseMode.Shuffle;
            //p.Mode = Performance.PreferenceChooseMode.Correlation;
            //p.Mode = Performance.PreferenceChooseMode.AntiCorrelation;
            //p.Mode = Performance.PreferenceChooseMode.Independent;

            p.SkylineUpToLevel = 1;

            //p.Strategy = null; //all algorithms should be tested
            //p.Strategy = new SkylineSQL();
            //p.Strategy = new SkylineBNL();
            //p.Strategy = new SkylineBNLSort();
            //p.Strategy = new SkylineDQ();
            //p.Strategy = new SkylineHexagon();
            p.Strategy = new SkylineDecisionTree();

            //p.Sampling = true;
            //p.SamplingSubspacesCount = 10;
            //p.SamplingSubspaceDimension = 3;
            //p.SamplingSamplesCount = 10;

            p.GeneratePerformanceQueries();
        }


        public void Run()
        {


            try
            {

                //SKYLINE Queries

                //string strPrefSQL = "SELECT t.id                           FROM cars t SKYLINE OF t.price LOW, t.mileage LOW";
                //string strPrefSQL = "SELECT *                           FROM cars t WHERE t.doors = 5 SKYLINE OF t.price LOW, t.mileage LOW, t.horsepower HIGH, t.consumption LOW, t.registrationnumeric HIGH";
                //string strPrefSQL = "SELECT t.title AS Modellname, t.price AS Preis, t.consumption AS Verbrauch FROM cars t LEFT OUTER JOIN Makes m ON t.make_id = m.id LEFT OUTER JOIN Bodies b ON t.body_id = b.id WHERE m.name = 'VW' AND b.name = 'Bus' SKYLINE OF t.price LOW 1000 EQUAL, t.consumption LOW";
                //string strPrefSQL = "SELECT t.id, t.title, t.price, c.name, t.enginesize FROM cars t LEFT OUTER JOIN colors c ON t.color_id = c.id SKYLINE OF t.price LOW, c.name ('pink' >> 'black' >> OTHERS INCOMPARABLE), t.enginesize HIGH ORDER BY BEST_RANK()";
                //string strPrefSQL = "SELECT t.id, t.title, t.price, c.name, t.enginesize FROM cars t LEFT OUTER JOIN colors c ON t.color_id = c.id SKYLINE OF t.price LOW, c.name (OTHERS EQUAL >> 'pink' >> 'red'), t.enginesize HIGH ORDER BY SUM_RANK()";
                //string strPrefSQL = "SELECT t.id, t.title, t.price         FROM cars t SKYLINE OF t1.price LOW, t1.mileage LOW ORDER BY BEST_RANK()";
                //string strPrefSQL = "SELECT t.id, t.price, t.mileage       FROM cars t SKYLINE OF t1.price LOW, t1.mileage LOW ORDER BY SUM_RANK()";



                //string strPrefSQL = "SELECT t.id, t.title, bodies.name AS Chassis, t.price, fuels.name                        FROM cars t LEFT OUTER JOIN fuels ON t.fuel_id = fuels.ID LEFT OUTER JOIN bodies ON t.body_id = bodies.ID SKYLINE OF bodies.name ('bus' >> OTHERS EQUAL) IS MORE IMPORTANT THAN t.price LOW, fuels.name ('petrol' >> OTHERS EQUAL)";
                //string strPrefSQL = "SELECT t.id, t.title, t.price, t.mileage, t.enginesize                    FROM cars t SKYLINE OF t.mileage LOW IS MORE IMPORTANT THAN t.price LOW, t.engineszie HIGH ";




                //SKYLINE Queries with JOINS
                //string strPrefSQL = "SELECT c.id, c.price, b.name          FROM cars t LEFT OUTER JOIN bodies b ON c.body_id = b.ID SKYLINE OF c.price LOW, b.name ('Bus' >> 'Kleinwagen')";
                //string strPrefSQL = "SELECT c.id, c.price                  FROM cars t LEFT OUTER JOIN colors cc ON c.color_id = cc.id SKYLINE OF c.horsepower HIGH, cc.name ('red' >> 'blue' >> 'yellow')";
                //string strPrefSQL = "SELECT t1.id, t1.title, t1.price      FROM cars t LEFT OUTER JOIN colors ON t1.color_id = colors.ID SKYLINE OF t1.price LOW, colors.name ('red' >> 'blue' >> OTHERS INCOMPARABLE)";
                //string strPrefSQL = "SELECT t1.id, t1.title, t1.price      FROM cars t LEFT OUTER JOIN colors ON t1.color_id = colors.ID SKYLINE OF t1.price LOW, t1.mileage LOW, t1.horsepower HIGH, t1.enginesize HIGH, t1.doors HIGH, t1.consumption LOW, t1.cylinders HIGH, colors.name ('red' >> 'blue' >> 'yellow' >> OTHERS INCOMPARABLE)";
                //string strPrefSQL = "SELECT t1.id, t1.title, t1.price      FROM cars t LEFT OUTER JOIN colors ON t1.color_id = colors.ID SKYLINE OF t1.price LOW, t1.mileage LOW, t1.horsepower HIGH, t1.enginesize HIGH, t1.doors HIGH, t1.consumption LOW, t1.cylinders HIGH";
                //string strPrefSQL = "SELECT t1.id, t1.title, t1.pric       FROM cars t LEFT OUTER JOIN colors ON t1.color_id = colors.ID WHERE t1.price < 10000 SKYLINE OF t1.price LOW, colors.name ('red' >> 'blue' >> OTHERS INCOMPARABLE)";
                //string strPrefSQL = "SELECT t1.id, t1.title, t1.price      FROM cars t LEFT OUTER JOIN colors ON t1.color_id = colors.ID WHERE t1.price < 10000 SKYLINE OF t1.price LOW, colors.name (OTHERS INCOMPARABLE >> 'blue' >> 'red')";
                //string strPrefSQL = "SELECT c.id, c.price                  FROM cars t LEFT OUTER JOIN colors cc ON c.color_id = cc.id LEFT OUTER JOIN fuels f ON f.id = c.fuel_id SKYLINE OF c.price LOW 1000 INCOMPARABLE, cc.name ('red' == 'blue' >> OTHERS INCOMPARABLE >> 'gray'), f.name ('petrol' >> OTHERS INCOMPARABLE >> 'diesel')";
                //string strPrefSQL = "SELECT c.title AS Name, c.Price       FROM cars t LEFT OUTER JOIN colors co ON c.color_id = co.ID LEFT OUTER JOIN bodies b ON c.body_id = b.ID SKYLINE OF c.Price LOW, c.Mileage LOW";
                //string strPrefSQL = "SELECT t1.id, t1.title, t1.price      FROM cars t LEFT OUTER JOIN colors ON t1.color_id = colors.ID WHERE t1.price < 10000 SKYLINE OF t1.price LOW, colors.name ('silver' >> 'yellow' >> OTHERS INCOMPARABLE)";
                //string strPrefSQL = "SELECT t.id, t.title, t.price         FROM cars t LEFT OUTER JOIN colors ON t.color_id = colors.id SKYLINE OF t.price LOW, t.mileage LOW, colors.name ('red' >> {'blue', 'yellow'} >> OTHERS INCOMPARABLE)";

                //string strPrefSQL = "SELECT cars.id                        FROM cars t LEFT OUTER JOIN bodies ON cars.body_id = bodies.ID LEFT OUTER JOIN colors ON colors.id = cars.color_id SKYLINE OF cars.price AROUND 10000, colors.name ('red' >> OTHERS EQUAL), bodies.name ('van' >> 'compact car' >> OTHERS EQUAL) ORDER BY BEST_RANK()";



                //RANKING Queries
                //string strPrefSQL = "SELECT t.id, t.title FROM cars t RANKING OF t.price LOW 0.8, t.mileage LOW 0.2";
                //string strPrefSQL = "SELECT t.id, t.title FROM cars t LEFT OUTER JOIN colors c ON t.color_id = c.id RANKING OF t.price LOW 0.5, c.name ('brown' >> 'green' >> OTHERS EQUAL) 0.5";





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
                //string strPrefSQL = "SELECT * FROM cars_small cs SKYLINE OF cs.price LOW, cs.mileage LOW";
                //string strPrefSQL = "SELECT t1.id, t1.price, t1.mileage FROM cars_small t1 SKYLINE OF t1.price LOW, t1.mileage LOW ORDER BY SUM_RANK()";
                //string strPrefSQL = "SELECT t1.id                          FROM cars t1  SKYLINE OF t1.price LOW, t1.mileage LOW, t1.horsepower HIGH, t1.enginesize HIGH, t1.doors HIGH, t1.consumption LOW";

                //string strPrefSQL = "SELECT c.id, c.price                  FROM cars_medium c   LEFT OUTER JOIN colors cc ON c.color_id = cc.id LEFT OUTER JOIN fuels f ON f.id = c.fuel_id SKYLINE OF c.price LOW 1000 INCOMPARABLE, cc.name ('red' == 'blue' >> OTHERS INCOMPARABLE >> 'gray'), f.name ('petrol' >> OTHERS INCOMPARABLE >> 'diesel')";
                //string strPrefSQL = "SELECT c.title AS Name, c.Price, c.Mileage, co.Name AS Color, b.Name AS Body FROM Cars AS c LEFT OUTER JOIN colors co ON c.color_id = co.ID LEFT OUTER JOIN bodies b ON c.body_id = b.ID SKYLINE OF c.Price LOW, c.Mileage LOW";

                //string strPrefSQL = "EXEC dbo.SP_SkylineBNLSortLevel 'SELECT  CAST(c.Price AS bigint) AS SkylineAttribute0, CAST(c.Mileage AS bigint) AS SkylineAttribute1 , c.title AS Name, c.Price, c.Mileage, co.Name AS Color, b.Name AS Body FROM Cars AS c LEFT OUTER JOIN colors co ON c.color_id = co.ID LEFT OUTER JOIN bodies b ON c.body_id = b.ID ORDER BY c.Price, c.Mileage', 'LOW;LOW', 0, 0";

                //string strPrefSQL = "SELECT t.id FROM cars t SKYLINE OF t.price LOW, t.mileage LOW";
                //string strPrefSQL = "SELECT t1.id, t1.title, t1.price, colors.name FROM cars t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID WHERE t1.price < 10000 SKYLINE OF t1.price LOW, colors.name ('silver' >> 'yellow' >> OTHERS INCOMPARABLE)";
                //string strPrefSQL = "SELECT cars.id, cars.title FROM  cars SKYLINE OF cars.price LOW,cars.mileage LOW,cars.horsepower HIGH,cars.enginesize HIGH,cars.consumption LOW,cars.doors HIGH,cars.seats HIGH";

                //string strPrefSQL = "SELECT t.id, t.title, t.price, colors.name AS colour FROM cars t LEFT OUTER JOIN colors ON t.color_id = colors.id SKYLINE OF t.price LOW, t.mileage LOW, colors.name ('red' >> {'blue', 'yellow'} >> OTHERS INCOMPARABLE)";
                //string strPrefSQL = "SELECT t.id, t.title FROM cars t RANKING OF t.price LOW 0.8, t.mileage LOW 0.2";

                //string strPrefSQL = "SELECT t.id, t.title  FROM cars t    RANKING OF t.price LOW 0.8, t.mileage LOW 0.2";

                //Query that results in more than 4000 Characters
                string strPrefSQL = "SELECT  t1.id	, t1.title	, t1.price	, t1.mileage	, colors.name FROM cars_small t1 " +
                                    "LEFT OUTER JOIN colors ON t1.color_id = colors.ID " +
                                    "LEFT OUTER JOIN bodies ON t1.body_id = bodies.id " +
                                    "LEFT OUTER JOIN Conditions ON t1.Condition_Id = Conditions.Id " +
                                    "LEFT OUTER JOIN Models ON t1.Model_Id = Models.Id " +
                                    "LEFT OUTER JOIN Makes ON t1.Make_Id = Makes.Id " +
                                    "LEFT OUTER JOIN Drives ON t1.Drive_Id = Drives.Id " +
                                    "LEFT OUTER JOIN Efficiencies ON t1.Efficiency_Id = Efficiencies.Id " +
                                    "LEFT OUTER JOIN Pollutions ON t1.Pollution_Id = Pollutions.Id " +
                                    "LEFT OUTER JOIN Transmissions ON t1.Transmission_Id = Transmissions.Id " +
                                    "LEFT OUTER JOIN Fuels ON t1.Fuel_Id = Fuels.Id " +
                                    "WHERE t1.price < 10000  " +
                                    "SKYLINE OF  " +
                                    "t1.price LOW " +
                                    ", colors.name " +
                                    "(" +
                                    "'anthracite' >> 'beige' >> 'blue' >> 'bordeaux' >> " +
                                    "    'brown' >> 'yellow' >> 'gold' >> 'gray' >> 'green' >> " +
                                    "    'orange' >> 'pink' >> 'red' >> 'black' >> 'silver' >> " +
                                    "    'turquoise' >> 'violet' >> 'white'" +
                                    ") " +

                                    ", bodies.name " +
                                    "(" +
                                    "    'bus' >> 'cabriolet' >> 'coupé' >> 'van' >> " +
                                    "    'compact car' >> 'estate car' >> 'minivan' >> " +
                                    "    'limousine' >> 'pick-up' >> 'scooter' >> 'suv' " +
                                    ")" +
                                    ", fuels.name " +
                                    "(" +
                                    "    'hybrid' >> 'bioethanol' >> 'diesel' >> 'gas' >> " +
                                    "    'electro' >> 'petrol' " +
                                    ")";




                Debug.WriteLine(strPrefSQL);
                SQLCommon parser = new SQLCommon();


                //Choose here your algorithm
                //parser.SkylineType = new SkylineSQL();
                //parser.SkylineType = new SkylineBNL();
                parser.SkylineType = new SkylineBNLSort();
                //parser.SkylineType = new SkylineHexagon();
                //parser.SkylineType = new SkylineDQ();
                //parser.SkylineType = new MultipleSkylineBNL();
                //parser.SkylineType = new SkylineDecisionTree();


                //Some other available properties
                //parser.ShowSkylineAttributes = true;
                parser.SkylineUpToLevel = 1;



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








    }
}
