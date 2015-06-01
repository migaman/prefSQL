using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using prefSQL.SQLParser;
using prefSQL.SQLParser.Models;
using prefSQL.SQLSkyline;
using Utility.Model;

namespace Utility
{
    using System.Collections.ObjectModel;
    using System.Data.Common;
    using System.Globalization;
    using System.Numerics;
    using prefSQL.Evaluation;
    using prefSQL.SQLParserTest;
    using prefSQL.SQLSkyline.SkylineSampling;

    /// <summary>
    /// Performance class implented on a similar idea like Lofi (2014)
    /// 
    /// </summary>
    /// <remarks>
    /// Lofi, Christoph (2014): skyline_simulator
    /// Bitbucket.      /// Available online at https://bitbucket.org/clofi/skyline_simulator.
    /// 
    /// </remarks>
    


    class Performance
    {

        private const string Path = @"C:\Users\Public\Documents\workspace\prefcom\prefSQL\root\PerformanceTests\";
        private int _trials = 5;                 //How many times each preferene query is executed  
        private int _randomDraws = 25;          //Only used for the shuffle set. How many random set will be generated
        static readonly Random Rnd = new Random();
        static readonly Mathematic Mathematic = new Mathematic();


        private enum ReportsSampling
        {
            TimeMin,TimeMax,TimeVar,TimeStdDev,SizeMin,SizeMax,SizeVar,SizeStdDev
        }

        private enum SkylineTypesSampling
        {
            RandomAvg, RandomMin, RandomMax, RandomVar, RandomStdDev,
            SampleAvg, SampleMin, SampleMax, SampleVar, SampleStdDev,
            BestRankAvg, BestRankMin, BestRankMax, BestRankVar, BestRankStdDev,
            SumRankAvg, SumRankMin, SumRankMax, SumRankVar, SumRankStdDev
        }

        private enum SkylineTypesSingleSampling
        {
            Random,
            Sample,
            BestRank,
            SumRank
        }

        private enum ClusterAnalysisSampling
        {
            EntireDb, EntireSkyline, SampleSkyline, BestRank, SumRank
        }

        public Performance()
        {
            UseCLR = false;
        }

        public int MinDimensions { get; set; }  //Up from this amount of dimension should be tested
        public int MaxDimensions { get; set; }  //Up to this amount of dimension should be tested

        public enum Size
        {
            Small,
            Medium,
            Large,
            Superlarge
        }

        public enum PreferenceSet
        {
            ArchiveComparable,      //Preferences from first performance tests, up to 13 dimnension
            ArchiveIncomparable,    //Preferences from first performance tests, up to 13 dimnension
            Jon,                    //Preference set from 2nd peformance phase
            Mya,                    //Preference set from 2nd peformance phase
            Barra,                  //Preference set from 2nd peformance phase
            
            All,                    //Take all preferences
            Numeric,                //Take only numeric preferences
            Categoric,              //Take only categoric preferences
            MinCardinality,         //Special collection of preferences which should perform well on Hexagon


        };

        public enum PreferenceChooseMode
        {
            Combination,            //Test every possible combination of the preferences
            Shuffle,                //Choose x randomly preferences from all possible combinations
            Correlation,            //Take 2 best correlated preferences
            AntiCorrelation,        //Take 2 worst correlated preferences
            Independent,            //Take 2 most independent correlated preferences
        }


        #region getter/setters

        public bool UseCLR { get; set; }

        internal Size TableSize { get; set; }
        

        public SkylineStrategy Strategy { get; set; }

        public bool GenerateScript { get; set; }

        internal PreferenceSet Set { get; set; }

        public PreferenceChooseMode Mode { get; set; }

        public int Trials
        {
            get { return _trials; }
            set { _trials = value; }
        }

        public int RandomDraws
        {
            get { return _randomDraws; }
            set { _randomDraws = value; }
        }

        public bool Sampling { get; set; }
        internal int SamplingSubspacesCount { get; set; }

        internal int SamplingSubspaceDimension { get; set; }

        public int SamplingSamplesCount { get; set; }

        #endregion


        #region preferences


        private ArrayList GetArchiveComparablePreferences()
        {
            ArrayList preferences = new ArrayList();

            preferences.Add("cars.price LOW");
            preferences.Add("cars.mileage LOW");
            preferences.Add("cars.horsepower HIGH");
            preferences.Add("cars.enginesize HIGH");
            preferences.Add("cars.registrationNumeric HIGH");
            preferences.Add("cars.consumption LOW");
            preferences.Add("cars.doors HIGH");
            preferences.Add("colors.name ('red' == 'blue' >> OTHERS EQUAL >> 'gray')");
            preferences.Add("fuels.name ('petrol' >> OTHERS EQUAL >> 'Diesel')");
            preferences.Add("bodies.name ('compact car' >> 'bus' >> 'estate car' >> 'scooter' >> OTHERS EQUAL >> 'pick-up')");
            preferences.Add("cars.title ('MERCEDES-BENZ SL 600' >> OTHERS EQUAL)");
            preferences.Add("makes.name ('ASTON MARTIN' >> 'VW' == 'Audi' >> OTHERS EQUAL >> 'FERRARI')");
            preferences.Add("conditions.name ('new' >> OTHERS EQUAL)");

            return preferences;
        }

        private ArrayList GetArchiveIncomparablePreferences()
        {
            ArrayList preferences = new ArrayList();

            preferences.Add("cars.price LOW");
            preferences.Add("cars.mileage LOW");
            preferences.Add("cars.horsepower HIGH");
            preferences.Add("cars.enginesize HIGH");
            preferences.Add("cars.registrationNumeric HIGH");
            preferences.Add("cars.consumption LOW");
            preferences.Add("cars.doors HIGH");
            preferences.Add("colors.name ('red' == 'blue' >> OTHERS INCOMPARABLE >> 'gray')");
            preferences.Add("fuels.name ('petrol' >> OTHERS INCOMPARABLE >> 'Diesel')");
            preferences.Add("bodies.name ('compact car' >> 'bus' >> 'estate car' >> 'scooter' >> OTHERS INCOMPARABLE >> 'pick-up')");
            preferences.Add("cars.title ('MERCEDES-BENZ SL 600' >> OTHERS INCOMPARABLE)");
            preferences.Add("makes.name ('ASTON MARTIN' >> 'VW' == 'Audi' >> OTHERS INCOMPARABLE >> 'FERRARI')");
            preferences.Add("conditions.name ('new' >> OTHERS INCOMPARABLE)");


            return preferences;
        }


        private ArrayList GetJonsPreferences()
        {
            ArrayList preferences = new ArrayList();

            preferences.Add("cars.price LOW");
            preferences.Add("cars.mileage LOW");
            preferences.Add("cars.horsepower HIGH");
            preferences.Add("cars.enginesize HIGH");
            preferences.Add("cars.consumption LOW");
            preferences.Add("cars.doors HIGH");
            preferences.Add("cars.seats HIGH");
            preferences.Add("cars.cylinders HIGH");
            preferences.Add("cars.gears HIGH");

            return preferences;
        }

        private ArrayList GetMyasPreferences()
        {
            ArrayList preferences = new ArrayList();
            preferences.Add("fuels.name ('petrol' >> OTHERS EQUAL)");
            preferences.Add("makes.name ('FISKER' >> OTHERS EQUAL)");
            preferences.Add("bodies.name ('scooter' >> OTHERS EQUAL)");
            preferences.Add("models.name ('123' >> OTHERS EQUAL)");
            return preferences;
        }

        private ArrayList GetBarrasPreferences()
        {
            ArrayList preferences = new ArrayList();
            preferences.Add("cars.price LOW 3000");
            preferences.Add("cars.mileage LOW 20000");
            preferences.Add("cars.horsepower HIGH 20");
            preferences.Add("cars.enginesize HIGH 1000");
            preferences.Add("cars.consumption LOW 10");
            preferences.Add("cars.doors HIGH");
            preferences.Add("cars.seats HIGH 2");
            preferences.Add("cars.cylinders HIGH");
            preferences.Add("cars.gears HIGH");
            return preferences;
        }

        internal static ArrayList GetNumericPreferences()
        {
            ArrayList preferences = new ArrayList();

            //Numeric preferences
            preferences.Add("cars.price LOW");
            preferences.Add("cars.mileage LOW");
            preferences.Add("cars.horsepower HIGH");
            preferences.Add("cars.enginesize HIGH");
            preferences.Add("cars.consumption LOW");
            preferences.Add("cars.doors HIGH");
            preferences.Add("cars.seats HIGH");
            preferences.Add("cars.cylinders HIGH");
            preferences.Add("cars.gears HIGH");
            preferences.Add("cars.registrationNumeric HIGH");

            return preferences;
        }

        internal static ArrayList GetCategoricalPreferences()
        {
            ArrayList preferences = new ArrayList();

            //Categorical preferences with a cardinality from 2 to 8 (descending)
            preferences.Add("colors.name ('red' >> 'blue' >> 'green' >> 'gold' >> 'black' >> 'gray' >> 'bordeaux' >> OTHERS EQUAL)");
            preferences.Add("bodies.name ('bus' >> 'cabriolet' >> 'limousine' >> 'coupé' >> 'van' >> 'estate car' >> OTHERS EQUAL)");
            preferences.Add("fuels.name ('petrol' >> 'diesel' >> 'bioethanol' >> 'electro' >> 'gas' >> 'hybrid' >> OTHERS EQUAL)");
            preferences.Add("makes.name ('BENTLEY' >> 'DAIMLER' >> 'FIAT'>> 'FORD'  >> OTHERS EQUAL)");
            preferences.Add("conditions.name ('new' >> 'occasion' >> 'demonstration car' >> 'oldtimer' >> OTHERS EQUAL)");
            preferences.Add("drives.name ('front wheel' >> 'all wheel' >> 'rear wheel' >> OTHERS EQUAL)");
            preferences.Add("transmissions.name ('manual' >> 'automatic' >> OTHERS EQUAL)");


            return preferences;
        }

        private ArrayList GetSpecialHexagonPreferences()
        {
            ArrayList preferences = new ArrayList();

            //Categorical preferences with a cardinality from 2 to 8 (descending)
            preferences.Add("cars.doors HIGH");
            preferences.Add("fuels.name ('petrol' >> 'diesel' >> 'bioethanol' >> 'elektro' >> 'gas' >> 'hybrid' >> OTHERS EQUAL)");
            preferences.Add("conditions.name ('new' >> 'occasion' >> 'demonstration car' >> 'oldtimer' >> OTHERS EQUAL)");
            preferences.Add("drives.name ('front wheel' >> 'all wheel' >> 'rear wheel' >> OTHERS EQUAL)");
            preferences.Add("transmissions.name ('manual' >> 'automatic' >> OTHERS EQUAL)");

            return preferences;
        }



        internal static ArrayList GetAllPreferences()
        {
            ArrayList preferences = new ArrayList();
            preferences.AddRange(GetNumericPreferences());
            preferences.AddRange(GetCategoricalPreferences());
            return preferences;
        }

        #endregion


        public void GeneratePerformanceQueries()
        {
            if (MaxDimensions < MinDimensions)
            {
                Debug.WriteLine("Max Dimensions must be >= Min Dimensions!");
                return;
            }

            //Open DBConnection --> Otherwise first query is slower as usual, because DBConnection is not open
            SQLCommon parser = new SQLCommon();
            DataTable dt = parser.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, "SELECT CAST(cars.id AS BIGINT) AS id FROM cars SKYLINE OF cars.price LOW");

            //Use the correct line, depending on how incomparable items should be compared
            ArrayList listPreferences = new ArrayList();
            SqlConnection cnnSQL = new SqlConnection(Helper.ConnectionString); //for CLR performance tets
            ArrayList preferencesMode = new ArrayList();
            if (UseCLR)
            {
                cnnSQL.Open();
            }

            switch (Set)
            {
                case PreferenceSet.ArchiveComparable:
                    preferencesMode = GetArchiveComparablePreferences();
                    break;
                case PreferenceSet.ArchiveIncomparable:
                    preferencesMode = GetArchiveIncomparablePreferences();
                    break;
                case PreferenceSet.Jon:
                    preferencesMode = GetJonsPreferences();
                    break;
                case PreferenceSet.Mya:
                    preferencesMode = GetMyasPreferences();
                    break;
                case PreferenceSet.Barra:
                    preferencesMode = GetBarrasPreferences();
                    break;
                case PreferenceSet.All:
                    preferencesMode = GetAllPreferences();
                    break;
                case PreferenceSet.Numeric:
                    preferencesMode = GetNumericPreferences();
                    break;
                case PreferenceSet.Categoric:
                    preferencesMode = GetCategoricalPreferences();
                    break;
                case PreferenceSet.MinCardinality:
                    preferencesMode = GetSpecialHexagonPreferences();
                    break;
            }


            //Calculate correlationmatrix and cardinality from the preferences
            ArrayList correlationMatrix = GetCorrelationMatrix(preferencesMode);
            ArrayList listCardinality = GetCardinalityOfPreferences(preferencesMode);
            
            //Depending on the mode create the sets from the preferences
            if (Mode == PreferenceChooseMode.Combination)
            {
                //Tests every possible combination with y preferences from the whole set of preferences

                if (MaxDimensions > preferencesMode.Count)
                {
                    Debug.WriteLine("Combination with more dimensions than preferences. Please reduce dimensions!");
                    return;
                }

                //create all possible combinations and add it to listPreferences
                for (int i = MinDimensions; i <= MaxDimensions; i++)
                {
                    GetCombinations(preferencesMode, i, 0, new ArrayList(), ref listPreferences);    
                }
                

            }
            else if (Mode == PreferenceChooseMode.Shuffle)
            {
                //Tests x times randomly y preferences
                for (int iChoose = 0; iChoose < _randomDraws; iChoose++)
                {
                    ArrayList preferencesRandom = new ArrayList();
                    ArrayList preferencesChoose = (ArrayList)preferencesMode.Clone();

                    //First define define randomly how many dimensions
                    int differentDimensions = MaxDimensions - MinDimensions + 1;
                    int sampleDimensions = Rnd.Next(differentDimensions) + MinDimensions;
                    
                    //Choose x preferences randomly
                    for (int i = 0; i < sampleDimensions; i++)
                    {
                        int r = Rnd.Next(preferencesChoose.Count);
                        preferencesRandom.Add(preferencesChoose[r]);
                        preferencesChoose.RemoveAt(r);
                    }

                    //add random preferences to listPreferences
                    listPreferences.Add(preferencesRandom);

                }

            }
            else if (Mode == PreferenceChooseMode.Correlation)
            {
                if (MaxDimensions > 2)
                {
                    Debug.WriteLine("This test mode only works for 2 dimensions!");
                    return;
                }

                //Sort correlations to find the strongest
                correlationMatrix.Sort(new CorrelationModel());

                //Sort correlations ascending
                CorrelationModel model = (CorrelationModel)correlationMatrix[0];
                preferencesMode.Clear();
                preferencesMode.Add(model.ColA);
                preferencesMode.Add(model.ColB);
                listPreferences.Add(preferencesMode);


            }
            else if (Mode == PreferenceChooseMode.AntiCorrelation)
            {
                if (MaxDimensions > 2)
                {
                    Debug.WriteLine("This test mode only works for 2 dimensions!");
                    return;
                }

                //Sort correlations ascending
                correlationMatrix.Sort(new CorrelationModel());

                //Take only the two preferences with the worst correlation
                CorrelationModel model = (CorrelationModel)correlationMatrix[correlationMatrix.Count - 1];
                preferencesMode.Clear();
                preferencesMode.Add(model.ColA);
                preferencesMode.Add(model.ColB);
                listPreferences.Add(preferencesMode);

            }
            else if (Mode == PreferenceChooseMode.Independent)
            {
                if (MaxDimensions > 2)
                {
                    Debug.WriteLine("This test mode only works for 2 dimensions!");
                    return;
                }

                //Sort correlations to find the strongest
                correlationMatrix.Sort(new CorrelationModel());

                //Find the most independent atributes (closest to zero)
                CorrelationModel modelBefore = new CorrelationModel();
                CorrelationModel modelAfter = new CorrelationModel();
                for (int i = 0; i <= correlationMatrix.Count; i++)
                {
                    CorrelationModel model = (CorrelationModel)correlationMatrix[i];
                    if (model.Correlation > 0)
                    {
                        //continue until the correlation turnaround
                        modelBefore = model;
                    }
                    else
                    {
                        modelAfter = model;
                        //Leave the function, because now the correlation is getting worse
                        break;
                    }
                }

                //Add the two preferences to the list, that are closer to zero
                preferencesMode.Clear();
                if (Math.Abs(modelBefore.Correlation) > Math.Abs(modelAfter.Correlation))
                {
                    preferencesMode.Add(modelAfter.ColA);
                    preferencesMode.Add(modelAfter.ColB);
                }
                else
                {
                    preferencesMode.Add(modelBefore.ColA);
                    preferencesMode.Add(modelBefore.ColB);
                }
                listPreferences.Add(preferencesMode);

            }

            

            List<SkylineStrategy> listStrategy = new List<SkylineStrategy>();
            if (Strategy == null)
            {
                //If no strategy is defined --> Take all possible algorithms
                //listStrategy.Add(new SkylineSQL());
                listStrategy.Add(new SkylineBNLSort());
                listStrategy.Add(new SkylineDQ());
                listStrategy.Add(new SkylineHexagon());
            }
            else
            {
                listStrategy.Add(Strategy);
            }
            foreach(SkylineStrategy currentStrategy in listStrategy)
            {
                //Take all strategies



                StringBuilder sb = new StringBuilder();
                string strSeparatorLine;
                if (Sampling)
                {
                    strSeparatorLine = FormatLineStringSample('-', "", "", "", "", "", "", "", "", "", "", "", "", "", "", new Dictionary<SkylineTypesSingleSampling, List<double>>(), new Dictionary<SkylineTypesSingleSampling, List<double>>(), new Dictionary<SkylineTypesSingleSampling, List<double>>(), new Dictionary<SkylineTypesSingleSampling, List<double>>(), new Dictionary<SkylineTypesSingleSampling, List<double>>(), new Dictionary<ClusterAnalysisSampling, string>(), new Dictionary<ClusterAnalysisSampling, string>(), new Dictionary<ClusterAnalysisSampling, string>(), new Dictionary<ClusterAnalysisSampling, string>(), "", "");
                }
                else
                {
                     strSeparatorLine = FormatLineString('-', "", "", "", "", "", "", "", "");
                }
                
                if (GenerateScript == false)
                {
                    //Header
                    sb.AppendLine("               Algorithm: " + currentStrategy);
                    sb.AppendLine("                 Use CLR: " + UseCLR);
                    sb.AppendLine("          Preference Set: " + Set.ToString());
                    sb.AppendLine("         Preference Mode: " + Mode.ToString());
                    sb.AppendLine("                    Host: " + Environment.MachineName);
                    sb.AppendLine("      Set of Preferences: " + listPreferences.Count);
                    sb.AppendLine("                  Trials: " + Trials);
                    sb.AppendLine("              Table size: " + TableSize.ToString());
                    sb.AppendLine("          Dimension from: " + MinDimensions.ToString());
                    sb.AppendLine("            Dimension to: " + MaxDimensions.ToString());
                    //sb.AppendLine("Correlation Coefficients:" + string.Join(",", (string[])preferences.ToArray(Type.GetType("System.String"))));
                    //sb.AppendLine("           Cardinalities:" + string.Join(",", (string[])preferences.ToArray(Type.GetType("System.String"))));
                    if (Sampling)
                    {
                        sb.AppendLine("                Sampling: true");
                        sb.AppendLine("         Subspaces Count: " + SamplingSubspacesCount);
                        sb.AppendLine("      Subspace Dimension: " + SamplingSubspaceDimension);
                        sb.AppendLine("           Sampling Runs: " + SamplingSamplesCount);
                    }
                    sb.AppendLine("");
                    if (Sampling)
                    {
                        sb.AppendLine(FormatLineStringSample(' ', "preference set", "trial", "dimensions", "avg skyline size", "avg time total", "avg time algorithm", "min time", "max time", "variance time", "stddeviation time", "min size", "max size", "variance size", "stddeviation size", new string[] { "avg sc random", "min sc random", "max sc random", "var sc random", "stddev sc random", "avg sc sample", "min sc sample", "max sc sample", "var sc sample", "stddev sc sample", "avg sc Best", "min sc Best", "max sc Best", "var sc Best", "stddev sc Best", "avg sc Sum", "min sc Sum", "max sc Sum", "var sc Sum", "stddev sc Sum" }, new string[] { "avg re random", "min re random", "max re random", "var re random", "stddev re random", "avg re sample", "min re sample", "max re sample", "var re sample", "stddev re sample", "avg re Best", "min re Best", "max re Best", "var re Best", "stddev re Best", "avg re Sum", "min re Sum", "max re Sum", "var re Sum", "stddev re Sum" }, new string[] { "avg reSum random", "min reSum random", "max reSum random", "var reSum random", "stddev reSum random", "avg reSum sample", "min reSum sample", "max reSum sample", "var reSum sample", "stddev reSum sample", "avg reSum Best", "min reSum Best", "max reSum Best", "var reSum Best", "stddev reSum Best", "avg reSum Sum", "min reSum Sum", "max reSum Sum", "var reSum Sum", "stddev reSum Sum" }, new string[] { "avg domCnt random", "min domCnt random", "max domCnt random", "var domCnt random", "stddev domCnt random", "avg domCnt sample", "min domCnt sample", "max domCnt sample", "var domCnt sample", "stddev domCnt sample", "avg domCnt Best", "min domCnt Best", "max domCnt Best", "var domCnt Best", "stddev domCnt Best", "avg domCnt Sum", "min domCnt Sum", "max domCnt Sum", "var domCnt Sum", "stddev domCnt Sum" }, new string[] { "avg domBst random", "min domBst random", "max domBst random", "var domBst random", "stddev domBst random", "avg domBst sample", "min domBst sample", "max domBst sample", "var domBst sample", "stddev domBst sample", "avg domBst Best", "min domBst Best", "max domBst Best", "var domBst Best", "stddev domBst Best", "avg domBst Sum", "min domBst Sum", "max domBst Sum", "var domBst Sum", "stddev domBst Sum" }, new string[] { "ca entire db", "ca entire skyline", "ca sample skyline", "ca best rank", "ca sum rank" }, new string[] { "caMed entire db", "caMed entire skyline", "caMed sample skyline", "caMed best rank", "caMed sum rank" }, new string[] { "caTopB entire db", "caTopB entire skyline", "caTopB sample skyline", "caTopB best rank", "caTopB sum rank" }, new string[] { "caMedTopB entire db", "caMedTopB entire skyline", "caMedTopB sample skyline", "caMedTopB best rank", "caMedTopB sum rank" }, "sum correlation*", "product cardinality"));
                    } else
                    {
                        sb.AppendLine(FormatLineString(' ', "preference set", "trial", "dimensions", "skyline size", "time total", "time algorithm", "sum correlation*", "product cardinality"));                        
                    }
                    sb.AppendLine(strSeparatorLine);
                    Debug.Write(sb);
                }



                List<long> reportDimensions = new List<long>();
                List<long> reportSkylineSize = new List<long>();
                List<long> reportTimeTotal = new List<long>();
                List<long> reportTimeAlgorithm = new List<long>();
                List<double> reportCorrelation = new List<double>();
                List<double> reportCardinality = new List<double>();

                Dictionary<ReportsSampling, List<long>> reportsSamplingLong;
                Dictionary<ReportsSampling, List<double>> reportsSamplingDouble;
                Dictionary<SkylineTypesSampling, List<double>> setCoverageSampling;
                Dictionary<SkylineTypesSampling, List<double>> representationErrorSampling;
                Dictionary<SkylineTypesSampling, List<double>> representationErrorSumSampling;
                Dictionary<SkylineTypesSampling, List<double>> dominatedObjectsCountSampling;
                Dictionary<SkylineTypesSampling, List<double>> dominatedObjectsOfBestObjectSampling;
                Dictionary<ClusterAnalysisSampling, List<List<double>>> clusterAnalysisSampling;
                Dictionary<ClusterAnalysisSampling, List<List<double>>> clusterAnalysisMedianSampling;
                Dictionary<ClusterAnalysisSampling, Dictionary<BigInteger, List<double>>> clusterAnalysisTopBucketsSampling;
                Dictionary<ClusterAnalysisSampling, Dictionary<BigInteger, List<double>>> clusterAnalysisMedianTopBucketsSampling;
                InitSamplingDataStructures(out reportsSamplingLong, out reportsSamplingDouble, out setCoverageSampling, out representationErrorSampling, out representationErrorSumSampling, out dominatedObjectsCountSampling, out dominatedObjectsOfBestObjectSampling, out clusterAnalysisSampling, out clusterAnalysisMedianSampling, out clusterAnalysisTopBucketsSampling, out clusterAnalysisMedianTopBucketsSampling);

                //For each preference set in the preference list
                for (int iPreferenceIndex = 0; iPreferenceIndex < listPreferences.Count; iPreferenceIndex++)
                {
                    ArrayList preferences = (ArrayList)listPreferences[iPreferenceIndex];
                    //Go only down two 3 dimension (because there are special algorithms for 1 and 2 dimensional skyline)
                    //for (int i = MinDimensions; i <= preferences.Count; i++)
                    //{
                    //ADD Preferences to SKYLINE
                    ArrayList subPreferences = preferences; //.GetRange(0, i);
                    string strSkylineOf = "SKYLINE OF " + string.Join(",", (string[])subPreferences.ToArray(Type.GetType("System.String")));

                    //SELECT FROM
                    string strSQL = "SELECT CAST CAST(cars.id AS BIGINT) AS id FROM ";
                    if (TableSize == Size.Small)
                    {
                        strSQL += "cars_small";
                    }
                    else if (TableSize == Size.Medium)
                    {
                        strSQL += "cars_medium";
                    }
                    else if (TableSize == Size.Large)
                    {
                        strSQL += "cars_large";
                    }
                    strSQL += " cars ";
                    //Add Joins
                    strSQL += GetJoinsForPreferences(strSkylineOf);



                    //Add Skyline-Clause
                    strSQL += strSkylineOf;


                    //Convert to real SQL
                    parser = new SQLCommon();
                    parser.SkylineType = currentStrategy;
                    parser.ShowSkylineAttributes = true;


                    if (GenerateScript == false)
                    {
                        for (int iTrial = 0; iTrial < Trials; iTrial++)
                        {
                            Stopwatch sw = new Stopwatch();

                            try
                            {
                                double correlation = SearchCorrelation(subPreferences, correlationMatrix);
                                double cardinality = SearchCardinality(subPreferences, listCardinality);

                                if (Sampling)
                                {
                                    string strTrial = iTrial + 1 + " / " + _trials;
                                    string strPreferenceSet = iPreferenceIndex + 1 + " / " + listPreferences.Count;
                                    Console.WriteLine(strPreferenceSet);

                                    List<IEnumerable<CLRSafeHashSet<int>>> producedSubspaces = ProduceSubspaces(preferences);

                                    InitClusterAnalysisSamplingDataStructures(out clusterAnalysisSampling);
                                    InitClusterAnalysisSamplingDataStructures(out clusterAnalysisMedianSampling);
                                    InitClusterAnalysisTopBucketsSamplingDataStructures(out clusterAnalysisTopBucketsSampling);
                                    InitClusterAnalysisTopBucketsSamplingDataStructures(out clusterAnalysisMedianTopBucketsSampling);

                                    DataTable entireSkylineDataTable =
                                        parser.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName,
                                            strSQL);

                                    List<long[]> entireDataTableSkylineValues =
                                        parser.SkylineType.Strategy.SkylineValues;

                                    int[] skylineAttributeColumns =
                                        SkylineSamplingHelper.GetSkylineAttributeColumns(entireSkylineDataTable);

                                    IReadOnlyDictionary<long, object[]> entireSkylineDatabase =
                                        prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(
                                            entireSkylineDataTable, 0);
                                    IReadOnlyDictionary<long, object[]> entireSkylineNormalized =
                                        prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(
                                            entireSkylineDataTable, 0);
                                    SkylineSamplingHelper.NormalizeColumns(entireSkylineNormalized,
                                        skylineAttributeColumns);

                                    DataTable entireDataTable;
                                        IReadOnlyDictionary<long, object[]> entireDatabaseNormalized =
                                        GetEntireDatabaseNormalized(parser, strSQL, skylineAttributeColumns, out entireDataTable);
                                        IReadOnlyDictionary<long, object[]> entireDatabase =
                                            prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(
                                                entireDataTable, 0);

                                      IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                                        entireDatabaseBuckets =
                                            ClusterAnalysis.GetBuckets(entireDatabaseNormalized,
                                                skylineAttributeColumns);

                                    IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                        aggregatedEntireDatabaseBuckets =
                                            ClusterAnalysis.GetAggregatedBuckets(entireDatabaseBuckets);

                                      foreach (KeyValuePair<BigInteger, List<IReadOnlyDictionary<long, object[]>>> s in entireDatabaseBuckets.OrderByDescending(l => l.Value.Count)
                         .ThenBy(l => l.Key).Take(5))
                                    {                                       
                                        double percent = (double) s.Value.Count / entireDatabaseNormalized.Count;
                                        clusterAnalysisTopBucketsSampling[ClusterAnalysisSampling.EntireDb].Add(s.Key,
                                            new List<double>());

                                        for (var i = 0; i < producedSubspaces.Count; i++) // to enable generalized average calculation
                                        {
                                            clusterAnalysisTopBucketsSampling[ClusterAnalysisSampling.EntireDb][s.Key]
                                                .Add(percent);
                                        }
                                    }

                                       IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                                        entireSkylineBuckets =
                                            ClusterAnalysis.GetBuckets(entireSkylineNormalized,
                                                skylineAttributeColumns);

                                    IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                        aggregatedEntireSkylineBuckets =
                                            ClusterAnalysis.GetAggregatedBuckets(entireSkylineBuckets);

                                     FillTopBucketsSampling(clusterAnalysisTopBucketsSampling, ClusterAnalysisSampling.EntireSkyline, entireSkylineBuckets, entireSkylineNormalized.Count, entireDatabaseNormalized.Count, entireSkylineNormalized.Count);
                                     foreach (
                                          KeyValuePair<BigInteger, List<double>> bucket in clusterAnalysisTopBucketsSampling[ClusterAnalysisSampling.EntireSkyline])
                                     {
                                         double percent =
                                             clusterAnalysisTopBucketsSampling[ClusterAnalysisSampling.EntireSkyline][bucket.Key][0];

                                         for (var i = 1; i < producedSubspaces.Count; i++) // to enable generalized average calculation
                                         {
                                             clusterAnalysisTopBucketsSampling[ClusterAnalysisSampling.EntireSkyline][bucket.Key].Add(percent);
                                         }
                                     }

                                    var clusterAnalysisForMedian = new ClusterAnalysis(entireDatabaseNormalized, skylineAttributeColumns);
                                 
                                    IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                                        entireDatabaseMedianBuckets =
                                            clusterAnalysisForMedian.GetBuckets(entireDatabaseNormalized,
                                                skylineAttributeColumns,true);

                                    IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                        aggregatedEntireDatabaseMedianBuckets =
                                            ClusterAnalysis.GetAggregatedBuckets(entireDatabaseMedianBuckets);

                                    foreach (
                                        KeyValuePair<BigInteger, List<IReadOnlyDictionary<long, object[]>>> s in
                                            entireDatabaseMedianBuckets.OrderByDescending(l => l.Value.Count)
                                                .ThenBy(l => l.Key).Take(5))
                                    {
                                        double percent = (double) s.Value.Count / entireDatabaseNormalized.Count;
                                        clusterAnalysisMedianTopBucketsSampling[ClusterAnalysisSampling.EntireDb].Add(
                                            s.Key,
                                            new List<double>());

                                        for (var i = 0; i < producedSubspaces.Count; i++)
                                            // to enable generalized average calculation
                                        {
                                            clusterAnalysisMedianTopBucketsSampling[ClusterAnalysisSampling.EntireDb][
                                                s.Key]
                                                .Add(percent);
                                        }
                                    }

                                    IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                                        entireSkylineMedianBuckets =
                                            clusterAnalysisForMedian.GetBuckets(entireSkylineNormalized,
                                                skylineAttributeColumns,true);

                                    IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                      aggregatedEntireSkylineMedianBuckets =
                                         ClusterAnalysis.GetAggregatedBuckets(entireSkylineMedianBuckets);

                                    FillTopBucketsSampling(clusterAnalysisMedianTopBucketsSampling, ClusterAnalysisSampling.EntireSkyline, entireSkylineMedianBuckets, entireSkylineNormalized.Count, entireDatabaseNormalized.Count, entireSkylineNormalized.Count);
                                    
                                    foreach (
                                       KeyValuePair<BigInteger, List<double>> bucket in clusterAnalysisMedianTopBucketsSampling[ClusterAnalysisSampling.EntireSkyline])
                                    {
                                        double percent =
                                            clusterAnalysisMedianTopBucketsSampling[ClusterAnalysisSampling.EntireSkyline][bucket.Key][0];

                                        for (var i = 1; i < producedSubspaces.Count; i++) // to enable generalized average calculation
                                        {
                                            clusterAnalysisMedianTopBucketsSampling[ClusterAnalysisSampling.EntireSkyline][bucket.Key].Add(percent);
                                        }
                                    }
                                    strSQL += " SAMPLE BY RANDOM_SUBSETS COUNT " + SamplingSubspacesCount +
                                              " DIMENSION " + SamplingSubspaceDimension;

                                    string strQuery;
                                    string operators;
                                    int numberOfRecords;
                                    string[] parameter;

                                    PrefSQLModel prefSqlModel = parser.GetPrefSqlModelFromPreferenceSql(strSQL);
                                    string ansiSql = parser.GetAnsiSqlFromPrefSqlModel(prefSqlModel);
                                    prefSQL.SQLParser.Helper.DetermineParameters(ansiSql, out parameter,
                                        out strQuery, out operators,
                                        out numberOfRecords);

                                    var subspaceObjects = new List<long>();
                                    var subspaceTime = new List<long>();
                                    var subspaceTimeElapsed = new List<long>();
                                    var setCoverageSecondRandom = new List<double>();
                                    var setCoverageSample = new List<double>();
                                    var setCoverageBestRank = new List<double>();
                                    var setCoverageSumRank = new List<double>();

                                    var representationErrorSecondRandom = new List<double>();
                                    var representationErrorSample = new List<double>();
                                    var representationErrorBestRank = new List<double>();
                                    var representationErrorSumRank = new List<double>();

                                    var representationErrorSumSecondRandom = new List<double>();
                                    var representationErrorSumSample = new List<double>();
                                    var representationErrorSumBestRank = new List<double>();
                                    var representationErrorSumSumRank = new List<double>();

                                    var dominatedObjectsCountSamplingSecondRandom = new List<double>();
                                    var dominatedObjectsCountSamplingSample = new List<double>();
                                    var dominatedObjectsCountSamplingBestRank = new List<double>();
                                    var dominatedObjectsCountSamplingSumRank = new List<double>();

                                    var dominatedObjectsOfBestObjectSamplingSecondRandom = new List<double>();
                                    var dominatedObjectsOfBestObjectSamplingSample = new List<double>();
                                    var dominatedObjectsOfBestObjectSamplingBestRank = new List<double>();
                                    var dominatedObjectsOfBestObjectSamplingSumRank = new List<double>();

                                    var subspaceCount = 1;
                                    foreach (IEnumerable<CLRSafeHashSet<int>> subspace in producedSubspaces)
                                    {
                                        Console.WriteLine(strPreferenceSet+ " (" + subspaceCount+" / " + producedSubspaces.Count+")");

                                        sw.Restart(); 
                                        var subspacesProducer = new FixedSkylineSamplingSubspacesProducer(subspace);
                                        var utility = new SkylineSamplingUtility(subspacesProducer);
                                        var skylineSample = new SkylineSampling(utility)
                                        {
                                            SubspacesCount = prefSqlModel.SkylineSampleCount,
                                            SubspaceDimension = prefSqlModel.SkylineSampleDimension,
                                            SelectedStrategy = parser.SkylineType
                                        };

                                        DataTable sampleSkylineDataTable = skylineSample.GetSkylineTable(strQuery,
                                            operators);

                                        sw.Stop();

                                        subspaceObjects.Add(sampleSkylineDataTable.Rows.Count);
                                        subspaceTime.Add(skylineSample.TimeMilliseconds);
                                        subspaceTimeElapsed.Add(sw.ElapsedMilliseconds);

                                        IReadOnlyDictionary<long, object[]> sampleSkylineDatabase =
                                            prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(
                                                sampleSkylineDataTable, 0);
                                        IReadOnlyDictionary<long, object[]> sampleSkylineNormalized =
                                            prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(
                                                sampleSkylineDataTable, 0);
                                        SkylineSamplingHelper.NormalizeColumns(sampleSkylineNormalized,
                                            skylineAttributeColumns);

                                        IReadOnlyDictionary<long, object[]> baseRandomSampleNormalized =
                                            SkylineSamplingHelper.GetRandomSample(entireSkylineNormalized,
                                                sampleSkylineDataTable.Rows.Count);

                                        IReadOnlyDictionary<long, object[]> secondRandomSampleDatabase =
                                            SkylineSamplingHelper.GetRandomSample(entireSkylineDatabase,
                                                sampleSkylineDataTable.Rows.Count);
                                        var secondRandomSampleNormalizedToBeCreated = new Dictionary<long, object[]>();
                                        foreach (KeyValuePair<long, object[]> k in secondRandomSampleDatabase)
                                        {
                                            var newValue=new object[k.Value.Length];
                                            k.Value.CopyTo(newValue, 0);
                                            secondRandomSampleNormalizedToBeCreated.Add(k.Key, newValue);
                                        }
                                        IReadOnlyDictionary<long, object[]> secondRandomSampleNormalized =
                                            new ReadOnlyDictionary<long, object[]>(secondRandomSampleNormalizedToBeCreated);
                                        SkylineSamplingHelper.NormalizeColumns(secondRandomSampleNormalized, skylineAttributeColumns);

                                        IReadOnlyDictionary<long, object[]> entireSkylineDataTableBestRankDatabase;
                                        IReadOnlyDictionary<long, object[]> entireSkylineDataTableBestRankNormalized =
                                            GetEntireSkylineDataTableRankNormalized(entireSkylineDataTable.Copy(),
                                                entireDataTableSkylineValues, skylineAttributeColumns,
                                                sampleSkylineDataTable.Rows.Count, 1, out entireSkylineDataTableBestRankDatabase);

                                        IReadOnlyDictionary<long, object[]> entireSkylineDataTableSumRankDatabase;
                                        IReadOnlyDictionary<long, object[]> entireSkylineDataTableSumRankNormalized =
                                            GetEntireSkylineDataTableRankNormalized(entireSkylineDataTable.Copy(),
                                                entireDataTableSkylineValues, skylineAttributeColumns,
                                                sampleSkylineDataTable.Rows.Count, 2, out entireSkylineDataTableSumRankDatabase);

                                        double setCoverageCoveredBySecondRandomSample = SetCoverage.GetCoverage(
                                            baseRandomSampleNormalized,
                                            secondRandomSampleNormalized, skylineAttributeColumns) * 100.0;
                                        double setCoverageCoveredBySkylineSample = SetCoverage.GetCoverage(
                                            baseRandomSampleNormalized,
                                            sampleSkylineNormalized, skylineAttributeColumns) * 100.0;
                                        double setCoverageCoveredByEntireBestRank =
                                            SetCoverage.GetCoverage(baseRandomSampleNormalized,
                                                entireSkylineDataTableBestRankNormalized, skylineAttributeColumns) *
                                            100.0;
                                        double setCoverageCoveredByEntireSumRank =
                                            SetCoverage.GetCoverage(baseRandomSampleNormalized,
                                                entireSkylineDataTableSumRankNormalized, skylineAttributeColumns) *
                                            100.0;

                                        setCoverageSecondRandom.Add(setCoverageCoveredBySecondRandomSample);
                                        setCoverageSample.Add(setCoverageCoveredBySkylineSample);
                                        setCoverageBestRank.Add(setCoverageCoveredByEntireBestRank);
                                        setCoverageSumRank.Add(setCoverageCoveredByEntireSumRank);

                                        double representationErrorSecondRandomSample = SetCoverage
                                            .GetRepresentationError(
                                                GetReducedSkyline(entireSkylineNormalized, secondRandomSampleNormalized),
                                                secondRandomSampleNormalized, skylineAttributeColumns).Max() * 100.0;
                                        double representationErrorSkylineSample = SetCoverage.GetRepresentationError(
                                            GetReducedSkyline(entireSkylineNormalized, sampleSkylineNormalized),
                                            sampleSkylineNormalized, skylineAttributeColumns).Max() * 100.0;
                                        double representationErrorEntireBestRank =
                                            SetCoverage.GetRepresentationError(
                                                GetReducedSkyline(entireSkylineNormalized,
                                                    entireSkylineDataTableBestRankNormalized),
                                                entireSkylineDataTableBestRankNormalized, skylineAttributeColumns).Max() *
                                            100.0;
                                        double representationErrorEntireSumRank =
                                            SetCoverage.GetRepresentationError(
                                                GetReducedSkyline(entireSkylineNormalized,
                                                    entireSkylineDataTableSumRankNormalized),
                                                entireSkylineDataTableSumRankNormalized, skylineAttributeColumns).Max() *
                                            100.0;

                                        representationErrorSecondRandom.Add(representationErrorSecondRandomSample);
                                        representationErrorSample.Add(representationErrorSkylineSample);
                                        representationErrorBestRank.Add(representationErrorEntireBestRank);
                                        representationErrorSumRank.Add(representationErrorEntireSumRank);

                                        double representationErrorSumSecondRandomSample = SetCoverage
                                            .GetRepresentationError(
                                                GetReducedSkyline(entireSkylineNormalized, secondRandomSampleNormalized),
                                                secondRandomSampleNormalized, skylineAttributeColumns).Sum() * 100.0;
                                        double representationErrorSumSkylineSample = SetCoverage.GetRepresentationError(
                                            GetReducedSkyline(entireSkylineNormalized, sampleSkylineNormalized),
                                            sampleSkylineNormalized, skylineAttributeColumns).Sum() * 100.0;
                                        double representationErrorSumEntireBestRank =
                                            SetCoverage.GetRepresentationError(
                                                GetReducedSkyline(entireSkylineNormalized,
                                                    entireSkylineDataTableBestRankNormalized),
                                                entireSkylineDataTableBestRankNormalized, skylineAttributeColumns).Sum() *
                                            100.0;
                                        double representationErrorSumEntireSumRank =
                                            SetCoverage.GetRepresentationError(
                                                GetReducedSkyline(entireSkylineNormalized,
                                                    entireSkylineDataTableSumRankNormalized),
                                                entireSkylineDataTableSumRankNormalized, skylineAttributeColumns)
                                                .Sum() * 100.0;

                                        representationErrorSumSecondRandom.Add(
                                            representationErrorSumSecondRandomSample);
                                        representationErrorSumSample.Add(representationErrorSumSkylineSample);
                                        representationErrorSumBestRank.Add(representationErrorSumEntireBestRank);
                                        representationErrorSumSumRank.Add(representationErrorSumEntireSumRank);

                                        var dominatedObjectsCountRandomSample =
                                            new DominatedObjects(entireDatabase,
                                                secondRandomSampleDatabase,
                                                skylineAttributeColumns);
                                        var dominatedObjectsCountSampleSkyline =
                                            new DominatedObjects(entireDatabase, sampleSkylineDatabase,
                                                skylineAttributeColumns);
                                        var dominatedObjectsCountEntireSkylineBestRank =
                                            new DominatedObjects(entireDatabase,
                                                entireSkylineDataTableBestRankDatabase, skylineAttributeColumns);
                                        var dominatedObjectsCountEntireSkylineSumRank =
                                            new DominatedObjects(entireDatabase,
                                                entireSkylineDataTableSumRankDatabase, skylineAttributeColumns);

                                        dominatedObjectsCountSamplingSecondRandom.Add(
                                            dominatedObjectsCountRandomSample.NumberOfDistinctDominatedObjects);
                                        dominatedObjectsCountSamplingSample.Add(
                                            dominatedObjectsCountSampleSkyline.NumberOfDistinctDominatedObjects);
                                        dominatedObjectsCountSamplingBestRank.Add(
                                            dominatedObjectsCountEntireSkylineBestRank.NumberOfDistinctDominatedObjects);
                                        dominatedObjectsCountSamplingSumRank.Add(
                                            dominatedObjectsCountEntireSkylineSumRank.NumberOfDistinctDominatedObjects);

                                        dominatedObjectsOfBestObjectSamplingSecondRandom.Add(
                                            dominatedObjectsCountRandomSample
                                                .NumberOfObjectsDominatedByEachObjectOrderedByDescCount.First().Value);
                                        dominatedObjectsOfBestObjectSamplingSample.Add(
                                            dominatedObjectsCountSampleSkyline
                                                .NumberOfObjectsDominatedByEachObjectOrderedByDescCount.First().Value);
                                        dominatedObjectsOfBestObjectSamplingBestRank.Add(
                                            dominatedObjectsCountEntireSkylineBestRank
                                                .NumberOfObjectsDominatedByEachObjectOrderedByDescCount.First().Value);
                                        dominatedObjectsOfBestObjectSamplingSumRank.Add(
                                            dominatedObjectsCountEntireSkylineSumRank
                                                .NumberOfObjectsDominatedByEachObjectOrderedByDescCount.First().Value);

                                        IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>> sampleBuckets =
                                                ClusterAnalysis.GetBuckets(sampleSkylineNormalized,
                                                    skylineAttributeColumns);
                                        IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                            aggregatedSampleBuckets =
                                                ClusterAnalysis.GetAggregatedBuckets(sampleBuckets);
                                         IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>> bestRankBuckets =
                                                ClusterAnalysis.GetBuckets(
                                                    entireSkylineDataTableBestRankNormalized,
                                                    skylineAttributeColumns);
                                        IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                            aggregatedBestRankBuckets =
                                                ClusterAnalysis.GetAggregatedBuckets(
                                                    bestRankBuckets);
                                        IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                                            sumRankBuckets =
                                                ClusterAnalysis.GetBuckets(
                                                    entireSkylineDataTableSumRankNormalized,
                                                    skylineAttributeColumns);
                                        IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                            aggregatedSumRankBuckets =
                                                ClusterAnalysis.GetAggregatedBuckets(
                                                    sumRankBuckets);

                                        FillTopBucketsSampling(clusterAnalysisTopBucketsSampling, ClusterAnalysisSampling.SampleSkyline, sampleBuckets, sampleSkylineNormalized.Count, entireDatabaseNormalized.Count,entireSkylineNormalized.Count);                            
                                        FillTopBucketsSampling(clusterAnalysisTopBucketsSampling, ClusterAnalysisSampling.BestRank, bestRankBuckets, entireSkylineDataTableBestRankNormalized.Count, entireDatabaseNormalized.Count,entireSkylineNormalized.Count);                            
                                        FillTopBucketsSampling(clusterAnalysisTopBucketsSampling, ClusterAnalysisSampling.SumRank, sumRankBuckets, entireSkylineDataTableSumRankNormalized.Count, entireDatabaseNormalized.Count,entireSkylineNormalized.Count);                            

                                         IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                                       sampleMedianBuckets =
                                           clusterAnalysisForMedian.GetBuckets(sampleSkylineNormalized,
                                               skylineAttributeColumns,true);
                                        IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                       aggregatedSampleMedianBuckets =
                                           ClusterAnalysis.GetAggregatedBuckets(sampleMedianBuckets);
                                        IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                                            bestRankMedianBuckets =
                                                clusterAnalysisForMedian.GetBuckets(
                                                    entireSkylineDataTableBestRankNormalized,
                                                    skylineAttributeColumns, true);
                                        IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                            aggregatedBestRankMedianBuckets =
                                                ClusterAnalysis.GetAggregatedBuckets(
                                                    bestRankMedianBuckets);
                                        IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                                            sumRankMedianBuckets =
                                                clusterAnalysisForMedian.GetBuckets(
                                                    entireSkylineDataTableSumRankNormalized,
                                                    skylineAttributeColumns, true);
                                        IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                            aggregatedSumRankMedianBuckets =
                                                ClusterAnalysis.GetAggregatedBuckets(
                                                    sumRankMedianBuckets);

                                        FillTopBucketsSampling(clusterAnalysisMedianTopBucketsSampling, ClusterAnalysisSampling.SampleSkyline, sampleMedianBuckets, sampleSkylineNormalized.Count, entireDatabaseNormalized.Count,entireSkylineNormalized.Count);                            
                                        FillTopBucketsSampling(clusterAnalysisMedianTopBucketsSampling, ClusterAnalysisSampling.BestRank, bestRankMedianBuckets, entireSkylineDataTableBestRankNormalized.Count, entireDatabaseNormalized.Count,entireSkylineNormalized.Count);                            
                                        FillTopBucketsSampling(clusterAnalysisMedianTopBucketsSampling, ClusterAnalysisSampling.SumRank, sumRankMedianBuckets, entireSkylineDataTableSumRankNormalized.Count, entireDatabaseNormalized.Count,entireSkylineNormalized.Count);                            

                                        var caEntireDbNew = new List<double>();
                                        var caEntireSkylineNew = new List<double>();
                                        var caSampleSkylineNew = new List<double>();
                                        var caBestRankNew = new List<double>();
                                        var caSumRankNew = new List<double>();
                                     
                                        for (var ii = 0; ii < skylineAttributeColumns.Length; ii++)
                                        {
                                            int entireSkyline = aggregatedEntireSkylineBuckets.ContainsKey(ii)
                                                ? aggregatedEntireSkylineBuckets[ii].Count
                                                : 0;
                                            int sampleSkyline = aggregatedSampleBuckets.ContainsKey(ii)
                                                ? aggregatedSampleBuckets[ii].Count
                                                : 0;
                                            double entireSkylinePercent = (double) entireSkyline /
                                                                          entireSkylineNormalized.Count;
                                            double sampleSkylinePercent = (double) sampleSkyline /
                                                                          sampleSkylineNormalized.Count;
                                            int entireDb = aggregatedEntireDatabaseBuckets.ContainsKey(ii)
                                                ? aggregatedEntireDatabaseBuckets[ii].Count
                                                : 0;
                                            double entireDbPercent = (double) entireDb /
                                                                     entireDatabaseNormalized.Count;

                                            int bestRank = aggregatedBestRankBuckets.ContainsKey(ii)
                                                ? aggregatedBestRankBuckets[ii].Count
                                                : 0;
                                            int sumRank = aggregatedSumRankBuckets.ContainsKey(ii)
                                                ? aggregatedSumRankBuckets[ii].Count
                                                : 0;

                                            double bestRankPercent = (double) bestRank /
                                                                     entireSkylineDataTableBestRankNormalized.Count;
                                            double sumRankPercent = (double) sumRank /
                                                                    entireSkylineDataTableSumRankNormalized.Count;
                                            caEntireDbNew.Add(entireDbPercent);
                                            caEntireSkylineNew.Add(entireSkylinePercent);
                                            caSampleSkylineNew.Add(sampleSkylinePercent);
                                            caBestRankNew.Add(bestRankPercent);
                                            caSumRankNew.Add(sumRankPercent);
                                        }

                                        var caMedianEntireDbNew = new List<double>();
                                        var caMedianEntireSkylineNew = new List<double>();
                                        var caMedianSampleSkylineNew = new List<double>();
                                        var caMedianBestRankNew = new List<double>();
                                        var caMedianSumRankNew = new List<double>();

                                        for (var ii = 0; ii < skylineAttributeColumns.Length; ii++)
                                        {
                                            int entireSkyline = aggregatedEntireSkylineMedianBuckets.ContainsKey(ii)
                                                ? aggregatedEntireSkylineMedianBuckets[ii].Count
                                                : 0;
                                            int sampleSkyline = aggregatedSampleMedianBuckets.ContainsKey(ii)
                                                ? aggregatedSampleMedianBuckets[ii].Count
                                                : 0;
                                            double entireSkylinePercent = (double)entireSkyline /
                                                                          entireSkylineNormalized.Count;
                                            double sampleSkylinePercent = (double)sampleSkyline /
                                                                          sampleSkylineNormalized.Count;
                                            int entireDb = aggregatedEntireDatabaseMedianBuckets.ContainsKey(ii)
                                                ? aggregatedEntireDatabaseMedianBuckets[ii].Count
                                                : 0;
                                            double entireDbPercent = (double)entireDb /
                                                                     entireDatabaseNormalized.Count;

                                            int bestRank = aggregatedBestRankMedianBuckets.ContainsKey(ii)
                                                ? aggregatedBestRankMedianBuckets[ii].Count
                                                : 0;
                                            int sumRank = aggregatedSumRankMedianBuckets.ContainsKey(ii)
                                                ? aggregatedSumRankMedianBuckets[ii].Count
                                                : 0;
                                            
                                            double bestRankPercent = (double)bestRank /
                                                                     entireSkylineDataTableBestRankNormalized.Count;
                                            double sumRankPercent = (double)sumRank /
                                                                    entireSkylineDataTableSumRankNormalized.Count;
                                            caMedianEntireDbNew.Add(entireDbPercent);
                                            caMedianEntireSkylineNew.Add(entireSkylinePercent);
                                            caMedianSampleSkylineNew.Add(sampleSkylinePercent);
                                            caMedianBestRankNew.Add(bestRankPercent);
                                            caMedianSumRankNew.Add(sumRankPercent);
                                        }

                                        clusterAnalysisSampling[ClusterAnalysisSampling.EntireDb].Add(caEntireDbNew);
                                        clusterAnalysisSampling[ClusterAnalysisSampling.EntireSkyline].Add(
                                            caEntireSkylineNew);
                                        clusterAnalysisSampling[ClusterAnalysisSampling.SampleSkyline].Add(
                                            caSampleSkylineNew);
                                        clusterAnalysisSampling[ClusterAnalysisSampling.BestRank].Add(
                                            caBestRankNew);
                                        clusterAnalysisSampling[ClusterAnalysisSampling.SumRank].Add(
                                            caSumRankNew);

                                        clusterAnalysisMedianSampling[ClusterAnalysisSampling.EntireDb].Add(caMedianEntireDbNew);
                                        clusterAnalysisMedianSampling[ClusterAnalysisSampling.EntireSkyline].Add(
                                            caMedianEntireSkylineNew);
                                        clusterAnalysisMedianSampling[ClusterAnalysisSampling.SampleSkyline].Add(
                                            caMedianSampleSkylineNew);
                                        clusterAnalysisMedianSampling[ClusterAnalysisSampling.BestRank].Add(
                                            caMedianBestRankNew);
                                        clusterAnalysisMedianSampling[ClusterAnalysisSampling.SumRank].Add(
                                            caMedianSumRankNew);

                                        subspaceCount++;
                                    }

                                    Dictionary<ClusterAnalysisSampling, string> clusterAnalysisStrings =
                                        GetClusterAnalysisStrings(skylineAttributeColumns, clusterAnalysisSampling);
                                    Dictionary<ClusterAnalysisSampling, string> clusterAnalysisMedianStrings =
                                        GetClusterAnalysisStrings(skylineAttributeColumns, clusterAnalysisMedianSampling);
                                    Dictionary<ClusterAnalysisSampling, string> clusterAnalysisTopBucketsStrings =
                                       GetClusterAnalysisTopBucketsStrings(clusterAnalysisTopBucketsSampling);
                                    Dictionary<ClusterAnalysisSampling, string> clusterAnalysisMedianTopBucketsStrings =
                                        GetClusterAnalysisTopBucketsStrings(clusterAnalysisMedianTopBucketsSampling);

                                    var time = (long) (subspaceTime.Average() + .5);
                                    var objects = (long) (subspaceObjects.Average() + .5);
                                    var elapsed = (long) (subspaceTimeElapsed.Average() + .5);

                                    reportDimensions.Add(preferences.Count);
                                    reportSkylineSize.Add(objects);
                                    reportTimeTotal.Add(elapsed);
                                    reportTimeAlgorithm.Add(time);
                                    reportCorrelation.Add(correlation);
                                    reportCardinality.Add(cardinality);

                                    var setCoverageSamplingSingle =
                                        new Dictionary<SkylineTypesSingleSampling, List<double>>
                                        {
                                            {SkylineTypesSingleSampling.Random, setCoverageSecondRandom},
                                            {SkylineTypesSingleSampling.Sample, setCoverageSample},
                                            {SkylineTypesSingleSampling.BestRank, setCoverageBestRank},
                                            {SkylineTypesSingleSampling.SumRank, setCoverageSumRank}
                                        };

                                    var representationErrorSamplingSingle =
                                        new Dictionary<SkylineTypesSingleSampling, List<double>>
                                        {
                                            {SkylineTypesSingleSampling.Random, representationErrorSecondRandom},
                                            {SkylineTypesSingleSampling.Sample, representationErrorSample},
                                            {SkylineTypesSingleSampling.BestRank, representationErrorBestRank},
                                            {SkylineTypesSingleSampling.SumRank, representationErrorSumRank}
                                        };

                                    var representationErrorSumSamplingSingle =
                                        new Dictionary<SkylineTypesSingleSampling, List<double>>
                                        {
                                            {SkylineTypesSingleSampling.Random, representationErrorSumSecondRandom},
                                            {SkylineTypesSingleSampling.Sample, representationErrorSumSample},
                                            {SkylineTypesSingleSampling.BestRank, representationErrorSumBestRank},
                                            {SkylineTypesSingleSampling.SumRank, representationErrorSumSumRank}
                                        };

                                    var dominatedObjectsCountSamplingSingle =
                                        new Dictionary<SkylineTypesSingleSampling, List<double>>()
                                        {
                                            {
                                                SkylineTypesSingleSampling.Random,
                                                dominatedObjectsCountSamplingSecondRandom
                                            },
                                            {
                                                SkylineTypesSingleSampling.Sample,
                                                dominatedObjectsCountSamplingSample
                                            },
                                            {
                                                SkylineTypesSingleSampling.BestRank,
                                                dominatedObjectsCountSamplingBestRank
                                            },
                                            {
                                                SkylineTypesSingleSampling.SumRank,
                                                dominatedObjectsCountSamplingSumRank
                                            }
                                        };                                   

                                    var dominatedObjectsOfBestObjectSamplingSingle =
                                        new Dictionary<SkylineTypesSingleSampling, List<double>>
                                        {
                                            {
                                                SkylineTypesSingleSampling.Random,
                                                dominatedObjectsOfBestObjectSamplingSecondRandom
                                            },
                                            {
                                                SkylineTypesSingleSampling.Sample,
                                                dominatedObjectsOfBestObjectSamplingSample
                                            },
                                            {
                                                SkylineTypesSingleSampling.BestRank,
                                                dominatedObjectsOfBestObjectSamplingBestRank
                                            },
                                            {
                                                SkylineTypesSingleSampling.SumRank,
                                                dominatedObjectsOfBestObjectSamplingSumRank
                                            }
                                        };

                                    AddToReportsSampling(reportsSamplingLong, subspaceObjects, subspaceTime,
                                        reportsSamplingDouble);
                                    AddToSetCoverageSampling(setCoverageSampling, setCoverageSamplingSingle);
                                    AddToSetCoverageSampling(representationErrorSampling,
                                        representationErrorSamplingSingle);
                                    AddToSetCoverageSampling(representationErrorSumSampling,
                                        representationErrorSumSamplingSingle);
                                    AddToSetCoverageSampling(dominatedObjectsCountSampling,
                                        dominatedObjectsCountSamplingSingle);
                                    AddToSetCoverageSampling(dominatedObjectsOfBestObjectSampling,
                                        dominatedObjectsOfBestObjectSamplingSingle);

                                    string strLine = FormatLineStringSample(strPreferenceSet, strTrial,
                                        preferences.Count, objects,
                                        elapsed, time, subspaceTime.Min(), subspaceTime.Max(),
                                        Mathematic.GetSampleVariance(subspaceTime),
                                        Mathematic.GetSampleStdDeviation(subspaceTime), subspaceObjects.Min(),
                                        subspaceObjects.Max(), Mathematic.GetSampleVariance(subspaceObjects),
                                        Mathematic.GetSampleStdDeviation(subspaceObjects),
                                        setCoverageSamplingSingle, representationErrorSamplingSingle,
                                        representationErrorSumSamplingSingle, dominatedObjectsCountSamplingSingle,
                                        dominatedObjectsOfBestObjectSamplingSingle,
                                        clusterAnalysisStrings, clusterAnalysisMedianStrings, clusterAnalysisTopBucketsStrings, clusterAnalysisMedianTopBucketsStrings, correlation,                                   
                                        cardinality);

                                    Debug.WriteLine(strLine);
                                    sb.AppendLine(strLine);
                                }
                                else
                                {
                                    sw.Start();
                                    if (UseCLR)
                                    {
                                        string strSP = parser.ParsePreferenceSQL(strSQL);
                                        var dap = new SqlDataAdapter(strSP, cnnSQL);
                                        dt.Clear(); //clear datatable
                                        dap.Fill(dt);
                                    }
                                    else
                                    {
                                        parser.Cardinality = (long) cardinality;
                                        dt = parser.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName,
                                            strSQL);
                                    }
                                    long timeAlgorithm = parser.TimeInMilliseconds;
                                    long numberOfOperations = parser.NumberOfOperations;
                                    sw.Stop();

                                    reportDimensions.Add(preferences.Count);
                                    reportSkylineSize.Add(dt.Rows.Count);
                                    reportTimeTotal.Add(sw.ElapsedMilliseconds);
                                    reportTimeAlgorithm.Add(timeAlgorithm);
                                    reportCorrelation.Add(correlation);
                                    reportCardinality.Add(cardinality);

                                    //trial|dimensions|skyline size|time total|time algorithm
                                    string strTrial = iTrial + 1 + " / " + _trials;
                                    string strPreferenceSet = iPreferenceIndex + 1 + " / " + listPreferences.Count;
                                    Console.WriteLine(strPreferenceSet);

                                    string strLine = FormatLineString(strPreferenceSet, strTrial, preferences.Count,
                                        dt.Rows.Count, sw.ElapsedMilliseconds, timeAlgorithm, correlation, cardinality);
                                    
                                    Debug.WriteLine(strLine);
                                    sb.AppendLine(strLine);
                                }
                            }
                            catch (Exception e)
                            {
                                Debug.WriteLine(e.Message);
                                return;
                            }
                        }
                    }
                    else
                    {


                        strSQL = parser.ParsePreferenceSQL(strSQL);

                        string[] sizes = { "small", "medium", "large", "superlarge" };

                        //Format for each of the customer profiles
                        sb.AppendLine("PRINT '----- -------------------------------------------------------- ------'");
                        sb.AppendLine("PRINT '----- " + (preferences.Count + 1) + " dimensions  ------'");
                        sb.AppendLine("PRINT '----- -------------------------------------------------------- ------'");
                        foreach (string size in sizes)
                        {
                            sb.AppendLine("GO"); //we need this in order the profiler shows each query in a new line
                            sb.AppendLine(strSQL.Replace("cars", "cars_" + size));

                        }

                            
                    }

                    //}

                }

                ////////////////////////////////
                //Summary
                ///////////////////////////////
                if (GenerateScript == false)
                {
                    if (Sampling)
                    {
                        AddSummarySample(sb, strSeparatorLine, reportDimensions, reportSkylineSize, reportTimeTotal, reportTimeAlgorithm, reportsSamplingLong, reportsSamplingDouble, setCoverageSampling, representationErrorSampling, representationErrorSumSampling, dominatedObjectsCountSampling, dominatedObjectsOfBestObjectSampling, reportCorrelation, reportCardinality);
                    }
                    else
                    {
                        AddSummary(sb, strSeparatorLine, reportDimensions, reportSkylineSize, reportTimeTotal, reportTimeAlgorithm, reportCorrelation, reportCardinality);
                    }
                }

                //Write some empty lines (clarification in output window)
                Debug.WriteLine("");
                Debug.WriteLine("");
                Debug.WriteLine("");


                //Write in file
                string strFiletype;

                if (GenerateScript == false)
                {
                    strFiletype = ".csv";
                }
                else
                {
                    strFiletype = ".sql";
                }
                //create filename
                string strFileName = Path + "Performance_" + Set.ToString() + "_" + currentStrategy + strFiletype;

                StreamWriter outfile = new StreamWriter(strFileName);
                outfile.Write(sb.ToString());
                outfile.Close();
            }

            //close connection
            if (UseCLR)
            {
                cnnSQL.Close();
            }
            
        }

        private static void FillTopBucketsSampling(Dictionary<ClusterAnalysisSampling, Dictionary<BigInteger, List<double>>> clusterAnalysisTopBuckets, ClusterAnalysisSampling sampleSkylineType, IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>> skylineTypeBuckets, int skylineCount, int entireDbCount, int entireSkylineCount)
        {            
            List<KeyValuePair<BigInteger, List<IReadOnlyDictionary<long, object[]>>>> sortedTop5 =
                skylineTypeBuckets.OrderByDescending(l => l.Value.Count).ThenBy(l => l.Key).Take(5).ToList(); // track top 5 buckets

            foreach (KeyValuePair<BigInteger, List<IReadOnlyDictionary<long, object[]>>> skylineTypeBucket in sortedTop5)
            {
                if (!clusterAnalysisTopBuckets[sampleSkylineType].ContainsKey(skylineTypeBucket.Key))
                {
                    clusterAnalysisTopBuckets[sampleSkylineType].Add(skylineTypeBucket.Key, new List<double>());
                }
                double percent = (double)skylineTypeBucket.Value.Count / skylineCount;
                clusterAnalysisTopBuckets[sampleSkylineType][skylineTypeBucket.Key].Add(percent);
            }

            foreach (KeyValuePair<BigInteger, List<double>> entireDbBucket in clusterAnalysisTopBuckets[ClusterAnalysisSampling.EntireDb]) // additionally track top 5 buckets of entire db
            {
                if (!clusterAnalysisTopBuckets[sampleSkylineType].ContainsKey(entireDbBucket.Key))
                {
                    clusterAnalysisTopBuckets[sampleSkylineType].Add(entireDbBucket.Key, new List<double>());
                }

                if (!skylineTypeBuckets.ContainsKey(entireDbBucket.Key)) // not contained => percentage = 0
                {
                    clusterAnalysisTopBuckets[sampleSkylineType][entireDbBucket.Key].Add(0);
                }
                else if (sortedTop5.All(item => item.Key != entireDbBucket.Key)) // else: already added in previous foreach => no need to add again
                {
                    double percent = (double) skylineTypeBuckets[entireDbBucket.Key].Count / entireDbCount;
                    clusterAnalysisTopBuckets[sampleSkylineType][entireDbBucket.Key].Add(percent);
                }
            }

            foreach (KeyValuePair<BigInteger, List<double>> entireSkylineBucket in clusterAnalysisTopBuckets[ClusterAnalysisSampling.EntireSkyline]) // additionally track top 5 buckets of entire skyline
            {
                if (!clusterAnalysisTopBuckets[sampleSkylineType].ContainsKey(entireSkylineBucket.Key))
                {
                    clusterAnalysisTopBuckets[sampleSkylineType].Add(entireSkylineBucket.Key, new List<double>());
                }

                if (!skylineTypeBuckets.ContainsKey(entireSkylineBucket.Key)) // not contained => percentage = 0
                {
                    clusterAnalysisTopBuckets[sampleSkylineType][entireSkylineBucket.Key].Add(0);
                }
                else if (sortedTop5.All(item => item.Key != entireSkylineBucket.Key)) // else: already added in previous foreach => no need to add again
                {
                    double percent = (double)skylineTypeBuckets[entireSkylineBucket.Key].Count / entireSkylineCount;
                    clusterAnalysisTopBuckets[sampleSkylineType][entireSkylineBucket.Key].Add(percent);
                }
            }
        }

        private IReadOnlyDictionary<long, object[]> GetReducedSkyline(IReadOnlyDictionary<long, object[]> baseSkylineNormalized, IReadOnlyDictionary<long, object[]> subtractSkylineNormalized)
        {
            var ret = new Dictionary<long, object[]>();

            foreach (KeyValuePair<long, object[]> baseObject in baseSkylineNormalized)
            {
                if (!subtractSkylineNormalized.ContainsKey(baseObject.Key))
                {
                    ret.Add(baseObject.Key, baseObject.Value);
                }
            }

            return new ReadOnlyDictionary<long, object[]>(ret);
        }

        private static Dictionary<ClusterAnalysisSampling, string> GetClusterAnalysisTopBucketsStrings(Dictionary<ClusterAnalysisSampling, Dictionary<BigInteger, List<double>>> clusterAnalysisTopBuckets)
        {
            var clusterAnalysisStrings =
              new Dictionary<ClusterAnalysisSampling, string>()
                {
                    {ClusterAnalysisSampling.EntireDb, ""},
                    {ClusterAnalysisSampling.EntireSkyline, ""},
                    {ClusterAnalysisSampling.SampleSkyline, ""},
                    {ClusterAnalysisSampling.BestRank, ""},
                    {ClusterAnalysisSampling.SumRank, ""}
                };

            int subspacesCount = clusterAnalysisTopBuckets[ClusterAnalysisSampling.EntireDb].Values.First().Count;

            IOrderedEnumerable<KeyValuePair<BigInteger, List<double>>> allEntireDbBuckets = clusterAnalysisTopBuckets[ClusterAnalysisSampling.EntireDb].OrderByDescending(
                l => l.Value.Sum() / subspacesCount).ThenBy(l => l.Key);
            IOrderedEnumerable<KeyValuePair<BigInteger, List<double>>> allEntireSkylineBuckets = clusterAnalysisTopBuckets[ClusterAnalysisSampling.EntireSkyline].OrderByDescending(
               l => l.Value.Sum() / subspacesCount).ThenBy(l => l.Key);

            foreach (
                ClusterAnalysisSampling clusterAnalysisType in
                    Enum.GetValues(typeof (ClusterAnalysisSampling)).Cast<ClusterAnalysisSampling>())
            {
                foreach (KeyValuePair<BigInteger, List<double>> bucket in allEntireDbBuckets)
                {
                    double percent = clusterAnalysisTopBuckets[clusterAnalysisType][bucket.Key].Sum() / subspacesCount;
                    clusterAnalysisStrings[clusterAnalysisType] += "EB-" + bucket.Key + ":" +
                                                                   string.Format("{0:0.00},", percent * 100);
                }

                clusterAnalysisStrings[clusterAnalysisType] = clusterAnalysisStrings[clusterAnalysisType].TrimEnd(',');

                if (clusterAnalysisType != ClusterAnalysisSampling.EntireDb)
                {
                    clusterAnalysisStrings[clusterAnalysisType] += ";";

                    foreach (KeyValuePair<BigInteger, List<double>> bucket in allEntireSkylineBuckets.Take(5))
                    {
                        double percent = clusterAnalysisTopBuckets[clusterAnalysisType][bucket.Key].Sum() /
                                         subspacesCount;
                        clusterAnalysisStrings[clusterAnalysisType] += "ESB-" + bucket.Key + ":" +
                                                                       string.Format("{0:0.00},", percent * 100);
                    }

                    clusterAnalysisStrings[clusterAnalysisType] =
                        clusterAnalysisStrings[clusterAnalysisType].TrimEnd(',');
                    if (clusterAnalysisType != ClusterAnalysisSampling.EntireSkyline)
                    {
                        clusterAnalysisStrings[clusterAnalysisType] += ";";

                        foreach (
                            KeyValuePair<BigInteger, List<double>> bucket in
                                clusterAnalysisTopBuckets[clusterAnalysisType].OrderByDescending(
                                    l => l.Value.Sum() / subspacesCount).ThenBy(l => l.Key).Take(5))
                        {
                            double percent = clusterAnalysisTopBuckets[clusterAnalysisType][bucket.Key].Sum() /
                                             subspacesCount;
                            clusterAnalysisStrings[clusterAnalysisType] += "SB-" + bucket.Key + ":" +
                                                                           string.Format("{0:0.00},", percent * 100);
                        }

                        clusterAnalysisStrings[clusterAnalysisType] =
                            clusterAnalysisStrings[clusterAnalysisType].TrimEnd(',');
                    }
                }
            }

            return clusterAnalysisStrings;
        }

        private static Dictionary<ClusterAnalysisSampling, string> GetClusterAnalysisStrings(int[] skylineAttributeColumns, Dictionary<ClusterAnalysisSampling, List<List<double>>> clusterAnalysisSampling)
        {
            var clusterAnalysisAverages =
                new Dictionary<ClusterAnalysisSampling, List<double>>()
                {
                    {ClusterAnalysisSampling.EntireDb, new List<double>()},
                    {ClusterAnalysisSampling.EntireSkyline, new List<double>()},
                    {ClusterAnalysisSampling.SampleSkyline, new List<double>()},
                    {ClusterAnalysisSampling.BestRank, new List<double>()},
                    {ClusterAnalysisSampling.SumRank, new List<double>()}
                };

            var clusterAnalysisStrings =
                new Dictionary<ClusterAnalysisSampling, string>()
                {
                    {ClusterAnalysisSampling.EntireDb, ""},
                    {ClusterAnalysisSampling.EntireSkyline, ""},
                    {ClusterAnalysisSampling.SampleSkyline, ""},
                    {ClusterAnalysisSampling.BestRank, ""},
                    {ClusterAnalysisSampling.SumRank, ""}
                };

            for (var bucket = 0; bucket < skylineAttributeColumns.Length; bucket++)
            {
                clusterAnalysisAverages[ClusterAnalysisSampling.EntireDb].Add(0);
                clusterAnalysisAverages[ClusterAnalysisSampling.EntireSkyline].Add(0);
                clusterAnalysisAverages[ClusterAnalysisSampling.SampleSkyline].Add(0);
                clusterAnalysisAverages[ClusterAnalysisSampling.BestRank].Add(0);
                clusterAnalysisAverages[ClusterAnalysisSampling.SumRank].Add(0);
            }

            foreach (
                ClusterAnalysisSampling clusterAnalysisType in
                    Enum.GetValues(typeof (ClusterAnalysisSampling)).Cast<ClusterAnalysisSampling>())
            {
                foreach (List<double> row in clusterAnalysisSampling[clusterAnalysisType])
                {
                    for (var bucket = 0; bucket < row.Count; bucket++)
                    {
                        clusterAnalysisAverages[clusterAnalysisType][bucket] += row[bucket];
                    }
                }

                for (var bucket = 0; bucket < skylineAttributeColumns.Length; bucket++)
                {
                    clusterAnalysisAverages[clusterAnalysisType][bucket] /= clusterAnalysisSampling[clusterAnalysisType].Count;
                }

                foreach (double averageValue in clusterAnalysisAverages[clusterAnalysisType])
                {
                    clusterAnalysisStrings[clusterAnalysisType] += string.Format("{0:0.00};", averageValue * 100);
                }

                clusterAnalysisStrings[clusterAnalysisType] = clusterAnalysisStrings[clusterAnalysisType].TrimEnd(';');
            }
            return clusterAnalysisStrings;
        }

        private static IReadOnlyDictionary<long, object[]> GetEntireSkylineDataTableRankNormalized(DataTable entireSkyline, List<long[]> skylineValues, int[] skylineAttributeColumns, int numberOfRecords, int sortType, out  IReadOnlyDictionary<long, object[]> skylineDatabase)
        {
            var sortedDataTable=new DataTable();

              if (sortType == 1)
              {
                  sortedDataTable = prefSQL.SQLSkyline.Helper.SortByRank(entireSkyline,skylineValues);
              } 
                else if (sortType == 2)
                {
                     sortedDataTable = prefSQL.SQLSkyline.Helper.SortBySum(entireSkyline,skylineValues);
                }

              prefSQL.SQLSkyline.Helper.GetAmountOfTuples(sortedDataTable, numberOfRecords);

            skylineDatabase = prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(sortedDataTable, 0);      
      
              IReadOnlyDictionary<long, object[]> sortedDataTableNormalized =
                prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(sortedDataTable, 0);
            SkylineSamplingHelper.NormalizeColumns(sortedDataTableNormalized, skylineAttributeColumns);
            return sortedDataTableNormalized;
        }

        private List<IEnumerable<CLRSafeHashSet<int>>> ProduceSubspaces(ArrayList preferences)
        {
            var randomSubspacesProducer = new RandomSkylineSamplingSubspacesProducer
            {
                AllPreferencesCount = preferences.Count,
                SubspacesCount = SamplingSubspacesCount,
                SubspaceDimension = SamplingSubspaceDimension
            };

            var producedSubspaces = new List<IEnumerable<CLRSafeHashSet<int>>>();
            for (var ii = 0; ii < SamplingSamplesCount; ii++)
            {
                producedSubspaces.Add(randomSubspacesProducer.GetSubspaces());
            }
            return producedSubspaces;
        }

        private static IReadOnlyDictionary<long, object[]> GetEntireDatabaseNormalized(SQLCommon parser, string strSQL, int[] skylineAttributeColumns, out DataTable dtEntire)
        {
            DbProviderFactory factory = DbProviderFactories.GetFactory(Helper.ProviderName);

            // use the factory object to create Data access objects.
            DbConnection connection = factory.CreateConnection();
            // will return the connection object (i.e. SqlConnection ...)
            connection.ConnectionString = Helper.ConnectionString;

            dtEntire = new DataTable();

            connection.Open();

            DbDataAdapter dap = factory.CreateDataAdapter();
            DbCommand selectCommand = connection.CreateCommand();
            selectCommand.CommandTimeout = 0; //infinite timeout

            string strQueryEntire;
            string operatorsEntire;
            int numberOfRecordsEntire;
            string[] parameterEntire;

            string ansiSqlEntire =
                parser.GetAnsiSqlFromPrefSqlModel(
                    parser.GetPrefSqlModelFromPreferenceSql(strSQL));
            prefSQL.SQLParser.Helper.DetermineParameters(ansiSqlEntire, out parameterEntire,
                out strQueryEntire, out operatorsEntire,
                out numberOfRecordsEntire);

            selectCommand.CommandText = strQueryEntire;
            dap.SelectCommand = selectCommand;
            dtEntire = new DataTable();

            dap.Fill(dtEntire);

            for (var ii = 0; ii < skylineAttributeColumns.Length; ii++)
            {
                dtEntire.Columns.RemoveAt(0);
            }

            IReadOnlyDictionary<long, object[]> entireDatabaseNormalized =
                                         prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(dtEntire, 0);
            SkylineSamplingHelper.NormalizeColumns(entireDatabaseNormalized, skylineAttributeColumns);

            return entireDatabaseNormalized;
        }

        private static void AddToSetCoverageSampling(Dictionary<SkylineTypesSampling, List<double>> setCoverageSampling, Dictionary<SkylineTypesSingleSampling, List<double>> setCoverageSingleSampling)
        {
            setCoverageSampling[SkylineTypesSampling.RandomAvg].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.Random].Average());
            setCoverageSampling[SkylineTypesSampling.RandomMin].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.Random].Min());
            setCoverageSampling[SkylineTypesSampling.RandomMax].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.Random].Max());
            setCoverageSampling[SkylineTypesSampling.RandomVar].Add(Mathematic.GetSampleVariance(setCoverageSingleSampling[SkylineTypesSingleSampling.Random]));
            setCoverageSampling[SkylineTypesSampling.RandomStdDev].Add(Mathematic.GetSampleStdDeviation(setCoverageSingleSampling[SkylineTypesSingleSampling.Random]));
            setCoverageSampling[SkylineTypesSampling.SampleAvg].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.Sample].Average());
            setCoverageSampling[SkylineTypesSampling.SampleMin].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.Sample].Min());
            setCoverageSampling[SkylineTypesSampling.SampleMax].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.Sample].Max());
            setCoverageSampling[SkylineTypesSampling.SampleVar].Add(Mathematic.GetSampleVariance(setCoverageSingleSampling[SkylineTypesSingleSampling.Sample]));
            setCoverageSampling[SkylineTypesSampling.SampleStdDev].Add(Mathematic.GetSampleStdDeviation(setCoverageSingleSampling[SkylineTypesSingleSampling.Sample]));

            setCoverageSampling[SkylineTypesSampling.BestRankAvg].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.BestRank].Average());
            setCoverageSampling[SkylineTypesSampling.BestRankMin].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.BestRank].Min());
            setCoverageSampling[SkylineTypesSampling.BestRankMax].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.BestRank].Max());
            setCoverageSampling[SkylineTypesSampling.BestRankVar].Add(Mathematic.GetSampleVariance(setCoverageSingleSampling[SkylineTypesSingleSampling.BestRank]));
            setCoverageSampling[SkylineTypesSampling.BestRankStdDev].Add(Mathematic.GetSampleStdDeviation(setCoverageSingleSampling[SkylineTypesSingleSampling.BestRank]));
            setCoverageSampling[SkylineTypesSampling.SumRankAvg].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.SumRank].Average());
            setCoverageSampling[SkylineTypesSampling.SumRankMin].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.SumRank].Min());
            setCoverageSampling[SkylineTypesSampling.SumRankMax].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.SumRank].Max());
            setCoverageSampling[SkylineTypesSampling.SumRankVar].Add(Mathematic.GetSampleVariance(setCoverageSingleSampling[SkylineTypesSingleSampling.SumRank]));
            setCoverageSampling[SkylineTypesSampling.SumRankStdDev].Add(Mathematic.GetSampleStdDeviation(setCoverageSingleSampling[SkylineTypesSingleSampling.SumRank]));
        }

        private static void AddToReportsSampling(Dictionary<ReportsSampling, List<long>> reportsSamplingLong, List<long> subspaceObjects, List<long> subspaceTime,
            Dictionary<ReportsSampling, List<double>> reportsSamplingDouble)
        {
            reportsSamplingLong[ReportsSampling.SizeMin].Add(subspaceObjects.Min());
            reportsSamplingLong[ReportsSampling.TimeMin].Add(subspaceTime.Min());
            reportsSamplingLong[ReportsSampling.SizeMax].Add(subspaceObjects.Max());
            reportsSamplingLong[ReportsSampling.TimeMax].Add(subspaceTime.Max());
            reportsSamplingDouble[ReportsSampling.SizeVar].Add(Mathematic.GetSampleVariance(subspaceObjects));
            reportsSamplingDouble[ReportsSampling.TimeVar].Add(Mathematic.GetSampleVariance(subspaceTime));
            reportsSamplingDouble[ReportsSampling.SizeStdDev].Add(Mathematic.GetSampleStdDeviation(subspaceObjects));
            reportsSamplingDouble[ReportsSampling.TimeStdDev].Add(Mathematic.GetSampleStdDeviation(subspaceTime));
        }

        private static void InitClusterAnalysisSamplingDataStructures(out Dictionary<ClusterAnalysisSampling, List<List<double>>> clusterAnalysisSampling)
        {
            clusterAnalysisSampling = new Dictionary<ClusterAnalysisSampling, List<List<double>>>()
            {
                {ClusterAnalysisSampling.EntireDb, new List<List<double>>()},
                {ClusterAnalysisSampling.EntireSkyline, new List<List<double>>()},
                {ClusterAnalysisSampling.SampleSkyline, new List<List<double>>()},
                {ClusterAnalysisSampling.BestRank, new List<List<double>>()},
                {ClusterAnalysisSampling.SumRank, new List<List<double>>()}
            };
        }

        private static void InitClusterAnalysisTopBucketsSamplingDataStructures(out Dictionary<ClusterAnalysisSampling, Dictionary<BigInteger, List<double>>> clusterAnalysisTopBucketsSampling)
        {
            clusterAnalysisTopBucketsSampling = new Dictionary<ClusterAnalysisSampling, Dictionary<BigInteger, List<double>>>()
            {
                {ClusterAnalysisSampling.EntireDb, new Dictionary<BigInteger, List<double>>()},
                {ClusterAnalysisSampling.EntireSkyline, new Dictionary<BigInteger, List<double>>()},
                {ClusterAnalysisSampling.SampleSkyline, new Dictionary<BigInteger, List<double>>()},
                {ClusterAnalysisSampling.BestRank, new Dictionary<BigInteger, List<double>>()},
                {ClusterAnalysisSampling.SumRank, new Dictionary<BigInteger, List<double>>()}
            };
        }

        private static void InitSamplingDataStructures(out Dictionary<ReportsSampling, List<long>> reportsSamplingLong, out Dictionary<ReportsSampling, List<double>> reportsSamplingDouble, out Dictionary<SkylineTypesSampling, List<double>> setCoverageSampling, out Dictionary<SkylineTypesSampling, List<double>> representationErrorSampling, out Dictionary<SkylineTypesSampling, List<double>> representationErrorSumSampling, out Dictionary<SkylineTypesSampling, List<double>> dominatedObjectsCountSampling, out Dictionary<SkylineTypesSampling, List<double>> dominatedObjectsByBestObjectSampling, out Dictionary<ClusterAnalysisSampling, List<List<double>>> clusterAnalysisSampling, out Dictionary<ClusterAnalysisSampling, List<List<double>>> clusterAnalysisMedianSampling, out Dictionary<ClusterAnalysisSampling, Dictionary<BigInteger, List<double>>> clusterAnalysisTopBucketsSampling, out Dictionary<ClusterAnalysisSampling, Dictionary<BigInteger, List<double>>> clusterAnalysisMedianTopBucketsSampling)
        {
            reportsSamplingLong = new Dictionary<ReportsSampling, List<long>>
            {
                {ReportsSampling.SizeMin, new List<long>()},
                {ReportsSampling.TimeMin, new List<long>()},
                {ReportsSampling.SizeMax, new List<long>()},
                {ReportsSampling.TimeMax, new List<long>()}
            };
            reportsSamplingDouble = new Dictionary<ReportsSampling, List<double>>
            {
                {ReportsSampling.SizeVar, new List<double>()},
                {ReportsSampling.TimeVar, new List<double>()},
                {ReportsSampling.SizeStdDev, new List<double>()},
                {ReportsSampling.TimeStdDev, new List<double>()}
            };

            setCoverageSampling = new Dictionary<SkylineTypesSampling, List<double>>()
            {
                {SkylineTypesSampling.RandomAvg, new List<double>()},
                {SkylineTypesSampling.RandomMin, new List<double>()},
                {SkylineTypesSampling.RandomMax, new List<double>()},
                {SkylineTypesSampling.RandomVar, new List<double>()},
                {SkylineTypesSampling.RandomStdDev, new List<double>()},
                {SkylineTypesSampling.SampleAvg, new List<double>()},
                {SkylineTypesSampling.SampleMin, new List<double>()},
                {SkylineTypesSampling.SampleMax, new List<double>()},
                {SkylineTypesSampling.SampleVar, new List<double>()},
                {SkylineTypesSampling.SampleStdDev, new List<double>()},
                {SkylineTypesSampling.BestRankAvg, new List<double>()},
                {SkylineTypesSampling.BestRankMin, new List<double>()},
                {SkylineTypesSampling.BestRankMax, new List<double>()},
                {SkylineTypesSampling.BestRankVar, new List<double>()},
                {SkylineTypesSampling.BestRankStdDev, new List<double>()},
                {SkylineTypesSampling.SumRankAvg, new List<double>()},
                {SkylineTypesSampling.SumRankMin, new List<double>()},
                {SkylineTypesSampling.SumRankMax, new List<double>()},
                {SkylineTypesSampling.SumRankVar, new List<double>()},
                {SkylineTypesSampling.SumRankStdDev, new List<double>()}
            };

            representationErrorSampling = new Dictionary<SkylineTypesSampling, List<double>>()
            {
                {SkylineTypesSampling.RandomAvg, new List<double>()},
                {SkylineTypesSampling.RandomMin, new List<double>()},
                {SkylineTypesSampling.RandomMax, new List<double>()},
                {SkylineTypesSampling.RandomVar, new List<double>()},
                {SkylineTypesSampling.RandomStdDev, new List<double>()},
                {SkylineTypesSampling.SampleAvg, new List<double>()},
                {SkylineTypesSampling.SampleMin, new List<double>()},
                {SkylineTypesSampling.SampleMax, new List<double>()},
                {SkylineTypesSampling.SampleVar, new List<double>()},
                {SkylineTypesSampling.SampleStdDev, new List<double>()},
                {SkylineTypesSampling.BestRankAvg, new List<double>()},
                {SkylineTypesSampling.BestRankMin, new List<double>()},
                {SkylineTypesSampling.BestRankMax, new List<double>()},
                {SkylineTypesSampling.BestRankVar, new List<double>()},
                {SkylineTypesSampling.BestRankStdDev, new List<double>()},
                {SkylineTypesSampling.SumRankAvg, new List<double>()},
                {SkylineTypesSampling.SumRankMin, new List<double>()},
                {SkylineTypesSampling.SumRankMax, new List<double>()},
                {SkylineTypesSampling.SumRankVar, new List<double>()},
                {SkylineTypesSampling.SumRankStdDev, new List<double>()}
            };

            representationErrorSumSampling = new Dictionary<SkylineTypesSampling, List<double>>()
            {
                {SkylineTypesSampling.RandomAvg, new List<double>()},
                {SkylineTypesSampling.RandomMin, new List<double>()},
                {SkylineTypesSampling.RandomMax, new List<double>()},
                {SkylineTypesSampling.RandomVar, new List<double>()},
                {SkylineTypesSampling.RandomStdDev, new List<double>()},
                {SkylineTypesSampling.SampleAvg, new List<double>()},
                {SkylineTypesSampling.SampleMin, new List<double>()},
                {SkylineTypesSampling.SampleMax, new List<double>()},
                {SkylineTypesSampling.SampleVar, new List<double>()},
                {SkylineTypesSampling.SampleStdDev, new List<double>()},
                {SkylineTypesSampling.BestRankAvg, new List<double>()},
                {SkylineTypesSampling.BestRankMin, new List<double>()},
                {SkylineTypesSampling.BestRankMax, new List<double>()},
                {SkylineTypesSampling.BestRankVar, new List<double>()},
                {SkylineTypesSampling.BestRankStdDev, new List<double>()},
                {SkylineTypesSampling.SumRankAvg, new List<double>()},
                {SkylineTypesSampling.SumRankMin, new List<double>()},
                {SkylineTypesSampling.SumRankMax, new List<double>()},
                {SkylineTypesSampling.SumRankVar, new List<double>()},
                {SkylineTypesSampling.SumRankStdDev, new List<double>()}
            };

            dominatedObjectsCountSampling = new Dictionary<SkylineTypesSampling, List<double>>()
            {
                {SkylineTypesSampling.RandomAvg, new List<double>()},
                {SkylineTypesSampling.RandomMin, new List<double>()},
                {SkylineTypesSampling.RandomMax, new List<double>()},
                {SkylineTypesSampling.RandomVar, new List<double>()},
                {SkylineTypesSampling.RandomStdDev, new List<double>()},
                {SkylineTypesSampling.SampleAvg, new List<double>()},
                {SkylineTypesSampling.SampleMin, new List<double>()},
                {SkylineTypesSampling.SampleMax, new List<double>()},
                {SkylineTypesSampling.SampleVar, new List<double>()},
                {SkylineTypesSampling.SampleStdDev, new List<double>()},
                {SkylineTypesSampling.BestRankAvg, new List<double>()},
                {SkylineTypesSampling.BestRankMin, new List<double>()},
                {SkylineTypesSampling.BestRankMax, new List<double>()},
                {SkylineTypesSampling.BestRankVar, new List<double>()},
                {SkylineTypesSampling.BestRankStdDev, new List<double>()},
                {SkylineTypesSampling.SumRankAvg, new List<double>()},
                {SkylineTypesSampling.SumRankMin, new List<double>()},
                {SkylineTypesSampling.SumRankMax, new List<double>()},
                {SkylineTypesSampling.SumRankVar, new List<double>()},
                {SkylineTypesSampling.SumRankStdDev, new List<double>()}
            };

            dominatedObjectsByBestObjectSampling = new Dictionary<SkylineTypesSampling, List<double>>()
            {
                {SkylineTypesSampling.RandomAvg, new List<double>()},
                {SkylineTypesSampling.RandomMin, new List<double>()},
                {SkylineTypesSampling.RandomMax, new List<double>()},
                {SkylineTypesSampling.RandomVar, new List<double>()},
                {SkylineTypesSampling.RandomStdDev, new List<double>()},
                {SkylineTypesSampling.SampleAvg, new List<double>()},
                {SkylineTypesSampling.SampleMin, new List<double>()},
                {SkylineTypesSampling.SampleMax, new List<double>()},
                {SkylineTypesSampling.SampleVar, new List<double>()},
                {SkylineTypesSampling.SampleStdDev, new List<double>()},
                {SkylineTypesSampling.BestRankAvg, new List<double>()},
                {SkylineTypesSampling.BestRankMin, new List<double>()},
                {SkylineTypesSampling.BestRankMax, new List<double>()},
                {SkylineTypesSampling.BestRankVar, new List<double>()},
                {SkylineTypesSampling.BestRankStdDev, new List<double>()},
                {SkylineTypesSampling.SumRankAvg, new List<double>()},
                {SkylineTypesSampling.SumRankMin, new List<double>()},
                {SkylineTypesSampling.SumRankMax, new List<double>()},
                {SkylineTypesSampling.SumRankVar, new List<double>()},
                {SkylineTypesSampling.SumRankStdDev, new List<double>()}
            };

            InitClusterAnalysisSamplingDataStructures(out clusterAnalysisSampling); 
            InitClusterAnalysisSamplingDataStructures(out clusterAnalysisMedianSampling);
            InitClusterAnalysisTopBucketsSamplingDataStructures(out clusterAnalysisTopBucketsSampling);
            InitClusterAnalysisTopBucketsSamplingDataStructures(out clusterAnalysisMedianTopBucketsSampling);
        }

        private DataTable GetSQLFromPreferences(ArrayList preferences, bool cardinality)
        {
            SQLCommon common = new SQLCommon();
            string strPrefSQL = "SELECT CAST(cars.id AS BIGINT) AS id FROM ";
            if (TableSize == Size.Small)
            {
                strPrefSQL += "cars_small";
            }
            else if (TableSize == Size.Medium)
            {
                strPrefSQL += "cars_medium";
            }
            else if (TableSize == Size.Large)
            {
                strPrefSQL += "cars_large";
            }
            strPrefSQL += " cars ";
            strPrefSQL += "SKYLINE OF ";


            for (int i = 0; i < preferences.Count; i++)
            {
                strPrefSQL += preferences[i] + ",";
            }
            strPrefSQL = strPrefSQL.TrimEnd(',');

            PrefSQLModel prefModel = common.GetPrefSqlModelFromPreferenceSql(strPrefSQL);

            string strSQL = "SELECT ";

            for (int i = 0; i < prefModel.Skyline.Count; i++)
            {
                if (cardinality)
                {
                    strSQL += "COUNT(DISTINCT " + prefModel.Skyline[i].Expression + "),";
                }
                else
                {
                    strSQL += prefModel.Skyline[i].Expression + ",";
                }
            }
            strSQL = strSQL.TrimEnd(',') + " FROM cars ";
            strSQL += GetJoinsForPreferences(strSQL);





            DataTable dt = Helper.ExecuteStatement(strSQL);

            return dt;
        }

        private string GetJoinsForPreferences(string strSkylineOf)
        {
            string strSQL = "";
            if (strSkylineOf.IndexOf("colors", StringComparison.Ordinal) > 0)
            {
                strSQL += "LEFT OUTER JOIN colors ON cars.color_id = colors.ID ";
            }
            if (strSkylineOf.IndexOf("fuels", StringComparison.Ordinal) > 0)
            {
                strSQL += "LEFT OUTER JOIN fuels ON cars.fuel_id = fuels.ID ";
            }
            if (strSkylineOf.IndexOf("bodies", StringComparison.Ordinal) > 0)
            {
                strSQL += "LEFT OUTER JOIN bodies ON cars.body_id = bodies.ID ";
            }
            if (strSkylineOf.IndexOf("makes", StringComparison.Ordinal) > 0)
            {
                strSQL += "LEFT OUTER JOIN makes ON cars.make_id = makes.ID ";
            }
            if (strSkylineOf.IndexOf("conditions", StringComparison.Ordinal) > 0)
            {
                strSQL += "LEFT OUTER JOIN conditions ON cars.condition_id = conditions.ID ";
            }
            if (strSkylineOf.IndexOf("models", StringComparison.Ordinal) > 0)
            {
                strSQL += "LEFT OUTER JOIN models ON cars.model_id = models.ID ";
            }
            if (strSkylineOf.IndexOf("transmissions", StringComparison.Ordinal) > 0)
            {
                strSQL += "LEFT OUTER JOIN transmissions ON cars.transmission_id = transmissions.ID ";
            }
            if (strSkylineOf.IndexOf("drives", StringComparison.Ordinal) > 0)
            {
                strSQL += "LEFT OUTER JOIN drives ON cars.drive_id = drives.ID ";
            }


            return strSQL;
        }

        private ArrayList GetCorrelationMatrix(ArrayList preferences)
        {
            DataTable dt = GetSQLFromPreferences(preferences, false);
            double[] colA = new double[dt.Rows.Count];
            double[] colB = new double[dt.Rows.Count];

            //Calculate correlation between the attributes
            ArrayList listCorrelation = new ArrayList();
            for (int iIndex = 0; iIndex < preferences.Count; iIndex++)
            {
                for (int iPref = 0; iPref <= iIndex; iPref++)
                {
                    //Don't compare same preferences (correlation is always 1)
                    if (iIndex != iPref)
                    {
                        for (int i = 0; i < dt.Rows.Count; i++)
                        {
                            colA[i] = (int)dt.Rows[i][iIndex];
                            colB[i] = (int)dt.Rows[i][iPref];
                        }

                        double correlation = Mathematic.GetPearson(colA, colB);
                        CorrelationModel model = new CorrelationModel(preferences[iIndex].ToString(), preferences[iPref].ToString(), correlation);
                        listCorrelation.Add(model);
                    }


                }
            }


            return listCorrelation;
        }



        private ArrayList GetCardinalityOfPreferences(ArrayList preferences)
        {
            DataTable dt = GetSQLFromPreferences(preferences, true);

            //Calculate correlation between the attributes
            ArrayList listCardinality = new ArrayList();

            for (int iIndex = 0; iIndex < preferences.Count; iIndex++)
            {
                CardinalityModel model = new CardinalityModel(preferences[iIndex].ToString(), (int)dt.Rows[0][iIndex]);
                listCardinality.Add(model);
            }

            return listCardinality;
        }

        private double SearchCorrelation(ArrayList preferences, ArrayList correlationMatrix)
        {

            double sumCorrelation = 0;
            for (int i = 0; i < preferences.Count; i++)
            {
                for (int ii = i+1; ii < preferences.Count; ii++)
                {
                    bool bFound = false;
                    for (int iModel = 0; iModel < correlationMatrix.Count; iModel++)
                    {
                        CorrelationModel model = (CorrelationModel)correlationMatrix[iModel];
                        if (model.ColA.Equals(preferences[i].ToString()) && model.ColB.Equals(preferences[ii].ToString()))
                        {
                            sumCorrelation += model.Correlation;
                            bFound = true;
                            break;
                        }
                        else if (model.ColB.Equals(preferences[i].ToString()) && model.ColA.Equals(preferences[ii].ToString()))
                        {
                            bFound = true;
                            sumCorrelation += model.Correlation;
                            break;
                        }
                    }
                    if (bFound == false)
                    {
                        throw new Exception("correlation factor not found");
                    }
                }
            }
            return sumCorrelation;
        }



        private double SearchCardinality(ArrayList preferences, ArrayList cardinality)
        {

            double product = 1;
            for (int i = 0; i < preferences.Count; i++)
            {
                bool bFound = false;
                for (int iModel = 0; iModel < cardinality.Count; iModel++)
                {
                    CardinalityModel model = (CardinalityModel)cardinality[iModel];
                    if (model.Col.Equals(preferences[i].ToString()))
                    {
                        product *= model.Cardinality;
                        bFound = true;
                        break;
                    }
                }
                if (bFound == false)
                {
                    throw new Exception("cardinality factor not found");
                }
            }
            return product;
        }


        /// <summary>
        /// Create all possible combinations from x preferences
        /// </summary>
        /// <param name="arr"></param>
        /// <param name="len"></param>
        /// <param name="startPosition"></param>
        /// <param name="result"></param>
        /// <param name="returnArray"></param>
        private void GetCombinations(ArrayList arr, int len, int startPosition, ArrayList result, ref ArrayList returnArray)
        {
            if(result.Count == 0)
            {
                for (int i = 0; i < len; i++)
                {
                    result.Add("");
                }
                    
            }

            if (len == 0)
            {
                returnArray.Add(result.Clone());
                return;
            }
            for (int i = startPosition; i <= arr.Count - len; i++)
            {
                result[result.Count - len] = (string)arr[i];
                GetCombinations(arr, len - 1, i + 1, result, ref returnArray);
            }
        }

        


        #region formatOutput

        private void AddSummarySample(StringBuilder sb, String strSeparatorLine, List<long> reportDimensions,
            List<long> reportSkylineSize, List<long> reportTimeTotal, List<long> reportTimeAlgorithm, IDictionary<ReportsSampling, List<long>> rsl, IDictionary<ReportsSampling, List<double>> rsd, Dictionary<SkylineTypesSampling, List<double>> setCoverageSampling, Dictionary<SkylineTypesSampling, List<double>> representationErrorSampling, Dictionary<SkylineTypesSampling, List<double>> representationErrorSumSampling, Dictionary<SkylineTypesSampling, List<double>> dominatedObjectsCountSampling, Dictionary<SkylineTypesSampling, List<double>> dominatedObjectsOfBestObjectSampling, List<double> reportCorrelation, List<double> reportCardinality)
        {
            //Separator Line
            Debug.WriteLine(strSeparatorLine);
            sb.AppendLine(strSeparatorLine);

            string[] setCoverageSamplingAverage = GetSummaryAverage(setCoverageSampling);
             string[] representationErrorSamplingAverage = GetSummaryAverage(representationErrorSampling);
             string[] representationErrorSumSamplingAverage = GetSummaryAverage(representationErrorSumSampling);
             string[] dominatedObjectsCountSamplingAverage = GetSummaryAverage(dominatedObjectsCountSampling);
             string[] dominatedObjectsOfBestObjectSamplingAverage = GetSummaryAverage(dominatedObjectsOfBestObjectSampling);

             string[] setCoverageSamplingMin = GetSummaryMin(setCoverageSampling);
             string[] representationErrorSamplingMin = GetSummaryMin(representationErrorSampling);
             string[] representationErrorSumSamplingMin = GetSummaryMin(representationErrorSumSampling);
             string[] dominatedObjectsCountSamplingMin = GetSummaryMin(dominatedObjectsCountSampling);
             string[] dominatedObjectsOfBestObjectSamplingMin = GetSummaryMin(dominatedObjectsOfBestObjectSampling);

              string[] setCoverageSamplingMax = GetSummaryMax(setCoverageSampling);
             string[] representationErrorSamplingMax = GetSummaryMax(representationErrorSampling);
             string[] representationErrorSumSamplingMax = GetSummaryMax(representationErrorSumSampling);
             string[] dominatedObjectsCountSamplingMax = GetSummaryMax(dominatedObjectsCountSampling);
             string[] dominatedObjectsOfBestObjectSamplingMax = GetSummaryMax(dominatedObjectsOfBestObjectSampling);

             string[] setCoverageSamplingVariance = GetSummaryVariance(setCoverageSampling);
             string[] representationErrorSamplingVariance = GetSummaryVariance(representationErrorSampling);
             string[] representationErrorSumSamplingVariance = GetSummaryVariance(representationErrorSumSampling);
             string[] dominatedObjectsCountSamplingVariance = GetSummaryVariance(dominatedObjectsCountSampling);
             string[] dominatedObjectsOfBestObjectSamplingVariance = GetSummaryVariance(dominatedObjectsOfBestObjectSampling);

             string[] setCoverageSamplingStdDev = GetSummaryStdDev(setCoverageSampling);
             string[] representationErrorSamplingStdDev = GetSummaryStdDev(representationErrorSampling);
             string[] representationErrorSumSamplingStdDev = GetSummaryStdDev(representationErrorSumSampling);
             string[] dominatedObjectsCountSamplingStdDev = GetSummaryStdDev(dominatedObjectsCountSampling);
             string[] dominatedObjectsOfBestObjectSamplingStdDev = GetSummaryStdDev(dominatedObjectsOfBestObjectSampling);

             string strAverage = FormatLineStringSample("average", "", reportDimensions.Average(), reportSkylineSize.Average(), reportTimeTotal.Average(), reportTimeAlgorithm.Average(), rsl[ReportsSampling.TimeMin].Average(), rsl[ReportsSampling.TimeMax].Average(), rsd[ReportsSampling.TimeVar].Average(), rsd[ReportsSampling.TimeStdDev].Average(), rsl[ReportsSampling.SizeMin].Average(), rsl[ReportsSampling.SizeMax].Average(), rsd[ReportsSampling.SizeVar].Average(), rsd[ReportsSampling.SizeStdDev].Average(), setCoverageSamplingAverage, representationErrorSamplingAverage, representationErrorSumSamplingAverage, dominatedObjectsCountSamplingAverage, dominatedObjectsOfBestObjectSamplingAverage, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, reportCorrelation.Average(), reportCardinality.Average());
             string strMin = FormatLineStringSample("minimum", "", reportDimensions.Min(), reportSkylineSize.Min(), reportTimeTotal.Min(), reportTimeAlgorithm.Min(), rsl[ReportsSampling.TimeMin].Min(), rsl[ReportsSampling.TimeMax].Min(), rsd[ReportsSampling.TimeVar].Min(), rsd[ReportsSampling.TimeStdDev].Min(), rsl[ReportsSampling.SizeMin].Min(), rsl[ReportsSampling.SizeMax].Min(), rsd[ReportsSampling.SizeVar].Min(), rsd[ReportsSampling.SizeStdDev].Min(), setCoverageSamplingMin, representationErrorSamplingMin, representationErrorSumSamplingMin, dominatedObjectsCountSamplingMin, dominatedObjectsOfBestObjectSamplingMin, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, reportCorrelation.Min(), reportCardinality.Min());
             string strMax = FormatLineStringSample("maximum", "", reportDimensions.Max(), reportSkylineSize.Max(), reportTimeTotal.Max(), reportTimeAlgorithm.Max(), rsl[ReportsSampling.TimeMin].Max(), rsl[ReportsSampling.TimeMax].Max(), rsd[ReportsSampling.TimeVar].Max(), rsd[ReportsSampling.TimeStdDev].Max(), rsl[ReportsSampling.SizeMin].Max(), rsl[ReportsSampling.SizeMax].Max(), rsd[ReportsSampling.SizeVar].Max(), rsd[ReportsSampling.SizeStdDev].Max(), setCoverageSamplingMax, representationErrorSamplingMax, representationErrorSumSamplingMax, dominatedObjectsCountSamplingMax, dominatedObjectsOfBestObjectSamplingMax, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, reportCorrelation.Max(), reportCardinality.Max());
             string strVar = FormatLineStringSample("variance", "", Mathematic.GetSampleVariance(reportDimensions), Mathematic.GetSampleVariance(reportSkylineSize), Mathematic.GetSampleVariance(reportTimeTotal), Mathematic.GetSampleVariance(reportTimeAlgorithm), Mathematic.GetSampleVariance(rsl[ReportsSampling.TimeMin]), Mathematic.GetSampleVariance(rsl[ReportsSampling.TimeMax]), Mathematic.GetSampleVariance(rsd[ReportsSampling.TimeVar]), Mathematic.GetSampleVariance(rsd[ReportsSampling.TimeStdDev]), Mathematic.GetSampleVariance(rsl[ReportsSampling.SizeMin]), Mathematic.GetSampleVariance(rsl[ReportsSampling.SizeMax]), Mathematic.GetSampleVariance(rsd[ReportsSampling.SizeVar]), Mathematic.GetSampleVariance(rsd[ReportsSampling.SizeStdDev]), setCoverageSamplingVariance, representationErrorSamplingVariance, representationErrorSumSamplingVariance, dominatedObjectsCountSamplingVariance, dominatedObjectsOfBestObjectSamplingVariance, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, Mathematic.GetSampleVariance(reportCorrelation), Mathematic.GetSampleVariance(reportCardinality));
             string strStd = FormatLineStringSample("stddeviation", "", Mathematic.GetSampleStdDeviation(reportDimensions), Mathematic.GetSampleStdDeviation(reportSkylineSize), Mathematic.GetSampleStdDeviation(reportTimeTotal), Mathematic.GetSampleStdDeviation(reportTimeAlgorithm), Mathematic.GetSampleStdDeviation(rsl[ReportsSampling.TimeMin]), Mathematic.GetSampleStdDeviation(rsl[ReportsSampling.TimeMax]), Mathematic.GetSampleStdDeviation(rsd[ReportsSampling.TimeVar]), Mathematic.GetSampleStdDeviation(rsd[ReportsSampling.TimeStdDev]), Mathematic.GetSampleStdDeviation(rsl[ReportsSampling.SizeMin]), Mathematic.GetSampleStdDeviation(rsl[ReportsSampling.SizeMax]), Mathematic.GetSampleStdDeviation(rsd[ReportsSampling.SizeVar]), Mathematic.GetSampleStdDeviation(rsd[ReportsSampling.SizeStdDev]), setCoverageSamplingStdDev, representationErrorSamplingStdDev, representationErrorSumSamplingStdDev, dominatedObjectsCountSamplingStdDev, dominatedObjectsOfBestObjectSamplingStdDev, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, Mathematic.GetSampleStdDeviation(reportCorrelation), Mathematic.GetSampleStdDeviation(reportCardinality));

            sb.AppendLine(strAverage);
            sb.AppendLine(strMin);
            sb.AppendLine(strMax);
            sb.AppendLine(strVar);
            sb.AppendLine(strStd);
            Debug.WriteLine(strAverage);
            Debug.WriteLine(strMin);
            Debug.WriteLine(strMax);
            Debug.WriteLine(strVar);
            Debug.WriteLine(strStd);

            //Separator Line
            sb.AppendLine(strSeparatorLine);
            Debug.WriteLine(strSeparatorLine);
        }

        private static string[] GetSummaryAverage(Dictionary<SkylineTypesSampling, List<double>> list)
        {
            var array = new[]
            {
                Math.Round(list[SkylineTypesSampling.RandomAvg].Average(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.RandomMin].Average(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.RandomMax].Average(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.RandomVar].Average(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.RandomStdDev].Average(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SampleAvg].Average(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SampleMin].Average(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SampleMax].Average(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SampleVar].Average(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SampleStdDev].Average(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.BestRankAvg].Average(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.BestRankMin].Average(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.BestRankMax].Average(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.BestRankVar].Average(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.BestRankStdDev].Average(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SumRankAvg].Average(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SumRankMin].Average(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SumRankMax].Average(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SumRankVar].Average(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SumRankStdDev].Average(), 2).ToString(CultureInfo.InvariantCulture)
            };
            return array;
        }

        private static string[] GetSummaryMin(Dictionary<SkylineTypesSampling, List<double>> list)
        {
            var array = new[]
            {
                Math.Round(list[SkylineTypesSampling.RandomAvg].Min(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.RandomMin].Min(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.RandomMax].Min(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.RandomVar].Min(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.RandomStdDev].Min(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SampleAvg].Min(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SampleMin].Min(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SampleMax].Min(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SampleVar].Min(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SampleStdDev].Min(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.BestRankAvg].Min(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.BestRankMin].Min(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.BestRankMax].Min(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.BestRankVar].Min(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.BestRankStdDev].Min(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SumRankAvg].Min(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SumRankMin].Min(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SumRankMax].Min(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SumRankVar].Min(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SumRankStdDev].Min(), 2).ToString(CultureInfo.InvariantCulture)
            };
            return array;
        }

        private static string[] GetSummaryMax(Dictionary<SkylineTypesSampling, List<double>> list)
        {
            var array = new[]
            {
                Math.Round(list[SkylineTypesSampling.RandomAvg].Max(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.RandomMin].Max(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.RandomMax].Max(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.RandomVar].Max(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.RandomStdDev].Max(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SampleAvg].Max(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SampleMin].Max(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SampleMax].Max(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SampleVar].Max(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SampleStdDev].Max(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.BestRankAvg].Max(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.BestRankMin].Max(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.BestRankMax].Max(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.BestRankVar].Max(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.BestRankStdDev].Max(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SumRankAvg].Max(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SumRankMin].Max(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SumRankMax].Max(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SumRankVar].Max(), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(list[SkylineTypesSampling.SumRankStdDev].Max(), 2).ToString(CultureInfo.InvariantCulture)
            };
            return array;
        }

        private static string[] GetSummaryVariance(Dictionary<SkylineTypesSampling, List<double>> list)
        {
            var array = new[]
            {
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.RandomAvg]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.RandomMin]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.RandomMax]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.RandomVar]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.RandomStdDev]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.SampleAvg]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.SampleMin]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.SampleMax]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.SampleVar]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.SampleStdDev]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.BestRankAvg]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.BestRankMin]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.BestRankMax]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.BestRankVar]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.BestRankStdDev]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.SumRankAvg]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.SumRankMin]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.SumRankMax]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.SumRankVar]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleVariance(list[SkylineTypesSampling.SumRankStdDev]), 2).ToString(CultureInfo.InvariantCulture)
            };
            return array;
        }

        private static string[] GetSummaryStdDev(Dictionary<SkylineTypesSampling, List<double>> list)
        {
            var array = new[]
            {
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.RandomAvg]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.RandomMin]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.RandomMax]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.RandomVar]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.RandomStdDev]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.SampleAvg]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.SampleMin]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.SampleMax]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.SampleVar]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.SampleStdDev]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.BestRankAvg]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.BestRankMin]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.BestRankMax]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.BestRankVar]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.BestRankStdDev]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.SumRankAvg]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.SumRankMin]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.SumRankMax]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.SumRankVar]), 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(Mathematic.GetSampleStdDeviation(list[SkylineTypesSampling.SumRankStdDev]), 2).ToString(CultureInfo.InvariantCulture)
            };
            return array;
        }

        private void AddSummary(StringBuilder sb, String strSeparatorLine, List<long> reportDimensions, List<long> reportSkylineSize, List<long> reportTimeTotal, List<long> reportTimeAlgorithm, List<double> reportCorrelation, List<double> reportCardinality)
        {
            //Separator Line
            Debug.WriteLine(strSeparatorLine);
            sb.AppendLine(strSeparatorLine);

            string strAverage = FormatLineString("average", "", reportDimensions.Average(), reportSkylineSize.Average(), reportTimeTotal.Average(), reportTimeAlgorithm.Average(), reportCorrelation.Average(), reportCardinality.Average());
            string strMin = FormatLineString("minimum", "", reportDimensions.Min(), reportSkylineSize.Min(), reportTimeTotal.Min(), reportTimeAlgorithm.Min(), reportCorrelation.Min(), reportCardinality.Min());
            string strMax = FormatLineString("maximum", "", reportDimensions.Max(), reportSkylineSize.Max(), reportTimeTotal.Max(), reportTimeAlgorithm.Max(), reportCorrelation.Max(), reportCardinality.Max());
            string strVar = FormatLineString("variance", "", Mathematic.GetVariance(reportDimensions), Mathematic.GetVariance(reportSkylineSize), Mathematic.GetVariance(reportTimeTotal), Mathematic.GetVariance(reportTimeAlgorithm), Mathematic.GetVariance(reportCorrelation), Mathematic.GetVariance(reportCardinality));
            string strStd = FormatLineString("stddeviation", "", Mathematic.GetStdDeviation(reportDimensions), Mathematic.GetStdDeviation(reportSkylineSize), Mathematic.GetStdDeviation(reportTimeTotal), Mathematic.GetStdDeviation(reportTimeAlgorithm), Mathematic.GetStdDeviation(reportCorrelation), Mathematic.GetStdDeviation(reportCardinality));
            string strSamplevar = FormatLineString("sample variance", "", Mathematic.GetSampleVariance(reportDimensions), Mathematic.GetSampleVariance(reportSkylineSize), Mathematic.GetSampleVariance(reportTimeTotal), Mathematic.GetSampleVariance(reportTimeAlgorithm), Mathematic.GetSampleVariance(reportCorrelation), Mathematic.GetSampleVariance(reportCardinality));
            string strSampleStd = FormatLineString("sample stddeviation", "", Mathematic.GetSampleStdDeviation(reportDimensions), Mathematic.GetSampleStdDeviation(reportSkylineSize), Mathematic.GetSampleStdDeviation(reportTimeTotal), Mathematic.GetSampleStdDeviation(reportTimeAlgorithm), Mathematic.GetSampleStdDeviation(reportCorrelation), Mathematic.GetSampleStdDeviation(reportCardinality));

            sb.AppendLine(strAverage);
            sb.AppendLine(strMin);
            sb.AppendLine(strMax);
            sb.AppendLine(strVar);
            sb.AppendLine(strStd);
            sb.AppendLine(strSamplevar);
            sb.AppendLine(strSampleStd);
            Debug.WriteLine(strAverage);
            Debug.WriteLine(strMin);
            Debug.WriteLine(strMax);
            Debug.WriteLine(strVar);
            Debug.WriteLine(strStd);
            Debug.WriteLine(strSamplevar);
            Debug.WriteLine(strSampleStd);

            //Separator Line
            sb.AppendLine(strSeparatorLine);
            Debug.WriteLine(strSeparatorLine);
        }



        private string FormatLineString(char paddingChar, string strTitle, string strTrial, string strDimension, string strSkyline, string strTimeTotal, string strTimeAlgo, string strCorrelation, string strCardinality)
        {
            //average line
            //trial|dimensions|skyline size|time total|time algorithm|correlation|
            string[] line = new string[9];
            line[0] = strTitle.PadLeft(19, paddingChar);
            line[1] = strTrial.PadLeft(11, paddingChar);
            line[2] = strDimension.PadLeft(10, paddingChar);
            line[3] = strSkyline.PadLeft(20, paddingChar);
            line[4] = strTimeTotal.PadLeft(20, paddingChar);
            line[5] = strTimeAlgo.PadLeft(20, paddingChar);
            line[6] = strCorrelation.PadLeft(20, paddingChar);
            line[7] = strCardinality.PadLeft(25, paddingChar);
            line[8] = "";
            return string.Join("|", line);
        }

        private static string FormatLineStringSample(char paddingChar, string strTitle, string strTrial,
            string strDimension, string strSkyline, string strTimeTotal, string strTimeAlgo, string minTime,
            string maxTime, string varianceTime, string sedDevTime, string minSize, string maxSize, string varianceSize,
            string stdDevSize, string[] setCoverageSampling,
            string[] representationErrorSampling,
            string[] representationErrorSumSampling,
            string[] dominatedObjectsCountSampling,
            string[] dominatedObjectsOfBestObjectSampling, string[] clusterAnalysisStrings, string[] clusterAnalysisMedianStrings, string[] clusterAnalysisTopBucketsStrings, string[] clusterAnalysisMedianTopBucketsStrings, string strCorrelation,
            string strCardinality)
        {
               var sb=new StringBuilder();

            sb.Append(strTitle.PadLeft(19, paddingChar));
            sb.Append("|");
            sb.Append(strTrial.PadLeft(11, paddingChar));
            sb.Append("|");
            sb.Append(strDimension.PadLeft(10, paddingChar));
            sb.Append("|");
            sb.Append(strSkyline.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strTimeTotal.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strTimeAlgo.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(minTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(maxTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(varianceTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(sedDevTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(minSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(maxSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(varianceSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(stdDevSize.PadLeft(20, paddingChar));
            sb.Append("|");
            foreach (string s in setCoverageSampling)
            {
                sb.Append(s.PadLeft(20, paddingChar));
                sb.Append("|");
            }
            foreach (string s in representationErrorSampling)
            {
                sb.Append(s.PadLeft(20, paddingChar));
                sb.Append("|");
            }
            foreach (string s in representationErrorSumSampling)
            {
                sb.Append(s.PadLeft(20, paddingChar));
                sb.Append("|");
            }
            foreach (string s in dominatedObjectsCountSampling)
            {
                sb.Append(s.PadLeft(20, paddingChar));
                sb.Append("|");
            }
            foreach (string s in dominatedObjectsOfBestObjectSampling)
            {
                sb.Append(s.PadLeft(20, paddingChar));
                sb.Append("|");
            }
            foreach (string s in clusterAnalysisStrings)
            {
                sb.Append(s.PadLeft(130, paddingChar));
                sb.Append("|");
            }
            foreach (string s in clusterAnalysisMedianStrings)
            {
                sb.Append(s.PadLeft(130, paddingChar));
                sb.Append("|");
            }
            foreach (string s in clusterAnalysisTopBucketsStrings)
            {
                sb.Append(s.PadLeft(250, paddingChar));
                sb.Append("|");
            }
            foreach (string s in clusterAnalysisMedianTopBucketsStrings)
            {
                sb.Append(s.PadLeft(250, paddingChar));
                sb.Append("|");
            }      
            sb.Append(strCorrelation.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strCardinality.PadLeft(25, paddingChar));
            sb.Append("|");

            return sb.ToString();
        }

        private static string FormatLineStringSample(char paddingChar, string strTitle, string strTrial, string strDimension, string strSkyline, string strTimeTotal, string strTimeAlgo, string minTime, string maxTime, string varianceTime, string sedDevTime, string minSize, string maxSize, string varianceSize, string stdDevSize, Dictionary<SkylineTypesSingleSampling, List<double>> setCoverageSampling, Dictionary<SkylineTypesSingleSampling, List<double>> representationErrorSampling, Dictionary<SkylineTypesSingleSampling, List<double>> representationErrorSumSampling, Dictionary<SkylineTypesSingleSampling, List<double>> dominatedObjectsCountSampling, Dictionary<SkylineTypesSingleSampling, List<double>> dominatedObjectsOfBestObjectSampling, Dictionary<ClusterAnalysisSampling, string> clusterAnalysisStrings, Dictionary<ClusterAnalysisSampling, string> clusterAnalysisMedianStrings, Dictionary<ClusterAnalysisSampling, string> clusterAnalysisTopBucketsStrings, Dictionary<ClusterAnalysisSampling, string> clusterAnalysisMedianTopBucketsStrings, string strCorrelation, string strCardinality)
        {
            var sb=new StringBuilder();

            sb.Append(strTitle.PadLeft(19, paddingChar));
            sb.Append("|");
            sb.Append(strTrial.PadLeft(11, paddingChar));
            sb.Append("|");
            sb.Append(strDimension.PadLeft(10, paddingChar));
            sb.Append("|");
            sb.Append(strSkyline.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strTimeTotal.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strTimeAlgo.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(minTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(maxTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(varianceTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(sedDevTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(minSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(maxSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(varianceSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(stdDevSize.PadLeft(20, paddingChar));
            sb.Append("|");
            AppendValues(sb, paddingChar, setCoverageSampling);
            AppendValues(sb, paddingChar, representationErrorSampling);
            AppendValues(sb, paddingChar, representationErrorSumSampling);
            AppendValues(sb, paddingChar, dominatedObjectsCountSampling);
            AppendValues(sb, paddingChar, dominatedObjectsOfBestObjectSampling);
            AppendClusterAnalysisValues(sb, paddingChar, 130, clusterAnalysisStrings);
            AppendClusterAnalysisValues(sb, paddingChar, 130, clusterAnalysisMedianStrings);
            AppendClusterAnalysisValues(sb, paddingChar, 250, clusterAnalysisTopBucketsStrings);
            AppendClusterAnalysisValues(sb, paddingChar, 250, clusterAnalysisMedianTopBucketsStrings); 
            sb.Append(strCorrelation.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strCardinality.PadLeft(25, paddingChar));
            sb.Append("|");

            return sb.ToString();
        }

        private static void AppendValues(StringBuilder sb, char paddingChar, Dictionary<SkylineTypesSingleSampling, List<double>> setCoverageSampling)
        {
            if (setCoverageSampling.Count == 0)
            {
                for (var i = 0; i < 20; i++)
                {
                    sb.Append("".PadLeft(20, paddingChar));
                    sb.Append("|");
                }

                return;
            }

            sb.Append(
                Math.Round(setCoverageSampling[SkylineTypesSingleSampling.Random].Average(), 2)
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(
                Math.Round(setCoverageSampling[SkylineTypesSingleSampling.Random].Min(), 2)
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(
                Math.Round(setCoverageSampling[SkylineTypesSingleSampling.Random].Max(), 2)
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(
                Mathematic.GetSampleVariance(setCoverageSampling[SkylineTypesSingleSampling.Random])
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(
                Mathematic.GetSampleStdDeviation(setCoverageSampling[SkylineTypesSingleSampling.Random])
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(
                Math.Round(setCoverageSampling[SkylineTypesSingleSampling.Sample].Average(), 2)
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(
                Math.Round(setCoverageSampling[SkylineTypesSingleSampling.Sample].Min(), 2)
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(
                Math.Round(setCoverageSampling[SkylineTypesSingleSampling.Sample].Max(), 2)
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(
                Mathematic.GetSampleVariance(setCoverageSampling[SkylineTypesSingleSampling.Sample])
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(
                Mathematic.GetSampleStdDeviation(setCoverageSampling[SkylineTypesSingleSampling.Sample])
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(
                Math.Round(setCoverageSampling[SkylineTypesSingleSampling.BestRank].Average(), 2)
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(
                Math.Round(setCoverageSampling[SkylineTypesSingleSampling.BestRank].Min(), 2)
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(
                Math.Round(setCoverageSampling[SkylineTypesSingleSampling.BestRank].Max(), 2)
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(
                Mathematic.GetSampleVariance(setCoverageSampling[SkylineTypesSingleSampling.BestRank])
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(
                Mathematic.GetSampleStdDeviation(setCoverageSampling[SkylineTypesSingleSampling.BestRank])
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(
                Math.Round(setCoverageSampling[SkylineTypesSingleSampling.SumRank].Average(), 2)
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(
                Math.Round(setCoverageSampling[SkylineTypesSingleSampling.SumRank].Min(), 2)
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(
                Math.Round(setCoverageSampling[SkylineTypesSingleSampling.SumRank].Max(), 2)
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(
                Mathematic.GetSampleVariance(setCoverageSampling[SkylineTypesSingleSampling.SumRank])
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(
                Mathematic.GetSampleStdDeviation(setCoverageSampling[SkylineTypesSingleSampling.SumRank])
                    .ToString(CultureInfo.InvariantCulture)
                    .PadLeft(20, paddingChar));
            sb.Append("|");
        }

        private static void AppendClusterAnalysisValues(StringBuilder sb, char paddingChar, int paddingCount, Dictionary<ClusterAnalysisSampling, string> clusterAnalysisStrings)
        {
            if (clusterAnalysisStrings.Count == 0)
            {
                for (var i = 0; i < 5; i++)
                {
                    sb.Append("".PadLeft(paddingCount, paddingChar));
                    sb.Append("|");
                }

                return;
            }

            sb.Append(clusterAnalysisStrings[ClusterAnalysisSampling.EntireDb].PadLeft(paddingCount, paddingChar));
            sb.Append("|");
            sb.Append(clusterAnalysisStrings[ClusterAnalysisSampling.EntireSkyline].PadLeft(paddingCount, paddingChar));
            sb.Append("|");
            sb.Append(clusterAnalysisStrings[ClusterAnalysisSampling.SampleSkyline].PadLeft(paddingCount, paddingChar));
            sb.Append("|");
            sb.Append(clusterAnalysisStrings[ClusterAnalysisSampling.BestRank].PadLeft(paddingCount, paddingChar));
            sb.Append("|");
            sb.Append(clusterAnalysisStrings[ClusterAnalysisSampling.SumRank].PadLeft(paddingCount, paddingChar));
            sb.Append("|");
        }

        private string FormatLineString(string strTitle, string strTrial, double dimension, double skyline, double timeTotal, double timeAlgo, double correlation, double cardinality)
        {
            return FormatLineString(' ', strTitle, strTrial, Math.Round(dimension, 2).ToString(CultureInfo.InvariantCulture), Math.Round(skyline, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeTotal, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeAlgo, 2).ToString(CultureInfo.InvariantCulture), Math.Round(correlation, 2).ToString(CultureInfo.InvariantCulture), ToLongString(Math.Round(cardinality, 2)));
        }

        private string FormatLineStringSample(string strTitle, string strTrial, double dimension, double skyline, double timeTotal, double timeAlgo, double minTime, double maxTime, double varianceTime, double stddeviationTime, double minSize, double maxSize, double varianceSize, double stddeviationSize, Dictionary<SkylineTypesSingleSampling, List<double>> setCoverageSampling, Dictionary<SkylineTypesSingleSampling, List<double>> representationErrorSampling, Dictionary<SkylineTypesSingleSampling, List<double>> representationErrorSumSampling, Dictionary<SkylineTypesSingleSampling, List<double>> dominatedObjectsCountSampling, Dictionary<SkylineTypesSingleSampling, List<double>> dominatedObjectsOfBestObjectSampling, Dictionary<ClusterAnalysisSampling, string> clusterAnalysisStrings, Dictionary<ClusterAnalysisSampling, string> clusterAnalysisMedianStrings, Dictionary<ClusterAnalysisSampling, string> clusterAnalysisTopBucketsStrings, Dictionary<ClusterAnalysisSampling, string> clusterAnalysisMedianTopBucketsStrings, double correlation, double cardinality)
        {
            return FormatLineStringSample(' ', strTitle, strTrial, Math.Round(dimension, 2).ToString(CultureInfo.InvariantCulture), Math.Round(skyline, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeTotal, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeAlgo, 2).ToString(CultureInfo.InvariantCulture), Math.Round(minTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(maxTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(varianceTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(stddeviationTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(minSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(maxSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(varianceSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(stddeviationSize, 2).ToString(CultureInfo.InvariantCulture), setCoverageSampling, representationErrorSampling, representationErrorSumSampling, dominatedObjectsCountSampling, dominatedObjectsOfBestObjectSampling, clusterAnalysisStrings, clusterAnalysisMedianStrings, clusterAnalysisTopBucketsStrings, clusterAnalysisMedianTopBucketsStrings, Math.Round(correlation, 2).ToString(CultureInfo.InvariantCulture), ToLongString(Math.Round(cardinality, 2)));
        }

        private string FormatLineStringSample(string strTitle, string strTrial, double dimension, double skyline, double timeTotal, double timeAlgo, double minTime, double maxTime, double varianceTime, double stddeviationTime, double minSize, double maxSize, double varianceSize, double stddeviationSize, string[] setCoverageSampling, string[] representationErrorSampling, string[] representationErrorSumSampling, string[] dominatedObjectsCountSampling, string[] dominatedObjectsOfBestObjectSampling, string[] clusterAnalysisStrings, string[] clusterAnalysisMedianStrings, string[] clusterAnalysisTopBucketsStrings, string[] clusterAnalysisMedianTopBucketsStrings, double correlation, double cardinality)
        {
            return FormatLineStringSample(' ', strTitle, strTrial, Math.Round(dimension, 2).ToString(CultureInfo.InvariantCulture), Math.Round(skyline, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeTotal, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeAlgo, 2).ToString(CultureInfo.InvariantCulture), Math.Round(minTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(maxTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(varianceTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(stddeviationTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(minSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(maxSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(varianceSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(stddeviationSize, 2).ToString(CultureInfo.InvariantCulture), setCoverageSampling, representationErrorSampling, representationErrorSumSampling, dominatedObjectsCountSampling, dominatedObjectsOfBestObjectSampling, clusterAnalysisStrings, clusterAnalysisMedianStrings, clusterAnalysisTopBucketsStrings, clusterAnalysisMedianTopBucketsStrings, Math.Round(correlation, 2).ToString(CultureInfo.InvariantCulture), ToLongString(Math.Round(cardinality, 2)));
        }

        
        /// <summary>
        /// Source: http://stackoverflow.com/questions/1546113/double-to-string-conversion-without-scientific-notation
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        private static string ToLongString(double input)
        {
            string str = input.ToString(CultureInfo.InvariantCulture).ToUpper();

            // if string representation was collapsed from scientific notation, just return it:
            if (!str.Contains("E")) return str;

            bool negativeNumber = false;

            if (str[0] == '-')
            {
                str = str.Remove(0, 1);
                negativeNumber = true;
            }

            string sep = Thread.CurrentThread.CurrentCulture.NumberFormat.NumberDecimalSeparator;
            char decSeparator = sep.ToCharArray()[0];

            string[] exponentParts = str.Split('E');
            string[] decimalParts = exponentParts[0].Split(decSeparator);

            // fix missing decimal point:
            if (decimalParts.Length == 1) decimalParts = new[] { exponentParts[0], "0" };

            int exponentValue = int.Parse(exponentParts[1]);

            string newNumber = decimalParts[0] + decimalParts[1];

            string result;

            if (exponentValue > 0)
            {
                result =
                    newNumber +
                    GetZeros(exponentValue - decimalParts[1].Length);
            }
            else // negative exponent
            {
                result =
                    "0" +
                    decSeparator +
                    GetZeros(exponentValue + decimalParts[0].Length) +
                    newNumber;

                result = result.TrimEnd('0');
            }

            if (negativeNumber)
                result = "-" + result;

            return result;
        }

        /// <summary>
        /// Source: http://stackoverflow.com/questions/1546113/double-to-string-conversion-without-scientific-notation
        /// </summary>
        /// <param name="zeroCount"></param>
        /// <returns></returns>
        private static string GetZeros(int zeroCount)
        {
            if (zeroCount < 0)
                zeroCount = Math.Abs(zeroCount);

            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < zeroCount; i++) sb.Append("0");

            return sb.ToString();
        }



        #endregion




        

    }
    

}
