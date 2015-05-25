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
    using System.Data.Common;
    using System.Globalization;
    using System.Numerics;
    using prefSQL.SQLParserTest;
    using prefSQL.SQLSkyline.SamplingSkyline;

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

        private const string Path = "E:\\Doc\\Studies\\PRJ_Thesis\\15 Performance\\Output\\";
        private int _trials = 5;                 //How many times each preferene query is executed  
        private int _randomDraws = 25;          //Only used for the shuffle set. How many random set will be generated
        static readonly Random Rnd = new Random();
        

        private enum ReportsSampling
        {
            TimeMin,TimeMax,TimeVar,TimeStdDev,SizeMin,SizeMax,SizeVar,SizeStdDev
        }

        private enum SetCoverageSampling
        {
            RandomAvg, RandomMin, RandomMax, RandomVar, RandomStdDev,
            SampleAvg, SampleMin, SampleMax, SampleVar, SampleStdDev,
            BestRankAvg, BestRankMin, BestRankMax, BestRankVar, BestRankStdDev,
            SumRankAvg, SumRankMin, SumRankMax, SumRankVar, SumRankStdDev
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
            DataTable dt = parser.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, "SELECT cars.id FROM cars SKYLINE OF cars.price LOW");

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
                    strSeparatorLine = FormatLineStringSample('-', "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "");
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
                        sb.AppendLine(FormatLineStringSample(' ', "preference set", "trial", "dimensions", "avg skyline size", "avg time total", "avg time algorithm", "min time", "max time", "variance time", "stddeviation time", "min size", "max size", "variance size", "stddeviation size", "avg sc random", "min sc random", "max sc random", "var sc random", "stddev sc random", "avg sc sample", "min sc sample", "max sc sample", "var sc sample", "stddev sc sample", "avg sc Best", "min sc Best", "max sc Best", "var sc Best", "stddev sc Best", "avg sc Sum", "min sc Sum", "max sc Sum", "var sc Sum", "stddev sc Sum", "ca entire db", "ca entire skyline", "ca sample skyline", "ca best rank", "ca sum rank", "sum correlation*", "product cardinality"));
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
                Dictionary<SetCoverageSampling, List<double>> setCoverageSampling;
                Dictionary<ClusterAnalysisSampling, List<List<double>>> clusterAnalysisSampling;
                InitSamplingDataStructures(out reportsSamplingLong, out reportsSamplingDouble, out setCoverageSampling, out clusterAnalysisSampling);

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
                    string strSQL = "SELECT cars.id FROM ";
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
                                        InitClusterAnalysisSamplingDataStructures(out clusterAnalysisSampling);

                                        DataTable entireSkylineDataTable =
                                            parser.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName,
                                                strSQL);

                                        List<long[]> entireDataTableSkylineValues = parser.SkylineType.Strategy.SkylineValues;

                                        int[] skylineAttributeColumns =
                                            SkylineSamplingHelper.GetSkylineAttributeColumns(entireSkylineDataTable);

                                        IReadOnlyDictionary<long, object[]> entireSkylineNormalized =
                                            prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(
                                                entireSkylineDataTable, 0);
                                        SkylineSamplingHelper.NormalizeColumns(entireSkylineNormalized,
                                            skylineAttributeColumns);

                                        IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                            aggregatedEntireSkylineBuckets =
                                                ClusterAnalysis.GetAggregatedBuckets(entireSkylineNormalized,
                                                    skylineAttributeColumns);

                                        IReadOnlyDictionary<long, object[]> entireDatabaseNormalized =
                                            GetEntireDatabaseNormalized(parser, strSQL, skylineAttributeColumns);

                                        IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                            aggregatedEntireDatabaseBuckets =
                                                ClusterAnalysis.GetAggregatedBuckets(entireDatabaseNormalized,
                                                    skylineAttributeColumns);

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

                                        List<HashSet<HashSet<int>>> producedSubspaces = ProduceSubspaces(preferences);

                                        var subspaceObjects = new List<long>();
                                        var subspaceTime = new List<long>();
                                        var subspaceTimeElapsed = new List<long>();
                                        var setCoverageSecondRandom = new List<double>();
                                        var setCoverageSample = new List<double>();
                                        var setCoverageBestRank = new List<double>();
                                        var setCoverageSumRank = new List<double>();

                                        foreach (HashSet<HashSet<int>> subspace in producedSubspaces)
                                        {
                                            sw.Restart();
                                            var subspacesProducer = new FixedSamplingSkylineSubspacesProducer(subspace);
                                            var utility = new SamplingSkylineUtility(subspacesProducer);
                                            var skylineSample = new SamplingSkyline(utility)
                                            {
                                                SubspacesCount = prefSqlModel.SkylineSampleCount,
                                                SubspaceDimension = prefSqlModel.SkylineSampleDimension
                                            };

                                            DataTable sampleSkylineDataTable = skylineSample.GetSkylineTable(strQuery,
                                                operators, parser.SkylineType);

                                            sw.Stop();

                                            subspaceObjects.Add(sampleSkylineDataTable.Rows.Count);
                                            subspaceTime.Add(skylineSample.TimeMilliseconds);
                                            subspaceTimeElapsed.Add(sw.ElapsedMilliseconds);

                                            IReadOnlyDictionary<long, object[]> sampleSkylineNormalized =
                                                prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(
                                                    sampleSkylineDataTable, 0);
                                            SkylineSamplingHelper.NormalizeColumns(sampleSkylineNormalized,
                                                skylineAttributeColumns);

                                            IReadOnlyDictionary<long, object[]> baseRandomSampleNormalized =
                                                SkylineSamplingHelper.GetRandomSample(entireSkylineNormalized,
                                                    sampleSkylineDataTable.Rows.Count);
                                            IReadOnlyDictionary<long, object[]> secondRandomSampleNormalized =
                                                SkylineSamplingHelper.GetRandomSample(entireSkylineNormalized,
                                                    sampleSkylineDataTable.Rows.Count);


                                            IReadOnlyDictionary<long, object[]> entireSkylineDataTableBestRankNormalized =
                                                GetEntireSkylineDataTableRankNormalized(entireSkylineDataTable.Copy(), entireDataTableSkylineValues, skylineAttributeColumns, sampleSkylineDataTable.Rows.Count, 1);

                                            IReadOnlyDictionary<long, object[]> entireSkylineDataTableSumRankNormalized =
                                                GetEntireSkylineDataTableRankNormalized(entireSkylineDataTable.Copy(), entireDataTableSkylineValues, skylineAttributeColumns, sampleSkylineDataTable.Rows.Count, 2);

                                            double setCoverageCoveredBySecondRandomSample = SetCoverage.GetCoverage(
                                                baseRandomSampleNormalized,
                                                secondRandomSampleNormalized, skylineAttributeColumns) * 100.0;
                                            double setCoverageCoveredBySkylineSample = SetCoverage.GetCoverage(
                                                baseRandomSampleNormalized,
                                                sampleSkylineNormalized, skylineAttributeColumns) * 100.0;
                                            double setCoverageCoveredByEntireBestRank =
                                                SetCoverage.GetCoverage(baseRandomSampleNormalized,
                                                    entireSkylineDataTableBestRankNormalized, skylineAttributeColumns) * 100.0;
                                            double setCoverageCoveredByEntireSumRank =
                                                SetCoverage.GetCoverage(baseRandomSampleNormalized,
                                                    entireSkylineDataTableSumRankNormalized, skylineAttributeColumns) * 100.0;

                                            setCoverageSecondRandom.Add(setCoverageCoveredBySecondRandomSample);
                                            setCoverageSample.Add(setCoverageCoveredBySkylineSample);
                                            setCoverageBestRank.Add(setCoverageCoveredByEntireBestRank);
                                            setCoverageSumRank.Add(setCoverageCoveredByEntireSumRank);

                                            IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                                aggregatedSampleBuckets =
                                                    ClusterAnalysis.GetAggregatedBuckets(sampleSkylineNormalized,
                                                        skylineAttributeColumns);

                                            IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                                aggregatedBestRankBuckets =
                                                    ClusterAnalysis.GetAggregatedBuckets(entireSkylineDataTableBestRankNormalized,
                                                        skylineAttributeColumns);
                                            IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                                aggregatedSumRankBuckets =
                                                    ClusterAnalysis.GetAggregatedBuckets(entireSkylineDataTableSumRankNormalized,
                                                        skylineAttributeColumns);

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

                                                double bestRankPercent = (double)bestRank /
                                                                              entireSkylineDataTableBestRankNormalized.Count;
                                                double sumRankPercent = (double)sumRank /
                                                                              entireSkylineDataTableSumRankNormalized.Count;
                                                caEntireDbNew.Add(entireDbPercent);
                                                caEntireSkylineNew.Add(entireSkylinePercent);
                                                caSampleSkylineNew.Add(sampleSkylinePercent);
                                                caBestRankNew.Add(bestRankPercent);
                                                caSumRankNew.Add(sumRankPercent);
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
                                        }

                                        Dictionary<ClusterAnalysisSampling, string> clusterAnalysisStrings =
                                            GetClusterAnalysisStrings(skylineAttributeColumns, clusterAnalysisSampling);

                                        var time = (long) (subspaceTime.Average() + .5);
                                        var objects = (long) (subspaceObjects.Average() + .5);
                                        var elapsed = (long) (subspaceTimeElapsed.Average() + .5);

                                        reportDimensions.Add(preferences.Count);
                                        reportSkylineSize.Add(objects);
                                        reportTimeTotal.Add(elapsed);
                                        reportTimeAlgorithm.Add(time);
                                        reportCorrelation.Add(correlation);
                                        reportCardinality.Add(cardinality);

                                        AddToReportsSampling(reportsSamplingLong, subspaceObjects, subspaceTime,
                                            reportsSamplingDouble);
                                        AddToSetCoverageSampling(setCoverageSampling, setCoverageSecondRandom,
                                            setCoverageSample, setCoverageBestRank, setCoverageSumRank);

                                        //trial|dimensions|skyline size|time total|time algorithm
                                        string strTrial = iTrial + 1 + " / " + _trials;
                                        string strPreferenceSet = iPreferenceIndex + 1 + " / " + listPreferences.Count;
                                        Console.WriteLine(strPreferenceSet);

                                        var mathematic = new Mathematic();

                                        string strLine = FormatLineStringSample(strPreferenceSet, strTrial, preferences.Count, objects,
                                            elapsed, time, subspaceTime.Min(), subspaceTime.Max(),
                                            mathematic.GetVariance(subspaceTime),
                                            mathematic.GetStdDeviation(subspaceTime), subspaceObjects.Min(),
                                            subspaceObjects.Max(), mathematic.GetVariance(subspaceObjects),
                                            mathematic.GetStdDeviation(subspaceObjects),
                                            setCoverageSecondRandom.Average(), setCoverageSecondRandom.Min(),
                                            setCoverageSecondRandom.Max(),
                                            mathematic.GetVariance(setCoverageSecondRandom),
                                            mathematic.GetStdDeviation(setCoverageSecondRandom),
                                            setCoverageSample.Average(), setCoverageSample.Min(),
                                            setCoverageSample.Max(), mathematic.GetVariance(setCoverageSample),
                                            mathematic.GetStdDeviation(setCoverageSample), setCoverageBestRank.Average(),
                                            setCoverageBestRank.Min(),
                                            setCoverageBestRank.Max(),
                                            mathematic.GetVariance(setCoverageBestRank),
                                            mathematic.GetStdDeviation(setCoverageBestRank),
                                            setCoverageSumRank.Average(), setCoverageSumRank.Min(),
                                            setCoverageSumRank.Max(), mathematic.GetVariance(setCoverageSumRank),
                                            mathematic.GetStdDeviation(setCoverageSumRank),
                                            clusterAnalysisStrings[ClusterAnalysisSampling.EntireDb],
                                            clusterAnalysisStrings[ClusterAnalysisSampling.EntireSkyline],
                                            clusterAnalysisStrings[ClusterAnalysisSampling.SampleSkyline], clusterAnalysisStrings[ClusterAnalysisSampling.BestRank], clusterAnalysisStrings[ClusterAnalysisSampling.SumRank], correlation,
                                            cardinality);

                                        Debug.WriteLine(strLine);
                                        sb.AppendLine(strLine);
                                    } else
                                {
                              
                                    

                                sw.Start();
                                if (UseCLR)
                                {
                                    string strSP = parser.ParsePreferenceSQL(strSQL);
                                    SqlDataAdapter dap = new SqlDataAdapter(strSP, cnnSQL);
                                    dt.Clear(); //clear datatable
                                    dap.Fill(dt);
                                }
                                else
                                {
                                    parser.Cardinality = (long)cardinality;
                                    dt = parser.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, strSQL);
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


                                string strLine = FormatLineString(strPreferenceSet, strTrial, preferences.Count, dt.Rows.Count, sw.ElapsedMilliseconds, timeAlgorithm, correlation, cardinality);


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
                        AddSummarySample(sb, strSeparatorLine, reportDimensions, reportSkylineSize, reportTimeTotal, reportTimeAlgorithm, reportsSamplingLong, reportsSamplingDouble, setCoverageSampling, reportCorrelation, reportCardinality);
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

        private static IReadOnlyDictionary<long, object[]> GetEntireSkylineDataTableRankNormalized(DataTable entireSkyline, List<long[]> skylineValues, int[] skylineAttributeColumns, int numberOfRecords, int sortType)
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

              IReadOnlyDictionary<long, object[]> sortedDataTableNormalized =
                prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(sortedDataTable, 0);
            SkylineSamplingHelper.NormalizeColumns(sortedDataTableNormalized, skylineAttributeColumns);
            return sortedDataTableNormalized;
        }

        private List<HashSet<HashSet<int>>> ProduceSubspaces(ArrayList preferences)
        {
            var randomSubspacesesProducer = new RandomSamplingSkylineSubspacesProducer
            {
                AllPreferencesCount = preferences.Count,
                SubspacesCount = SamplingSubspacesCount,
                SubspaceDimension = SamplingSubspaceDimension
            };

            var producedSubspaces = new List<HashSet<HashSet<int>>>();
            for (var ii = 0; ii < SamplingSamplesCount; ii++)
            {
                producedSubspaces.Add(randomSubspacesesProducer.GetSubspaces());
            }
            return producedSubspaces;
        }

        private static IReadOnlyDictionary<long, object[]> GetEntireDatabaseNormalized(SQLCommon parser, string strSQL, int[] skylineAttributeColumns)
        {
            DbProviderFactory factory = DbProviderFactories.GetFactory(Helper.ProviderName);

            // use the factory object to create Data access objects.
            DbConnection connection = factory.CreateConnection();
            // will return the connection object (i.e. SqlConnection ...)
            connection.ConnectionString = Helper.ConnectionString;

            var dtEntire = new DataTable();

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

        private static void AddToSetCoverageSampling(Dictionary<SetCoverageSampling, List<double>> setCoverageSampling, List<double> setCoverageSecondRandom,
             List<double> setCoverageSample, List<double> setCoverageBestRank, List<double> setCoverageSumRank)
        {
            var mathematic = new Mathematic();
            setCoverageSampling[SetCoverageSampling.RandomAvg].Add(setCoverageSecondRandom.Average());
            setCoverageSampling[SetCoverageSampling.RandomMin].Add(setCoverageSecondRandom.Min());
            setCoverageSampling[SetCoverageSampling.RandomMax].Add(setCoverageSecondRandom.Max());
            setCoverageSampling[SetCoverageSampling.RandomVar].Add(mathematic.GetVariance(setCoverageSecondRandom));
            setCoverageSampling[SetCoverageSampling.RandomStdDev].Add(mathematic.GetStdDeviation(setCoverageSecondRandom));
            setCoverageSampling[SetCoverageSampling.SampleAvg].Add(setCoverageSample.Average());
            setCoverageSampling[SetCoverageSampling.SampleMin].Add(setCoverageSample.Min());
            setCoverageSampling[SetCoverageSampling.SampleMax].Add(setCoverageSample.Max());
            setCoverageSampling[SetCoverageSampling.SampleVar].Add(mathematic.GetVariance(setCoverageSample));
            setCoverageSampling[SetCoverageSampling.SampleStdDev].Add(mathematic.GetStdDeviation(setCoverageSample));

            setCoverageSampling[SetCoverageSampling.BestRankAvg].Add(setCoverageBestRank.Average());
            setCoverageSampling[SetCoverageSampling.BestRankMin].Add(setCoverageBestRank.Min());
            setCoverageSampling[SetCoverageSampling.BestRankMax].Add(setCoverageBestRank.Max());
            setCoverageSampling[SetCoverageSampling.BestRankVar].Add(mathematic.GetVariance(setCoverageBestRank));
            setCoverageSampling[SetCoverageSampling.BestRankStdDev].Add(mathematic.GetStdDeviation(setCoverageBestRank));
            setCoverageSampling[SetCoverageSampling.SumRankAvg].Add(setCoverageSumRank.Average());
            setCoverageSampling[SetCoverageSampling.SumRankMin].Add(setCoverageSumRank.Min());
            setCoverageSampling[SetCoverageSampling.SumRankMax].Add(setCoverageSumRank.Max());
            setCoverageSampling[SetCoverageSampling.SumRankVar].Add(mathematic.GetVariance(setCoverageSumRank));
            setCoverageSampling[SetCoverageSampling.SumRankStdDev].Add(mathematic.GetStdDeviation(setCoverageSumRank));
        }

        private static void AddToReportsSampling(Dictionary<ReportsSampling, List<long>> reportsSamplingLong, List<long> subspaceObjects, List<long> subspaceTime,
            Dictionary<ReportsSampling, List<double>> reportsSamplingDouble)
        {
            var mathematic = new Mathematic();
            reportsSamplingLong[ReportsSampling.SizeMin].Add(subspaceObjects.Min());
            reportsSamplingLong[ReportsSampling.TimeMin].Add(subspaceTime.Min());
            reportsSamplingLong[ReportsSampling.SizeMax].Add(subspaceObjects.Max());
            reportsSamplingLong[ReportsSampling.TimeMax].Add(subspaceTime.Max());
            reportsSamplingDouble[ReportsSampling.SizeVar].Add(mathematic.GetVariance(subspaceObjects));
            reportsSamplingDouble[ReportsSampling.TimeVar].Add(mathematic.GetVariance(subspaceTime));
            reportsSamplingDouble[ReportsSampling.SizeStdDev].Add(mathematic.GetStdDeviation(subspaceObjects));
            reportsSamplingDouble[ReportsSampling.TimeStdDev].Add(mathematic.GetStdDeviation(subspaceTime));
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

        private static void InitSamplingDataStructures(out Dictionary<ReportsSampling, List<long>> reportsSamplingLong, out Dictionary<ReportsSampling, List<double>> reportsSamplingDouble,
            out Dictionary<SetCoverageSampling, List<double>> setCoverageSampling, out Dictionary<ClusterAnalysisSampling, List<List<double>>> clusterAnalysisSampling)
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

            setCoverageSampling = new Dictionary<SetCoverageSampling, List<double>>()
            {
                {SetCoverageSampling.RandomAvg, new List<double>()},
                {SetCoverageSampling.RandomMin, new List<double>()},
                {SetCoverageSampling.RandomMax, new List<double>()},
                {SetCoverageSampling.RandomVar, new List<double>()},
                {SetCoverageSampling.RandomStdDev, new List<double>()},
                {SetCoverageSampling.SampleAvg, new List<double>()},
                {SetCoverageSampling.SampleMin, new List<double>()},
                {SetCoverageSampling.SampleMax, new List<double>()},
                {SetCoverageSampling.SampleVar, new List<double>()},
                {SetCoverageSampling.SampleStdDev, new List<double>()},
                {SetCoverageSampling.BestRankAvg, new List<double>()},
                {SetCoverageSampling.BestRankMin, new List<double>()},
                {SetCoverageSampling.BestRankMax, new List<double>()},
                {SetCoverageSampling.BestRankVar, new List<double>()},
                {SetCoverageSampling.BestRankStdDev, new List<double>()},
                {SetCoverageSampling.SumRankAvg, new List<double>()},
                {SetCoverageSampling.SumRankMin, new List<double>()},
                {SetCoverageSampling.SumRankMax, new List<double>()},
                {SetCoverageSampling.SumRankVar, new List<double>()},
                {SetCoverageSampling.SumRankStdDev, new List<double>()}
            };

            InitClusterAnalysisSamplingDataStructures(out clusterAnalysisSampling);
        }

        private DataTable GetSQLFromPreferences(ArrayList preferences, bool cardinality)
        {
            SQLCommon common = new SQLCommon();
            string strPrefSQL = "SELECT cars.id FROM ";
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
            Mathematic mathematic = new Mathematic();
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

                        double correlation = mathematic.GetPearson(colA, colB);
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
            List<long> reportSkylineSize, List<long> reportTimeTotal, List<long> reportTimeAlgorithm, IDictionary<ReportsSampling, List<long>> rsl, IDictionary<ReportsSampling, List<double>> rsd, Dictionary<SetCoverageSampling, List<double>> scs, List<double> reportCorrelation, List<double> reportCardinality)
        {
            //Separator Line
            Debug.WriteLine(strSeparatorLine);
            sb.AppendLine(strSeparatorLine);

            var mathematic = new Mathematic();
            string strAverage = FormatLineStringSample("average", "", reportDimensions.Average(), reportSkylineSize.Average(), reportTimeTotal.Average(), reportTimeAlgorithm.Average(), rsl[ReportsSampling.TimeMin].Average(), rsl[ReportsSampling.TimeMax].Average(), rsd[ReportsSampling.TimeVar].Average(), rsd[ReportsSampling.TimeStdDev].Average(), rsl[ReportsSampling.SizeMin].Average(), rsl[ReportsSampling.SizeMax].Average(), rsd[ReportsSampling.SizeVar].Average(), rsd[ReportsSampling.SizeStdDev].Average(), scs[SetCoverageSampling.RandomAvg].Average(), scs[SetCoverageSampling.RandomMin].Average(), scs[SetCoverageSampling.RandomMax].Average(), scs[SetCoverageSampling.RandomVar].Average(), scs[SetCoverageSampling.RandomStdDev].Average(), scs[SetCoverageSampling.SampleAvg].Average(), scs[SetCoverageSampling.SampleMin].Average(), scs[SetCoverageSampling.SampleMax].Average(), scs[SetCoverageSampling.SampleVar].Average(), scs[SetCoverageSampling.SampleStdDev].Average(), scs[SetCoverageSampling.BestRankAvg].Average(), scs[SetCoverageSampling.BestRankMin].Average(), scs[SetCoverageSampling.BestRankMax].Average(), scs[SetCoverageSampling.BestRankVar].Average(), scs[SetCoverageSampling.BestRankStdDev].Average(), scs[SetCoverageSampling.SumRankAvg].Average(), scs[SetCoverageSampling.SumRankMin].Average(), scs[SetCoverageSampling.SumRankMax].Average(), scs[SetCoverageSampling.SumRankVar].Average(), scs[SetCoverageSampling.SumRankStdDev].Average(), "", "", "" ,"","",reportCorrelation.Average(), reportCardinality.Average());
            string strMin = FormatLineStringSample("minimum", "", reportDimensions.Min(), reportSkylineSize.Min(), reportTimeTotal.Min(), reportTimeAlgorithm.Min(), rsl[ReportsSampling.TimeMin].Min(), rsl[ReportsSampling.TimeMax].Min(), rsd[ReportsSampling.TimeVar].Min(), rsd[ReportsSampling.TimeStdDev].Min(), rsl[ReportsSampling.SizeMin].Min(), rsl[ReportsSampling.SizeMax].Min(), rsd[ReportsSampling.SizeVar].Min(), rsd[ReportsSampling.SizeStdDev].Min(), scs[SetCoverageSampling.RandomAvg].Min(), scs[SetCoverageSampling.RandomMin].Min(), scs[SetCoverageSampling.RandomMax].Min(), scs[SetCoverageSampling.RandomVar].Min(), scs[SetCoverageSampling.RandomStdDev].Min(), scs[SetCoverageSampling.SampleAvg].Min(), scs[SetCoverageSampling.SampleMin].Min(), scs[SetCoverageSampling.SampleMax].Min(), scs[SetCoverageSampling.SampleVar].Min(), scs[SetCoverageSampling.SampleStdDev].Min(), scs[SetCoverageSampling.BestRankAvg].Min(), scs[SetCoverageSampling.BestRankMin].Min(), scs[SetCoverageSampling.BestRankMax].Min(), scs[SetCoverageSampling.BestRankVar].Min(), scs[SetCoverageSampling.BestRankStdDev].Min(), scs[SetCoverageSampling.SumRankAvg].Min(), scs[SetCoverageSampling.SumRankMin].Min(), scs[SetCoverageSampling.SumRankMax].Min(), scs[SetCoverageSampling.SumRankVar].Min(), scs[SetCoverageSampling.SumRankStdDev].Min(), "", "", "", "", "", reportCorrelation.Min(), reportCardinality.Min());
            string strMax = FormatLineStringSample("maximum", "", reportDimensions.Max(), reportSkylineSize.Max(), reportTimeTotal.Max(), reportTimeAlgorithm.Max(), rsl[ReportsSampling.TimeMin].Max(), rsl[ReportsSampling.TimeMax].Max(), rsd[ReportsSampling.TimeVar].Max(), rsd[ReportsSampling.TimeStdDev].Max(), rsl[ReportsSampling.SizeMin].Max(), rsl[ReportsSampling.SizeMax].Max(), rsd[ReportsSampling.SizeVar].Max(), rsd[ReportsSampling.SizeStdDev].Max(), scs[SetCoverageSampling.RandomAvg].Max(), scs[SetCoverageSampling.RandomMin].Max(), scs[SetCoverageSampling.RandomMax].Max(), scs[SetCoverageSampling.RandomVar].Max(), scs[SetCoverageSampling.RandomStdDev].Max(), scs[SetCoverageSampling.SampleAvg].Max(), scs[SetCoverageSampling.SampleMin].Max(), scs[SetCoverageSampling.SampleMax].Max(), scs[SetCoverageSampling.SampleVar].Max(), scs[SetCoverageSampling.SampleStdDev].Max(), scs[SetCoverageSampling.BestRankAvg].Max(), scs[SetCoverageSampling.BestRankMin].Max(), scs[SetCoverageSampling.BestRankMax].Max(), scs[SetCoverageSampling.BestRankVar].Max(), scs[SetCoverageSampling.BestRankStdDev].Max(), scs[SetCoverageSampling.SumRankAvg].Max(), scs[SetCoverageSampling.SumRankMin].Max(), scs[SetCoverageSampling.SumRankMax].Max(), scs[SetCoverageSampling.SumRankVar].Max(), scs[SetCoverageSampling.SumRankStdDev].Max(), "", "", "", "", "", reportCorrelation.Max(), reportCardinality.Max());
            string strVar = FormatLineStringSample("variance", "", mathematic.GetVariance(reportDimensions), mathematic.GetVariance(reportSkylineSize), mathematic.GetVariance(reportTimeTotal), mathematic.GetVariance(reportTimeAlgorithm), mathematic.GetVariance(rsl[ReportsSampling.TimeMin]), mathematic.GetVariance(rsl[ReportsSampling.TimeMax]), mathematic.GetVariance(rsd[ReportsSampling.TimeVar]), mathematic.GetVariance(rsd[ReportsSampling.TimeStdDev]), mathematic.GetVariance(rsl[ReportsSampling.SizeMin]), mathematic.GetVariance(rsl[ReportsSampling.SizeMax]), mathematic.GetVariance(rsd[ReportsSampling.SizeVar]), mathematic.GetVariance(rsd[ReportsSampling.SizeStdDev]), mathematic.GetVariance(scs[SetCoverageSampling.RandomAvg]), mathematic.GetVariance(scs[SetCoverageSampling.RandomMin]), mathematic.GetVariance(scs[SetCoverageSampling.RandomMax]), mathematic.GetVariance(scs[SetCoverageSampling.RandomVar]), mathematic.GetVariance(scs[SetCoverageSampling.RandomStdDev]), mathematic.GetVariance(scs[SetCoverageSampling.SampleAvg]), mathematic.GetVariance(scs[SetCoverageSampling.SampleMin]), mathematic.GetVariance(scs[SetCoverageSampling.SampleMax]), mathematic.GetVariance(scs[SetCoverageSampling.SampleVar]), mathematic.GetVariance(scs[SetCoverageSampling.SampleStdDev]), mathematic.GetVariance(scs[SetCoverageSampling.BestRankAvg]), mathematic.GetVariance(scs[SetCoverageSampling.BestRankMin]), mathematic.GetVariance(scs[SetCoverageSampling.BestRankMax]), mathematic.GetVariance(scs[SetCoverageSampling.BestRankVar]), mathematic.GetVariance(scs[SetCoverageSampling.BestRankStdDev]), mathematic.GetVariance(scs[SetCoverageSampling.SumRankAvg]), mathematic.GetVariance(scs[SetCoverageSampling.SumRankMin]), mathematic.GetVariance(scs[SetCoverageSampling.SumRankMax]), mathematic.GetVariance(scs[SetCoverageSampling.SumRankVar]), mathematic.GetVariance(scs[SetCoverageSampling.SumRankStdDev]), "", "", "", "", "", mathematic.GetVariance(reportCorrelation), mathematic.GetVariance(reportCardinality));
            string strStd = FormatLineStringSample("stddeviation", "", mathematic.GetStdDeviation(reportDimensions), mathematic.GetStdDeviation(reportSkylineSize), mathematic.GetStdDeviation(reportTimeTotal), mathematic.GetStdDeviation(reportTimeAlgorithm), mathematic.GetStdDeviation(rsl[ReportsSampling.TimeMin]), mathematic.GetStdDeviation(rsl[ReportsSampling.TimeMax]), mathematic.GetStdDeviation(rsd[ReportsSampling.TimeVar]), mathematic.GetStdDeviation(rsd[ReportsSampling.TimeStdDev]), mathematic.GetStdDeviation(rsl[ReportsSampling.SizeMin]), mathematic.GetStdDeviation(rsl[ReportsSampling.SizeMax]), mathematic.GetStdDeviation(rsd[ReportsSampling.SizeVar]), mathematic.GetStdDeviation(rsd[ReportsSampling.SizeStdDev]), mathematic.GetStdDeviation(scs[SetCoverageSampling.RandomAvg]), mathematic.GetStdDeviation(scs[SetCoverageSampling.RandomMin]), mathematic.GetStdDeviation(scs[SetCoverageSampling.RandomMax]), mathematic.GetStdDeviation(scs[SetCoverageSampling.RandomVar]), mathematic.GetStdDeviation(scs[SetCoverageSampling.RandomStdDev]), mathematic.GetStdDeviation(scs[SetCoverageSampling.SampleAvg]), mathematic.GetStdDeviation(scs[SetCoverageSampling.SampleMin]), mathematic.GetStdDeviation(scs[SetCoverageSampling.SampleMax]), mathematic.GetStdDeviation(scs[SetCoverageSampling.SampleVar]), mathematic.GetStdDeviation(scs[SetCoverageSampling.SampleStdDev]), mathematic.GetStdDeviation(scs[SetCoverageSampling.BestRankAvg]), mathematic.GetStdDeviation(scs[SetCoverageSampling.BestRankMin]), mathematic.GetStdDeviation(scs[SetCoverageSampling.BestRankMax]), mathematic.GetStdDeviation(scs[SetCoverageSampling.BestRankVar]), mathematic.GetStdDeviation(scs[SetCoverageSampling.BestRankStdDev]), mathematic.GetStdDeviation(scs[SetCoverageSampling.SumRankAvg]), mathematic.GetStdDeviation(scs[SetCoverageSampling.SumRankMin]), mathematic.GetStdDeviation(scs[SetCoverageSampling.SumRankMax]), mathematic.GetStdDeviation(scs[SetCoverageSampling.SumRankVar]), mathematic.GetStdDeviation(scs[SetCoverageSampling.SumRankStdDev]), "", "", "", "", "", mathematic.GetStdDeviation(reportCorrelation), mathematic.GetStdDeviation(reportCardinality));

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

        private void AddSummary(StringBuilder sb, String strSeparatorLine, List<long> reportDimensions, List<long> reportSkylineSize, List<long> reportTimeTotal, List<long> reportTimeAlgorithm, List<double> reportCorrelation, List<double> reportCardinality)
        {
            //Separator Line
            Debug.WriteLine(strSeparatorLine);
            sb.AppendLine(strSeparatorLine);

            Mathematic mathematic = new Mathematic();
            string strAverage = FormatLineString("average", "", reportDimensions.Average(), reportSkylineSize.Average(), reportTimeTotal.Average(), reportTimeAlgorithm.Average(), reportCorrelation.Average(), reportCardinality.Average());
            string strMin = FormatLineString("minimum", "", reportDimensions.Min(), reportSkylineSize.Min(), reportTimeTotal.Min(), reportTimeAlgorithm.Min(), reportCorrelation.Min(), reportCardinality.Min());
            string strMax = FormatLineString("maximum", "", reportDimensions.Max(), reportSkylineSize.Max(), reportTimeTotal.Max(), reportTimeAlgorithm.Max(), reportCorrelation.Max(), reportCardinality.Max());
            string strVar = FormatLineString("variance", "", mathematic.GetVariance(reportDimensions), mathematic.GetVariance(reportSkylineSize), mathematic.GetVariance(reportTimeTotal), mathematic.GetVariance(reportTimeAlgorithm), mathematic.GetVariance(reportCorrelation), mathematic.GetVariance(reportCardinality));
            string strStd = FormatLineString("stddeviation", "", mathematic.GetStdDeviation(reportDimensions), mathematic.GetStdDeviation(reportSkylineSize), mathematic.GetStdDeviation(reportTimeTotal), mathematic.GetStdDeviation(reportTimeAlgorithm), mathematic.GetStdDeviation(reportCorrelation), mathematic.GetStdDeviation(reportCardinality));
            string strSamplevar = FormatLineString("sample variance", "", mathematic.GetSampleVariance(reportDimensions), mathematic.GetSampleVariance(reportSkylineSize), mathematic.GetSampleVariance(reportTimeTotal), mathematic.GetSampleVariance(reportTimeAlgorithm), mathematic.GetSampleVariance(reportCorrelation), mathematic.GetSampleVariance(reportCardinality));
            string strSampleStd = FormatLineString("sample stddeviation", "", mathematic.GetSampleStdDeviation(reportDimensions), mathematic.GetSampleStdDeviation(reportSkylineSize), mathematic.GetSampleStdDeviation(reportTimeTotal), mathematic.GetSampleStdDeviation(reportTimeAlgorithm), mathematic.GetSampleStdDeviation(reportCorrelation), mathematic.GetSampleStdDeviation(reportCardinality));

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

        private string FormatLineStringSample(char paddingChar, string strTitle, string strTrial, string strDimension, string strSkyline, string strTimeTotal, string strTimeAlgo, string minTime, string maxTime, string varianceTime, string sedDevTime, string minSize, string maxSize, string varianceSize, string stdDevSize, string scRandomAvg, string scRandomMin, string scRandomMax, string scRandomVar, string scRandomStdDev, string scSampleAvg, string scSampleMin, string scSampleMax, string scSampleVar, string scSampleStdDev, string scBestAvg, string scBestMin, string scBestMax, string scBestVar, string scBestStdDev, string scSumAvg, string scSumMin, string scSumMax, string scSumVar, string scSumStdDev, string caEntireDb, string caEntireSkyline, string caSampleSkyline, string caBestRank, string caSumRank, string strCorrelation, string strCardinality)
        {
            //average line
            //trial|dimensions|skyline size|time total|time algorithm|correlation|
            string[] line = new string[42];
            line[0] = strTitle.PadLeft(19, paddingChar);
            line[1] = strTrial.PadLeft(11, paddingChar);
            line[2] = strDimension.PadLeft(10, paddingChar);
            line[3] = strSkyline.PadLeft(20, paddingChar);
            line[4] = strTimeTotal.PadLeft(20, paddingChar);
            line[5] = strTimeAlgo.PadLeft(20, paddingChar);
            line[6] = minTime.PadLeft(20, paddingChar);
            line[7] = maxTime.PadLeft(20, paddingChar);
            line[8] = varianceTime.PadLeft(20, paddingChar);
            line[9] = sedDevTime.PadLeft(20, paddingChar);
            line[10] = minSize.PadLeft(20, paddingChar);
            line[11] = maxSize.PadLeft(20, paddingChar);
            line[12] = varianceSize.PadLeft(20, paddingChar);
            line[13] = stdDevSize.PadLeft(20, paddingChar);
            line[14] = scRandomAvg.PadLeft(20, paddingChar);
            line[15] = scRandomMin.PadLeft(20, paddingChar);
            line[16] = scRandomMax.PadLeft(20, paddingChar);
            line[17] = scRandomVar.PadLeft(20, paddingChar);
            line[18] = scRandomStdDev.PadLeft(20, paddingChar);
            line[19] = scSampleAvg.PadLeft(20, paddingChar);
            line[20] = scSampleMin.PadLeft(20, paddingChar);
            line[21] = scSampleMax.PadLeft(20, paddingChar);
            line[22] = scSampleVar.PadLeft(20, paddingChar);
            line[23] = scSampleStdDev.PadLeft(20, paddingChar);
            line[24] = scBestAvg.PadLeft(20, paddingChar);
            line[25] = scBestMin.PadLeft(20, paddingChar);
            line[26] = scBestMax.PadLeft(20, paddingChar);
            line[27] = scBestVar.PadLeft(20, paddingChar);
            line[28] = scBestStdDev.PadLeft(20, paddingChar);
            line[29] = scSumAvg.PadLeft(20, paddingChar);
            line[30] = scSumMin.PadLeft(20, paddingChar);
            line[31] = scSumMax.PadLeft(20, paddingChar);
            line[32] = scSumVar.PadLeft(20, paddingChar);
            line[33] = scSumStdDev.PadLeft(20, paddingChar);
            line[34] = caEntireDb.PadLeft(130, paddingChar);
            line[35] = caEntireSkyline.PadLeft(130, paddingChar);
            line[36] = caSampleSkyline.PadLeft(130, paddingChar);
            line[37] = caBestRank.PadLeft(130, paddingChar);
            line[38] = caSumRank.PadLeft(130, paddingChar);
            line[39] = strCorrelation.PadLeft(20, paddingChar);
            line[40] = strCardinality.PadLeft(25, paddingChar);
            line[41] = "";
            return string.Join("|", line);
        }

        private string FormatLineString(string strTitle, string strTrial, double dimension, double skyline, double timeTotal, double timeAlgo, double correlation, double cardinality)
        {
            return FormatLineString(' ', strTitle, strTrial, Math.Round(dimension, 2).ToString(CultureInfo.InvariantCulture), Math.Round(skyline, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeTotal, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeAlgo, 2).ToString(CultureInfo.InvariantCulture), Math.Round(correlation, 2).ToString(CultureInfo.InvariantCulture), ToLongString(Math.Round(cardinality, 2)));
        }

        private string FormatLineStringSample(string strTitle, string strTrial, double dimension, double skyline, double timeTotal, double timeAlgo, double minTime, double maxTime, double varianceTime, double stddeviationTime, double minSize, double maxSize, double varianceSize, double stddeviationSize, double scRandomAvg, double scRandomMin, double scRandomMax, double scRandomVar, double scRandomStdDev, double scSampleAvg, double scSampleMin, double scSampleMax, double scSampleVar, double scSampleStdDev, double scBestAvg, double scBestMin, double scBestMax, double scBestVar, double scBestStdDev, double scSumAvg, double scSumMin, double scSumMax, double scSumVar, double scSumStdDev, string caEntireDb, string caEntireSkyline, string caSampleSkyline, string caBestRank, string caSumRank, double correlation, double cardinality)
        {
            return FormatLineStringSample(' ', strTitle, strTrial, Math.Round(dimension, 2).ToString(CultureInfo.InvariantCulture), Math.Round(skyline, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeTotal, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeAlgo, 2).ToString(CultureInfo.InvariantCulture), Math.Round(minTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(maxTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(varianceTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(stddeviationTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(minSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(maxSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(varianceSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(stddeviationSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scRandomAvg, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scRandomMin, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scRandomMax, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scRandomVar, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scRandomStdDev, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scSampleAvg, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scSampleMin, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scSampleMax, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scSampleVar, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scSampleStdDev, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scBestAvg, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scBestMin, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scBestMax, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scBestVar, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scBestStdDev, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scSumAvg, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scSumMin, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scSumMax, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scSumVar, 2).ToString(CultureInfo.InvariantCulture), Math.Round(scSumStdDev, 2).ToString(CultureInfo.InvariantCulture), caEntireDb, caEntireSkyline, caSampleSkyline, caBestRank, caSumRank, Math.Round(correlation, 2).ToString(CultureInfo.InvariantCulture), ToLongString(Math.Round(cardinality, 2)));
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
