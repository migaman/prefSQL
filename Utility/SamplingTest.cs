namespace Utility
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics;
    using System.Numerics;
    using prefSQL.SQLParser;
    using prefSQL.SQLParser.Models;
    using prefSQL.SQLParserTest;
    using prefSQL.SQLSkyline;
    using prefSQL.SQLSkyline.SamplingSkyline;

    public sealed class SamplingTest
    {
        private const string BaseSkylineSQL =
            "SELECT cs.*, colors.name, bodies.name, fuels.name, makes.name, conditions.name, drives.name, transmissions.name FROM cars cs LEFT OUTER JOIN colors ON cs.color_id = colors.ID LEFT OUTER JOIN bodies ON cs.body_id = bodies.ID LEFT OUTER JOIN fuels ON cs.fuel_id = fuels.ID LEFT OUTER JOIN makes ON cs.make_id = makes.ID LEFT OUTER JOIN conditions ON cs.condition_id = conditions.ID LEFT OUTER JOIN drives ON cs.drive_id = drives.ID LEFT OUTER JOIN transmissions ON cs.transmission_id = transmissions.ID SKYLINE OF ";

        private readonly string _entireSkylineSql;
        private readonly string _entireSkylineSqlBestRank;
        private readonly string _entireSkylineSqlSumRank;
        private readonly string _skylineSampleSql;

        public SamplingTest()
        {
            var addPreferences = "";
            foreach (object preference in Performance.GetCategoricalPreferences())
            {
                addPreferences += preference.ToString().Replace("cars", "cs") + ", ";
            }
            addPreferences = addPreferences.TrimEnd(", ".ToCharArray());
            _entireSkylineSql = BaseSkylineSQL + addPreferences;
            _skylineSampleSql = _entireSkylineSql + " SAMPLE BY RANDOM_SUBSETS COUNT 15 DIMENSION 3";
            //_entireSkylineSql =
            //    "SELECT cs.*, colors.name, fuels.name, bodies.name, makes.name, conditions.name FROM cars cs LEFT OUTER JOIN colors ON cs.color_id = colors.ID LEFT OUTER JOIN fuels ON cs.fuel_id = fuels.ID LEFT OUTER JOIN bodies ON cs.body_id = bodies.ID LEFT OUTER JOIN makes ON cs.make_id = makes.ID LEFT OUTER JOIN conditions ON cs.condition_id = conditions.ID SKYLINE OF cs.price LOW, cs.mileage LOW, cs.horsepower HIGH, cs.enginesize HIGH, cs.consumption LOW, cs.cylinders HIGH, cs.seats HIGH, cs.doors HIGH, cs.gears HIGH, colors.name ('red' >> 'blue' >> OTHERS EQUAL), fuels.name ('diesel' >> 'petrol' >> OTHERS EQUAL), bodies.name ('limousine' >> 'coupé' >> 'suv' >> 'minivan' >> OTHERS EQUAL), makes.name ('BMW' >> 'MERCEDES-BENZ' >> 'HUMMER' >> OTHERS EQUAL), conditions.name ('new' >> 'occasion' >> OTHERS EQUAL)";
            //_skylineSampleSql = _entireSkylineSql + " SAMPLE BY RANDOM_SUBSETS COUNT 15 DIMENSION 3";

            _entireSkylineSqlBestRank = _entireSkylineSql.Replace("SELECT ", "SELECT TOP XXX ") +
                                        " ORDER BY BEST_RANK()";
            _entireSkylineSqlSumRank = _entireSkylineSql.Replace("SELECT ", "SELECT TOP XXX ") + " ORDER BY SUM_RANK()";
        }

        public static void Main(string[] args)
        {
            var samplingTest = new SamplingTest();

            samplingTest.TestExecutionForPerformance(10);
            //samplingTest.TestForSetCoverage();
            //samplingTest.TestForClusterAnalysis();            

            Console.ReadKey();
        }

        private void TestForClusterAnalysis()
        {
            var common = new SQLCommon
            {
                SkylineType =
                    new SkylineBNL() {Provider = Helper.ProviderName, ConnectionString = Helper.ConnectionString},
                ShowSkylineAttributes = true
            };

            DbProviderFactory factory = DbProviderFactories.GetFactory(Helper.ProviderName);

            // use the factory object to create Data access objects.
            DbConnection connection = factory.CreateConnection();
            // will return the connection object (i.e. SqlConnection ...)
            connection.ConnectionString = Helper.ConnectionString;

            var dt = new DataTable();

            connection.Open();

            DbDataAdapter dap = factory.CreateDataAdapter();
            DbCommand selectCommand = connection.CreateCommand();
            selectCommand.CommandTimeout = 0; //infinite timeout

            string strQuery;
            string operators;
            int numberOfRecords;
            string[] parameter;

            string ansiSql =
                common.GetAnsiSqlFromPrefSqlModel(common.GetPrefSqlModelFromPreferenceSql(_entireSkylineSql));
            prefSQL.SQLParser.Helper.DetermineParameters(ansiSql, out parameter, out strQuery, out operators,
                out numberOfRecords);

            selectCommand.CommandText = strQuery;
            dap.SelectCommand = selectCommand;
            dt = new DataTable();

            dap.Fill(dt);

            DataTable entireSkylineDataTable = common.ParseAndExecutePrefSQL(Helper.ConnectionString,
                Helper.ProviderName,
                _entireSkylineSql);

            int[] skylineAttributeColumns = SkylineSamplingHelper.GetSkylineAttributeColumns(entireSkylineDataTable);
            Dictionary<long, object[]> entireSkylineNormalized =
                prefSQL.SQLSkyline.Helper.GetDictionaryFromDataTable(entireSkylineDataTable, 0);
            SkylineSamplingHelper.NormalizeColumns(entireSkylineNormalized, skylineAttributeColumns);

            DataTable sampleSkylineDataTable = common.ParseAndExecutePrefSQL(Helper.ConnectionString,
                Helper.ProviderName,
                _skylineSampleSql);
            Dictionary<long, object[]> sampleSkylineNormalized =
                prefSQL.SQLSkyline.Helper.GetDictionaryFromDataTable(sampleSkylineDataTable, 0);
            SkylineSamplingHelper.NormalizeColumns(sampleSkylineNormalized, skylineAttributeColumns);

            Dictionary<BigInteger, List<Dictionary<long, object[]>>> entireBuckets =
                ClusterAnalysis.GetBuckets(entireSkylineNormalized, skylineAttributeColumns);
            Dictionary<BigInteger, List<Dictionary<long, object[]>>> sampleBuckets =
                ClusterAnalysis.GetBuckets(sampleSkylineNormalized, skylineAttributeColumns);

            Dictionary<BigInteger, List<Dictionary<long, object[]>>> aggregatedEntireBuckets =
                ClusterAnalysis.GetAggregatedBuckets(entireBuckets);
            Dictionary<BigInteger, List<Dictionary<long, object[]>>> aggregatedSampleBuckets =
                ClusterAnalysis.GetAggregatedBuckets(sampleBuckets);

            for (var i = 0; i < skylineAttributeColumns.Length; i++)
            {
                dt.Columns.RemoveAt(0);
            }

            Dictionary<long, object[]> full = prefSQL.SQLSkyline.Helper.GetDictionaryFromDataTable(dt, 0);
            SkylineSamplingHelper.NormalizeColumns(full, skylineAttributeColumns);

            Dictionary<BigInteger, List<Dictionary<long, object[]>>> fullB =
                ClusterAnalysis.GetBuckets(full, skylineAttributeColumns);
            Dictionary<BigInteger, List<Dictionary<long, object[]>>> aFullB =
                ClusterAnalysis.GetAggregatedBuckets(fullB);

            for (var i = 0; i < skylineAttributeColumns.Length; i++)
            {
                int entire = aggregatedEntireBuckets.ContainsKey(i) ? aggregatedEntireBuckets[i].Count : 0;
                int sample = aggregatedSampleBuckets.ContainsKey(i) ? aggregatedSampleBuckets[i].Count : 0;
                double entirePercent = (double) entire / entireSkylineNormalized.Count;
                double samplePercent = (double) sample / sampleSkylineNormalized.Count;
                int fullX = aFullB.ContainsKey(i) ? aFullB[i].Count : 0;
                double fullP = (double) fullX / full.Count;
                Console.WriteLine("-- {0,2} -- {5,6} ({6,7:P2} %) -- {1,6} ({3,7:P2} %) -- {2,6} ({4,7:P2} %)", i,
                    entire, sample, entirePercent,
                    samplePercent, fullX, fullP);
            }

            Console.WriteLine();
            Console.WriteLine("{0} - {1} - {2}", entireSkylineNormalized.Count, sampleSkylineNormalized.Count,
                full.Count);
        }

        private void TestForSetCoverage()
        {
            var common = new SQLCommon
            {
                SkylineType =
                    new SkylineBNL() {Provider = Helper.ProviderName, ConnectionString = Helper.ConnectionString},
                ShowSkylineAttributes = true
            };

            DataTable entireSkylineDataTable = common.ParseAndExecutePrefSQL(Helper.ConnectionString,
                Helper.ProviderName, _entireSkylineSql);
            //Console.WriteLine("time entire: " + common.TimeInMilliseconds);
            //Console.WriteLine("count entire: " + entireSkylineDataTable.Rows.Count);

            int[] skylineAttributeColumns = SkylineSamplingHelper.GetSkylineAttributeColumns(entireSkylineDataTable);
            Dictionary<long, object[]> entireSkylineNormalized =
                prefSQL.SQLSkyline.Helper.GetDictionaryFromDataTable(entireSkylineDataTable, 0);
            SkylineSamplingHelper.NormalizeColumns(entireSkylineNormalized, skylineAttributeColumns);

            for (var i = 0; i < 10; i++)
            {
                DataTable sampleSkylineDataTable = common.ParseAndExecutePrefSQL(Helper.ConnectionString,
                    Helper.ProviderName, _skylineSampleSql);
                //Console.WriteLine("time sample: " + common.TimeInMilliseconds);
                //Console.WriteLine("count sample: " + sampleSkylineDataTable.Rows.Count);

                Dictionary<long, object[]> sampleSkylineNormalized =
                    prefSQL.SQLSkyline.Helper.GetDictionaryFromDataTable(sampleSkylineDataTable, 0);
                SkylineSamplingHelper.NormalizeColumns(sampleSkylineNormalized, skylineAttributeColumns);

                Dictionary<long, object[]> baseRandomSampleNormalized =
                    SkylineSamplingHelper.GetRandomSample(entireSkylineNormalized,
                        sampleSkylineDataTable.Rows.Count);
                Dictionary<long, object[]> secondRandomSampleNormalized =
                    SkylineSamplingHelper.GetRandomSample(entireSkylineNormalized,
                        sampleSkylineDataTable.Rows.Count);

                double setCoverageCoveredBySecondRandomSample = SetCoverage.GetCoverage(baseRandomSampleNormalized,
                    secondRandomSampleNormalized, skylineAttributeColumns);
                double setCoverageCoveredBySkylineSample = SetCoverage.GetCoverage(baseRandomSampleNormalized,
                    sampleSkylineNormalized, skylineAttributeColumns);

                DataTable entireSkylineDataTableBestRank = common.ParseAndExecutePrefSQL(Helper.ConnectionString,
                    Helper.ProviderName,
                    _entireSkylineSqlBestRank.Replace("XXX", sampleSkylineDataTable.Rows.Count.ToString()));
                DataTable entireSkylineDataTableSumRank = common.ParseAndExecutePrefSQL(Helper.ConnectionString,
                    Helper.ProviderName,
                    _entireSkylineSqlSumRank.Replace("XXX", sampleSkylineDataTable.Rows.Count.ToString()));

                Dictionary<long, object[]> entireSkylineDataTableBestRankNormalized =
                    prefSQL.SQLSkyline.Helper.GetDictionaryFromDataTable(entireSkylineDataTableBestRank, 0);
                SkylineSamplingHelper.NormalizeColumns(entireSkylineDataTableBestRankNormalized, skylineAttributeColumns);
                Dictionary<long, object[]> entireSkylineDataTableSumRankNormalized =
                    prefSQL.SQLSkyline.Helper.GetDictionaryFromDataTable(entireSkylineDataTableSumRank, 0);
                SkylineSamplingHelper.NormalizeColumns(entireSkylineDataTableSumRankNormalized, skylineAttributeColumns);

                double setCoverageCoveredByEntireBestRank = SetCoverage.GetCoverage(baseRandomSampleNormalized,
                    entireSkylineDataTableBestRankNormalized, skylineAttributeColumns);
                double setCoverageCoveredByEntireSumRank = SetCoverage.GetCoverage(baseRandomSampleNormalized,
                    entireSkylineDataTableSumRankNormalized, skylineAttributeColumns);

                Console.WriteLine("sc second random: " + setCoverageCoveredBySecondRandomSample);
                Console.WriteLine("sc sample       : " + setCoverageCoveredBySkylineSample);
                Console.WriteLine("sc entire best  : " + setCoverageCoveredByEntireBestRank);
                Console.WriteLine("sc entire sum   : " + setCoverageCoveredByEntireSumRank);
                //Console.WriteLine("set coverage covered by second random sample: " +
                //                  setCoverageCoveredBySecondRandomSample);
                //Console.WriteLine("set coverage covered by skyline sample: " + setCoverageCoveredBySkylineSample);
            }
        }

        private void TestExecutionForPerformance(int runs)
        {
            var common = new SQLCommon
            {
                SkylineType =
                    new SkylineBNL() {Provider = Helper.ProviderName, ConnectionString = Helper.ConnectionString}
            };

            PrefSQLModel prefSqlModel = common.GetPrefSqlModelFromPreferenceSql(_skylineSampleSql);
            var randomSubspacesesProducer = new RandomSamplingSkylineSubspacesProducer
            {
                AllPreferencesCount = prefSqlModel.Skyline.Count,
                SubspacesCount = prefSqlModel.SkylineSampleCount,
                SubspaceDimension = prefSqlModel.SkylineSampleDimension
            };

            //DataTable dataTable = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName,
            //    _entireSkylineSql);
            //Console.WriteLine(common.TimeInMilliseconds);
            //Console.WriteLine(dataTable.Rows.Count);
            //Console.WriteLine();

            var producedSubspaces = new List<HashSet<HashSet<int>>>();

            for (var i = 0; i < runs; i++)
            {
                producedSubspaces.Add(randomSubspacesesProducer.GetSubspaces());
            }

            //var temp = new HashSet<HashSet<int>>
            //{
            //    new HashSet<int>() {0, 1, 2},
            //    new HashSet<int>() {2, 3, 4},
            //    new HashSet<int>() {4, 5, 6}
            //};

            //producedSubspaces.Add(temp);
            //var temp = new HashSet<HashSet<int>>
            //{
            //    new HashSet<int>() {0, 1, 2},
            //    new HashSet<int>() {3, 4, 5},
            //    new HashSet<int>() {6, 7, 8},
            //    new HashSet<int>() {9, 10, 11},
            //    new HashSet<int>() {1, 2, 12},
            //    new HashSet<int>() {1, 3, 12},
            //    new HashSet<int>() {1, 4, 8},
            //    new HashSet<int>() {1, 5, 8},
            //    new HashSet<int>() {1, 6, 9},
            //    new HashSet<int>() {2, 6, 9},
            //    new HashSet<int>() {5, 1, 4},
            //    new HashSet<int>() {4, 3, 2},
            //    new HashSet<int>() {2, 5, 1},
            //    new HashSet<int>() {3, 2, 0},
            //    new HashSet<int>() {13, 10, 7}
            //};

            //producedSubspaces.Add(temp);
            ExecuteSampleSkylines(producedSubspaces, prefSqlModel, common);
            //ExecuteSampleSkylines(producedSubspaces, prefSqlModel, common);
            //ExecuteSampleSkylines(producedSubspaces, prefSqlModel, common);
        }

        private void ExecuteSampleSkylines(List<HashSet<HashSet<int>>> producedSubspaces,
            PrefSQLModel prefSqlModel, SQLCommon common)
        {
            var objectsCount = 0;
            var timeSpent = 0L;

            string strQuery;
            string operators;
            int numberOfRecords;
            string[] parameter;

            string ansiSql = common.GetAnsiSqlFromPrefSqlModel(prefSqlModel);
            Debug.Write(ansiSql);
            prefSQL.SQLParser.Helper.DetermineParameters(ansiSql, out parameter, out strQuery, out operators,
                out numberOfRecords);

            foreach (HashSet<HashSet<int>> subspace in producedSubspaces)
            {
                var subspacesProducer = new FixedSamplingSkylineSubspacesProducer(subspace);
                var utility = new SamplingSkylineUtility(subspacesProducer);
                var skylineSample = new SamplingSkyline(utility)
                {
                    SubspacesCount = prefSqlModel.SkylineSampleCount,
                    SubspaceDimension = prefSqlModel.SkylineSampleDimension
                };

                DataTable dataTable = skylineSample.GetSkylineTable(strQuery, operators, common.SkylineType);

                objectsCount += dataTable.Rows.Count;
                timeSpent += skylineSample.TimeMilliseconds;
                foreach (HashSet<int> attribute in subspace)
                {
                    Console.Write("[");
                    foreach (int attribute1 in attribute)
                    {
                        Console.Write(attribute1 + ",");
                    }
                    Console.Write("],");
                }
                Console.WriteLine();
                Console.WriteLine("time : " + skylineSample.TimeMilliseconds);
                Console.WriteLine("objects : " + dataTable.Rows.Count);
            }

            Console.WriteLine("time average: " + (double) timeSpent / producedSubspaces.Count);
            Console.WriteLine("objects average: " + (double) objectsCount / producedSubspaces.Count);
        }
    }
}