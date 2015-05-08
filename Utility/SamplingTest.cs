namespace Utility
{
    using System;
    using System.Collections.Generic;
    using prefSQL.SQLParser;
    using prefSQL.SQLParser.Models;
    using prefSQL.SQLParserTest;
    using prefSQL.SQLSkyline;
    using prefSQL.SQLSkyline.SamplingSkyline;

    public class SamplingTest
    {
        private const string SkylineSampleSql =
            "SELECT cs.*, colors.name, fuels.name, bodies.name, makes.name, conditions.name FROM cars cs LEFT OUTER JOIN colors ON cs.color_id = colors.ID LEFT OUTER JOIN fuels ON cs.fuel_id = fuels.ID LEFT OUTER JOIN bodies ON cs.body_id = bodies.ID LEFT OUTER JOIN makes ON cs.make_id = makes.ID LEFT OUTER JOIN conditions ON cs.condition_id = conditions.ID SKYLINE OF cs.price LOW, cs.mileage LOW, cs.horsepower HIGH, cs.enginesize HIGH, cs.consumption LOW, cs.cylinders HIGH, cs.seats HIGH, cs.doors HIGH, cs.gears HIGH, colors.name ('red' >> 'blue' >> OTHERS EQUAL), fuels.name ('diesel' >> 'petrol' >> OTHERS EQUAL), bodies.name ('limousine' >> 'coupé' >> 'suv' >> 'minivan' >> OTHERS EQUAL), makes.name ('BMW' >> 'MERCEDES-BENZ' >> 'HUMMER' >> OTHERS EQUAL), conditions.name ('new' >> 'occasion' >> OTHERS EQUAL) SAMPLE BY RANDOM_SUBSETS COUNT 15 DIMENSION 3";

        private const string EntireSkylineSampleSql =
            "SELECT cs.*, colors.name, fuels.name, bodies.name, makes.name, conditions.name FROM cars cs LEFT OUTER JOIN colors ON cs.color_id = colors.ID LEFT OUTER JOIN fuels ON cs.fuel_id = fuels.ID LEFT OUTER JOIN bodies ON cs.body_id = bodies.ID LEFT OUTER JOIN makes ON cs.make_id = makes.ID LEFT OUTER JOIN conditions ON cs.condition_id = conditions.ID SKYLINE OF cs.price LOW, cs.mileage LOW, cs.horsepower HIGH, cs.enginesize HIGH, cs.consumption LOW, cs.cylinders HIGH, cs.seats HIGH, cs.doors HIGH, cs.gears HIGH, colors.name ('red' >> 'blue' >> OTHERS EQUAL), fuels.name ('diesel' >> 'petrol' >> OTHERS EQUAL), bodies.name ('limousine' >> 'coupé' >> 'suv' >> 'minivan' >> OTHERS EQUAL), makes.name ('BMW' >> 'MERCEDES-BENZ' >> 'HUMMER' >> OTHERS EQUAL), conditions.name ('new' >> 'occasion' >> OTHERS EQUAL)";


        public static void Main(string[] args)
        {
            //TestExecutionForPerformance();
            TestForSetCoverage();
        }

        private static void TestForSetCoverage()
        {
            var common = new SQLCommon
            {
                SkylineType = new SkylineBNL(),
                ShowSkylineAttributes = true
            };

            var entireSkylineDataTable = common.parseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName,
                EntireSkylineSampleSql);
            //Console.WriteLine("time entire: " + common.TimeInMilliseconds);
            //Console.WriteLine("count entire: " + entireSkylineDataTable.Rows.Count);

            var skylineAttributeColumns = SkylineSamplingHelper.GetSkylineAttributeColumns(entireSkylineDataTable);
            var entireSkylineNormalized = prefSQL.SQLSkyline.Helper.GetDictionaryFromDataTable(entireSkylineDataTable, 0);
            SkylineSamplingHelper.NormalizeColumns(entireSkylineNormalized, skylineAttributeColumns);

            for (var i = 0; i < 10; i++)
            {
                var sampleSkylineDataTable = common.parseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName,
                    SkylineSampleSql);
                //Console.WriteLine("time sample: " + common.TimeInMilliseconds);
                //Console.WriteLine("count sample: " + sampleSkylineDataTable.Rows.Count);

                var sampleSkylineNormalized =
                    prefSQL.SQLSkyline.Helper.GetDictionaryFromDataTable(sampleSkylineDataTable, 0);
                SkylineSamplingHelper.NormalizeColumns(sampleSkylineNormalized, skylineAttributeColumns);

                var baseRandomSampleNormalized = SkylineSamplingHelper.GetRandomSample(entireSkylineNormalized,
                    sampleSkylineDataTable.Rows.Count);
                var secondRandomSampleNormalized = SkylineSamplingHelper.GetRandomSample(entireSkylineNormalized,
                    sampleSkylineDataTable.Rows.Count);

                var setCoverageCoveredBySecondRandomSample = SetCoverage.GetCoverage(baseRandomSampleNormalized,
                    secondRandomSampleNormalized, skylineAttributeColumns);
                var setCoverageCoveredBySkylineSample = SetCoverage.GetCoverage(baseRandomSampleNormalized,
                    sampleSkylineNormalized, skylineAttributeColumns);

                Console.WriteLine(setCoverageCoveredBySecondRandomSample);
                Console.WriteLine(setCoverageCoveredBySkylineSample);
                //Console.WriteLine("set coverage covered by second random sample: " +
                //                  setCoverageCoveredBySecondRandomSample);
                //Console.WriteLine("set coverage covered by skyline sample: " + setCoverageCoveredBySkylineSample);
            }
        }

        private static void TestExecutionForPerformance()
        {
            var common = new SQLCommon {SkylineType = new SkylineBNL()};

            var prefSqlModel = common.GetPrefSqlModelFromPreferenceSql(SkylineSampleSql);
            var randomSubspacesesProducer = new RandomSamplingSkylineSubspacesProducer
            {
                AllPreferencesCount = prefSqlModel.Skyline.Count,
                SubspacesCount = prefSqlModel.SkylineSampleCount,
                SubspaceDimension = prefSqlModel.SkylineSampleDimension
            };

            //var dataTable = common.parseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName,
            //    entireSkylineSampleSql);
            //Console.WriteLine(common.TimeInMilliseconds);
            //Console.WriteLine(dataTable.Rows.Count);
            //Console.WriteLine();

            var producedSubspaces = new List<HashSet<HashSet<int>>>();

            for (var i = 0; i < 100; i++)
            {
                producedSubspaces.Add(randomSubspacesesProducer.GetSubspaces());
            }

            ExecuteSampleSkylines(producedSubspaces, prefSqlModel, common);
            //ExecuteSampleSkylines(producedSubspaces, prefSqlModel, common);
            //ExecuteSampleSkylines(producedSubspaces, prefSqlModel, common);
        }

        private static void ExecuteSampleSkylines(List<HashSet<HashSet<int>>> producedSubspaces,
            PrefSQLModel prefSqlModel, SQLCommon common)
        {
            var objectsCount = 0;
            var timeSpent = 0L;

            string baseQuery;
            string operators;
            int numberOfRecords;
            string[] parameter;

            var ansiSql = common.GetAnsiSqlFromPrefSqlModel(prefSqlModel);
            prefSQL.SQLParser.Helper.DetermineParameters(ansiSql, out parameter, out baseQuery, out operators,
                out numberOfRecords);

            foreach (var subspace in producedSubspaces)
            {
                var subspacesProducer = new FixedSamplingSkylineSubspacesProducer(subspace);
                var utility = new SamplingSkylineUtility(subspacesProducer);
                var skylineSample = new SamplingSkyline(utility) {DbProvider = Helper.ProviderName};

                var dataTable = skylineSample.GetSkylineTable(Helper.ConnectionString, baseQuery, operators,
                    numberOfRecords, prefSqlModel.WithIncomparable, parameter, common.SkylineType,
                    prefSqlModel.SkylineSampleCount, prefSqlModel.SkylineSampleDimension, 0);

                objectsCount += dataTable.Rows.Count;
                timeSpent += skylineSample.timeMilliseconds;
            }

            Console.WriteLine("time average: " + (double) timeSpent/producedSubspaces.Count);
            Console.WriteLine("objects average: " + (double) objectsCount/producedSubspaces.Count);
        }
    }
}