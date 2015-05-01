namespace Utility
{
    using System;
    using System.Collections.Generic;
    using prefSQL.SQLParser;
    using prefSQL.SQLParser.Models;
    using prefSQL.SQLParserTest;
    using prefSQL.SQLSkyline;

    public class SamplingTest
    {
        public static void Main(string[] args)
        {
            //var skylineSampleSql =
            //    "SELECT * FROM cars cs SKYLINE OF cs.price LOW, cs.mileage LOW, cs.horsepower HIGH, cs.enginesize HIGH, cs.consumption LOW, cs.cylinders HIGH, cs.seats HIGH, cs.doors HIGH, cs.gears HIGH SAMPLE BY RANDOM_SUBSETS COUNT 10 DIMENSION 3";
            var skylineSampleSql =
                "SELECT cs.*, colors.name, fuels.name, bodies.name, makes.name, conditions.name FROM cars cs LEFT OUTER JOIN colors ON cs.color_id = colors.ID LEFT OUTER JOIN fuels ON cs.fuel_id = fuels.ID LEFT OUTER JOIN bodies ON cs.body_id = bodies.ID LEFT OUTER JOIN makes ON cs.make_id = makes.ID LEFT OUTER JOIN conditions ON cs.condition_id = conditions.ID SKYLINE OF cs.price LOW, cs.mileage LOW, cs.horsepower HIGH, cs.enginesize HIGH, cs.consumption LOW, cs.cylinders HIGH, cs.seats HIGH, cs.doors HIGH, cs.gears HIGH, colors.name ('red' >> 'blue' >> OTHERS EQUAL), fuels.name ('diesel' >> 'petrol' >> OTHERS EQUAL), bodies.name ('limousine' >> 'coupé' >> 'suv' >> 'minivan' >> OTHERS EQUAL), makes.name ('BMW' >> 'MERCEDES-BENZ' >> 'HUMMER' >> OTHERS EQUAL), conditions.name ('new' >> 'occasion' >> OTHERS EQUAL) SAMPLE BY RANDOM_SUBSETS COUNT 15 DIMENSION 3";
            var entireSkylineSampleSql =
                "SELECT cs.*, colors.name, fuels.name, bodies.name, makes.name, conditions.name FROM cars cs LEFT OUTER JOIN colors ON cs.color_id = colors.ID LEFT OUTER JOIN fuels ON cs.fuel_id = fuels.ID LEFT OUTER JOIN bodies ON cs.body_id = bodies.ID LEFT OUTER JOIN makes ON cs.make_id = makes.ID LEFT OUTER JOIN conditions ON cs.condition_id = conditions.ID SKYLINE OF cs.price LOW, cs.mileage LOW, cs.horsepower HIGH, cs.enginesize HIGH, cs.consumption LOW, cs.cylinders HIGH, cs.seats HIGH, cs.doors HIGH, cs.gears HIGH, colors.name ('red' >> 'blue' >> OTHERS EQUAL), fuels.name ('diesel' >> 'petrol' >> OTHERS EQUAL), bodies.name ('limousine' >> 'coupé' >> 'suv' >> 'minivan' >> OTHERS EQUAL), makes.name ('BMW' >> 'MERCEDES-BENZ' >> 'HUMMER' >> OTHERS EQUAL), conditions.name ('new' >> 'occasion' >> OTHERS EQUAL)";

            var common = new SQLCommon {SkylineType = new SkylineBNL()};

            var prefSqlModel = common.GetPrefSqlModelFromPreferenceSql(skylineSampleSql);
            var randomSubspacesesProducer = new RandomSubspacesProducer
            {
                AllPreferencesCount = prefSqlModel.Skyline.Count,
                SampleCount = prefSqlModel.SkylineSampleCount,
                SampleDimension = prefSqlModel.SkylineSampleDimension
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
            ExecuteSampleSkylines(producedSubspaces, prefSqlModel, common);
            ExecuteSampleSkylines(producedSubspaces, prefSqlModel, common);
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
                var subspacesProducer = new FixedSubspacesProducer(subspace);
                var utility = new SkylineSampleUtility(subspacesProducer);
                var skylineSample = new SkylineSample(utility) {Provider = Helper.ProviderName};

                var dataTable = skylineSample.getSkylineTable(Helper.ConnectionString, baseQuery, operators,
                    numberOfRecords, prefSqlModel.WithIncomparable, parameter, common.SkylineType,
                    prefSqlModel.SkylineSampleCount, prefSqlModel.SkylineSampleDimension);

                objectsCount += dataTable.Rows.Count;
                timeSpent += skylineSample.timeMilliseconds;
            }

            Console.WriteLine("time average: " + (double) timeSpent/producedSubspaces.Count);
            Console.WriteLine("objects average: " + (double) objectsCount/producedSubspaces.Count);
        }
    }
}