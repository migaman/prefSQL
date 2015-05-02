namespace Utility
{
    using System;
    using prefSQL.SQLParser;
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

            var dataTable = common.parseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName,
                entireSkylineSampleSql);
            Console.WriteLine(common.TimeInMilliseconds);
            Console.WriteLine(dataTable.Rows.Count);
            Console.WriteLine();

            for (var i = 0; i < 100; i++)
            {
                dataTable = common.parseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, skylineSampleSql);
                Console.WriteLine(common.TimeInMilliseconds);
                Console.WriteLine(dataTable.Rows.Count);
            }
        }
    }
}