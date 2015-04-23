namespace Utility
{
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics;
    using prefSQL.SQLParser;
    using prefSQL.SQLSkyline;

    public class SamplingTest
    {
        private const string DbConnection = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
        private const string DbProvider = "System.Data.SqlClient";
        
        public static void Main(string[] args)
        {
            var skylineSampleSql =
                "SELECT * FROM cars cs SKYLINE OF cs.price LOW, cs.mileage LOW, cs.horsepower HIGH, cs.enginesize HIGH, cs.consumption LOW, cs.cylinders HIGH, cs.seats HIGH, cs.doors HIGH, cs.gears HIGH SAMPLE BY RANDOM_SUBSETS COUNT 10 DIMENSION 3";

            var common = new SQLCommon { SkylineType = new SkylineBNL() };

            common.parseAndExecutePrefSQL(DbConnection, DbProvider, skylineSampleSql);
        }
    }
}