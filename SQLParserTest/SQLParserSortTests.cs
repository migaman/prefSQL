using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;
using System.Data.SqlClient;

namespace prefSQL.SQLParserTest
{
    [TestClass]
    public class SQLParserSortTests
    {

        [TestMethod]
        public void TestOrderingAttributePosition()
        {
            string strPrefSQL = "SELECT * FROM cars PREFERENCE LOW cars.price AND LOW cars.mileage AND HIGH cars.horsepower";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower >= cars.horsepower AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower > cars.horsepower) )  ORDER BY price ASC, mileage ASC, horsepower DESC";
            SQLCommon common = new SQLCommon();
            common.OrderType = SQLCommon.OrderingType.AttributePosition;
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }

        [TestMethod]
        public void TestOrderingAsIs()
        {
            string strPrefSQL = "SELECT * FROM cars PREFERENCE LOW cars.price AND LOW cars.mileage AND HIGH cars.horsepower";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower >= cars.horsepower AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower > cars.horsepower) ) ";
            SQLCommon common = new SQLCommon();
            common.OrderType = SQLCommon.OrderingType.AsIs;
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestOrderingRandom()
        {
            string strPrefSQL = "SELECT * FROM cars PREFERENCE LOW cars.price AND LOW cars.mileage AND HIGH cars.horsepower";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower >= cars.horsepower AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower > cars.horsepower) )  ORDER BY NEWID()";
            SQLCommon common = new SQLCommon();
            common.OrderType = SQLCommon.OrderingType.Random;
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }



        [TestMethod]
        public void TestOrderingRankingBestOf()
        {
            string strPrefSQL = "SELECT * FROM cars PREFERENCE LOW cars.price AND LOW cars.mileage AND HIGH cars.horsepower";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower >= cars.horsepower AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower > cars.horsepower) )  ORDER BY CASE WHEN RANK() over (ORDER BY cars.price ASC) <=RANK() over (ORDER BY cars.mileage ASC) AND RANK() over (ORDER BY cars.price ASC) <=RANK() over (ORDER BY cars.horsepower DESC) THEN RANK() over (ORDER BY cars.price ASC) WHEN RANK() over (ORDER BY cars.mileage ASC) <=RANK() over (ORDER BY cars.horsepower DESC) THEN RANK() over (ORDER BY cars.mileage ASC)  ELSE RANK() over (ORDER BY cars.horsepower DESC) END";
            SQLCommon common = new SQLCommon();
            common.OrderType = SQLCommon.OrderingType.RankingBestOf;
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");



        }


        [TestMethod]
        public void TestOrderingRankingSum()
        {
            string strPrefSQL = "SELECT * FROM cars PREFERENCE LOW cars.price AND LOW cars.mileage AND HIGH cars.horsepower";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower >= cars.horsepower AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower > cars.horsepower) )  ORDER BY RANK() over (ORDER BY cars.price ASC) + RANK() over (ORDER BY cars.mileage ASC) + RANK() over (ORDER BY cars.horsepower DESC)";
            SQLCommon common = new SQLCommon();
            common.OrderType = SQLCommon.OrderingType.RankingSummarize;
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");



        }
    }
}
