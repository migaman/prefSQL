using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;


namespace prefSQL.SQLParserTest
{
    [TestClass]
    public class SQLParserSortTests
    {
        /*
        [TestMethod]
        public void TestOrderingAttributePosition()
        {
            string strPrefSQL = "SELECT * FROM cars SKYLINE OF LOW cars.price AND LOW cars.mileage AND HIGH cars.horsepower";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower >= cars.horsepower AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower > cars.horsepower) )  ORDER BY price ASC, mileage ASC, horsepower DESC";
            SQLCommon common = new SQLCommon();
            common.OrderType = SQLCommon.Ordering.AttributePosition;
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }

        [TestMethod]
        public void TestOrderingAsIs()
        {
            string strPrefSQL = "SELECT * FROM cars SKYLINE OF LOW cars.price AND LOW cars.mileage AND HIGH cars.horsepower";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower >= cars.horsepower AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower > cars.horsepower) ) ";
            SQLCommon common = new SQLCommon();
            common.OrderType = SQLCommon.Ordering.AsIs;
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestOrderingRandom()
        {
            string strPrefSQL = "SELECT * FROM cars SKYLINE OF LOW cars.price AND LOW cars.mileage AND HIGH cars.horsepower";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower >= cars.horsepower AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower > cars.horsepower) )  ORDER BY NEWID()";
            SQLCommon common = new SQLCommon();
            common.OrderType = SQLCommon.Ordering.Random;
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }



        [TestMethod]
        public void TestOrderingRankingBestOf()
        {
            string strPrefSQL = "SELECT * FROM cars SKYLINE OF LOW cars.price AND LOW cars.mileage AND HIGH cars.horsepower";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower >= cars.horsepower AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower > cars.horsepower) )  ORDER BY CASE WHEN ROW_NUMBER() over (ORDER BY cars.price ASC) <=ROW_NUMBER() over (ORDER BY cars.mileage ASC) AND ROW_NUMBER() over (ORDER BY cars.price ASC) <=ROW_NUMBER() over (ORDER BY cars.horsepower DESC) THEN ROW_NUMBER() over (ORDER BY cars.price ASC) WHEN ROW_NUMBER() over (ORDER BY cars.mileage ASC) <=ROW_NUMBER() over (ORDER BY cars.horsepower DESC) THEN ROW_NUMBER() over (ORDER BY cars.mileage ASC)  ELSE ROW_NUMBER() over (ORDER BY cars.horsepower DESC) END";
            SQLCommon common = new SQLCommon();
            common.OrderType = SQLCommon.Ordering.RankingBestOf;
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");



        }


        [TestMethod]
        public void TestOrderingRankingSum()
        {
            string strPrefSQL = "SELECT * FROM cars SKYLINE OF LOW cars.price AND LOW cars.mileage AND HIGH cars.horsepower";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower >= cars.horsepower AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower > cars.horsepower) )  ORDER BY ROW_NUMBER() over (ORDER BY cars.price ASC) + ROW_NUMBER() over (ORDER BY cars.mileage ASC) + ROW_NUMBER() over (ORDER BY cars.horsepower DESC)";
            SQLCommon common = new SQLCommon();
            common.OrderType = SQLCommon.Ordering.RankingSummarize;
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");



        }*/
    }
}
