using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;

namespace prefSQL.SQLParserTest
{
    [TestClass]
    public class SQLParserTests
    {


        [TestMethod]
        public void TestSKYLINE2DimensionsTextNoJoin()
        {
            String strPrefSQL = "SELECT cars.id, cars.price, cars.title FROM cars PREFERENCE LOW cars.title {'MERCEDES-BENZ SL 600' >> OTHERS} AND LOW cars.price";

            String expected = "SELECT cars.id, cars.price, cars.title FROM cars WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.price, cars_INNER.title FROM cars cars_INNER WHERE CASE WHEN cars_INNER.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 1 END <= CASE WHEN cars.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 1 END AND cars_INNER.price <= cars.price AND ( CASE WHEN cars_INNER.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 1 END < CASE WHEN cars.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 1 END OR cars_INNER.price < cars.price) )  ORDER BY CASE WHEN cars.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 1 END ASC, price ASC";
            SQLCommon common = new SQLCommon();
            String actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }



        [TestMethod]
        public void TestSKYLINE2DimensionsTextWithJoin()
        {
            String strPrefSQL = "SELECT cars.id, cars.price, cars.title, colors.name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID PREFERENCE LOW colors.name {'rot' >> OTHERS} AND LOW cars.price";

            String expected = "SELECT cars.id, cars.price, cars.title, colors.name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.price, cars_INNER.title, colors_INNER.name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE CASE WHEN colors_INNER.name = 'rot' THEN 0 ELSE 1 END <= CASE WHEN colors.name = 'rot' THEN 0 ELSE 1 END AND cars_INNER.price <= cars.price AND ( CASE WHEN colors_INNER.name = 'rot' THEN 0 ELSE 1 END < CASE WHEN colors.name = 'rot' THEN 0 ELSE 1 END OR cars_INNER.price < cars.price) )  ORDER BY CASE WHEN colors.name = 'rot' THEN 0 ELSE 1 END ASC, price ASC";

            SQLCommon common = new SQLCommon();
            String actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestSKYLINE2Dimensions()
        {
            String strPrefSQL = "SELECT * FROM cars PREFERENCE LOW cars.price AND LOW cars.mileage";

            String expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage) )  ORDER BY price ASC, mileage ASC";
            SQLCommon common = new SQLCommon();
            String actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");



        }


        [TestMethod]
        public void TestSKYLINE3Dimensions()
        {
            String strPrefSQL = "SELECT * FROM cars PREFERENCE LOW cars.price AND LOW cars.mileage AND HIGH cars.horsepower";

            String expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower >= cars.horsepower AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower > cars.horsepower) )  ORDER BY price ASC, mileage ASC, horsepower DESC";
            SQLCommon common = new SQLCommon();
            String actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");



        }


        [TestMethod]
        public void TestSKYLINE6Dimensions()
        {
            String strPrefSQL = "SELECT * FROM cars PREFERENCE LOW cars.price AND LOW cars.mileage AND HIGH cars.horsepower AND HIGH cars.enginesize AND HIGH cars.registration AND LOW cars.consumption";

            String expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower >= cars.horsepower AND cars_INNER.enginesize >= cars.enginesize AND cars_INNER.Registration >= cars.Registration AND cars_INNER.Consumption <= cars.Consumption AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower > cars.horsepower OR cars_INNER.EngineSize > cars.EngineSize OR cars_INNER.Registration > cars.Registration OR cars_INNER.Consumption < cars.Consumption) )  ORDER BY price ASC, mileage ASC, horsepower DESC, enginesize DESC, registration DESC, consumption ASC";
            SQLCommon common = new SQLCommon();
            String actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");



        }


        [TestMethod]
        public void TestLOW()
        {
            String strPrefSQL = "SELECT * FROM cars PREFERENCE LOW mileage";

            String expected = "SELECT * FROM cars ORDER BY mileage ASC";
            SQLCommon common = new SQLCommon();
            String actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");



        }


        [TestMethod]
        public void TestCustomLOW()
        {
            String strPrefSQL = "SELECT * FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID PREFERENCE LOW colors.name {'rot' >> 'blau' >> OTHERS >> 'grau'} ";

            String expected = "SELECT * FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID " +
                            "ORDER BY CASE WHEN colors.name = 'rot' THEN 0 WHEN colors.name = 'blau' THEN 1 WHEN colors.name = 'grau' THEN 3 ELSE 2 END ASC";
            SQLCommon common = new SQLCommon();
            String actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");

        }


        [TestMethod]
        public void TestHIGH()
        {
            String strPrefSQL = "SELECT * FROM cars PREFERENCE HIGH horsepower";

            String expected = "SELECT * FROM cars ORDER BY horsepower DESC";
            SQLCommon common = new SQLCommon();
            String actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");

        }


        [TestMethod]
        public void TestAROUND()
        {
            String strPrefSQL = "SELECT * FROM cars PREFERENCE price AROUND 15000";

            String expected = "SELECT * FROM cars ORDER BY ABS(price - 15000) ASC";
            SQLCommon common = new SQLCommon();
            String actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");

        }


        [TestMethod]
        public void TestAROUNDGeo()
        {
            String strPrefSQL = "SELECT * FROM cars PREFERENCE Location AROUND (47.0484, 8.32629)";

            String expected = "SELECT * FROM cars ORDER BY ABS(DISTANCE(Location, \"47.0484,8.32629\")) ASC";
            SQLCommon common = new SQLCommon();
            String actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");

        }




        [TestMethod]
        public void TestFAVOUR()
        {
            String strPrefSQL = "SELECT * FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID PREFERENCE colors.name FAVOUR 'rot'";

            String expected = "SELECT * FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID ORDER BY CASE WHEN colors.name = 'rot' THEN 1 ELSE 2 END ASC";
            SQLCommon common = new SQLCommon();
            String actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");

        }



        [TestMethod]
        public void TestDISFAVOUR()
        {
            String strPrefSQL = "SELECT * FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID PREFERENCE colors.name DISFAVOUR 'rot'";

            String expected = "SELECT * FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID ORDER BY CASE WHEN colors.name = 'rot' THEN 1 ELSE 2 END DESC";
            SQLCommon common = new SQLCommon();
            String actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");

        }

    }
}
