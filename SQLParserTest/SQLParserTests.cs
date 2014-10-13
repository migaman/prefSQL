using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;

namespace prefSQL.SQLParserTest
{
    [TestClass]
    public class SQLParserTests
    {
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
