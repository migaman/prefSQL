using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;

namespace prefSQL.SQLParserTest
{
    [TestClass]
    public class SQLParserTests
    {

        [TestMethod]
        public void TestSyntaxError()
        {
            string strPrefSQL = "SELECT * FROM cars PREFERENCE cars.price LOW";

            SQLCommon common = new SQLCommon();
            try
            {
                string actual = common.parsePreferenceSQL(strPrefSQL);
                Assert.Fail("Preference SQL Query should throw an Error");
            }
            catch(Exception e)
            {
                String strError = e.Message;
                Assert.IsTrue(true);
            }
        }


        [TestMethod]
        public void TestWithoutPreference()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars";

            string expected = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestLOW()
        {
            string strPrefSQL = "SELECT * FROM cars PREFERENCE LOW mileage";

            string expected = "SELECT * FROM cars ORDER BY mileage ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestHIGH()
        {
            string strPrefSQL = "SELECT * FROM cars PREFERENCE HIGH horsepower";

            string expected = "SELECT * FROM cars ORDER BY horsepower DESC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }

        [TestMethod]
        public void TestHIGHCustom()
        {
            string strPrefSQL = "SELECT * FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID PREFERENCE HIGH colors.name {'rot' >> 'blau' >> OTHERSEQUAL >> 'grau'} ";

            string expected = "SELECT * FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID ORDER BY CASE WHEN colors.name = 'rot' THEN 0 WHEN colors.name = 'blau' THEN 100 WHEN colors.name = 'grau' THEN 300 ELSE 200 END ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestAROUND()
        {
            string strPrefSQL = "SELECT * FROM cars PREFERENCE cars.price AROUND 15000";

            string expected = "SELECT * FROM cars ORDER BY ABS(cars.price - 15000) ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");

        }


        [TestMethod]
        public void TestAROUNDGeoCoordinate()
        {
            string strPrefSQL = "SELECT * FROM cars PREFERENCE cars.Location AROUND (47.0484, 8.32629)";

            string expected = "SELECT * FROM cars ORDER BY ABS(DISTANCE(cars.Location, \"47.0484,8.32629\")) ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");

        }

        [TestMethod]
        public void TestFAVOUR()
        {
            string strPrefSQL = "SELECT * FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID PREFERENCE colors.name FAVOUR 'rot'";

            string expected = "SELECT * FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID ORDER BY CASE WHEN colors.name = 'rot' THEN 1 ELSE 2 END ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");

        }



        [TestMethod]
        public void TestDISFAVOUR()
        {
            string strPrefSQL = "SELECT * FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID PREFERENCE colors.name DISFAVOUR 'rot'";

            string expected = "SELECT * FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID ORDER BY CASE WHEN colors.name = 'rot' THEN 1 ELSE 2 END DESC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");

        }

    }
}
