using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;

namespace prefSQL.SQLParserTest
{
    [TestClass]
    public class SQLParserPrioritizeTest
    {

        [TestMethod]
        public void TestPRIORITIZEWithJoinAndALIAS()
        {
            string strPrefSQL = "SELECT t1.id, t1.title FROM cars t1 LEFT OUTER JOIN colors t2 ON t1.color_id = t2.ID PREFERENCE LOW t1.price PRIORITIZE LOW t1.mileage PRIORITIZE HIGH t2.name {'pink' >> OTHERSEQUAL}";
            string expected = "SELECT * FROM (SELECT t1.id, t1.title, RANK() over (ORDER BY t1.price ASC) AS Rankprice, RANK() over (ORDER BY t1.mileage ASC) AS Rankmileage, RANK() over (ORDER BY CASE WHEN t2.name = 'pink' THEN 0 ELSE 100 END ASC) AS Rankt2name FROM cars t1 LEFT OUTER JOIN colors t2 ON t1.color_id = t2.ID) RankedResult  WHERE Rankprice = 1 OR Rankmileage = 1 OR Rankt2name = 1";
            SQLCommon common = new SQLCommon();
            common.OrderType = SQLCommon.Ordering.AsIs;
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert
            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }

        [TestMethod]
        public void TestPRIORITIZE2Dimensions()
        {
            string strPrefSQL = "SELECT t1.id, t1.title, t1.price, t1.mileage, t1.horsepower FROM cars t1 PREFERENCE LOW t1.price PRIORITIZE LOW t1.mileage";

            string expected = "SELECT * FROM (SELECT t1.id, t1.title, t1.price, t1.mileage, t1.horsepower, RANK() over (ORDER BY t1.price ASC) AS Rankprice, RANK() over (ORDER BY t1.mileage ASC) AS Rankmileage FROM cars t1) RankedResult  WHERE Rankprice = 1 OR Rankmileage = 1 ORDER BY price ASC, mileage ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected, actual, true, "SQL not built correctly");
        }

    }
}
