using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;
using prefSQL.SQLSkyline;
using System;
using System.Data;

namespace prefSQL.SQLParserTest
{
    [TestClass]
    public class SQLParserSkylineSamplingTests
    {

        [TestMethod]
        public void TestSyntaxValidity()
        {
            var skylineSampleSQL = "SELECT * FROM cars_small cs SKYLINE OF cs.price LOW, cs.mileage LOW SAMPLE BY (cs.price), (cs.mileage)";
            var common = new SQLCommon();
            common.SkylineType = new SkylineSQL();
            try
            {
                common.parsePreferenceSQL(skylineSampleSQL);
            }
            catch (Exception exception)
            {
                Assert.Fail(String.Format("{0} - {1}"), "Syntactically correct SQL Query should not throw an Exception.", exception.Message);
            }
        }
    }
}
