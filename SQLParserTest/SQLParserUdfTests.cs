using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;

namespace prefSQL.SQLParserTest
{
    [TestClass]
    public class SQLParserUdfTests
    {
        [TestMethod]
        [TestCategory("UnitTest")]
        public void TestUdfAsField()
        {
            // prefSQL with UDF
            const string prefQuery = "SELECT c.id, dbo.udf1(param1), mySchema.udf2(param1) AS MyField " + 
                                     "FROM cars AS c " +
                                     "SKYLINE OF c.price LOW " +
                                     "ORDER BY c.price ASC";
            const string expectedQuery = "SELECT c.id, dbo.udf1(param1), mySchema.udf2(param1) AS MyField FROM cars AS c WHERE NOT EXISTS(SELECT c_INNER.id, dbo.udf1(param1), mySchema.udf2(param1) AS MyField FROM cars AS c_INNER WHERE c_INNER.price <= c.price AND ( c_INNER.price < c.price) ) ORDER BY c.price ASC";

            // build query
            var engine = new SQLCommon();
            var actualQuery = engine.ParsePreferenceSQL(prefQuery);

            // verify outcome
            Assert.AreEqual(expectedQuery, actualQuery);
        }
 
        [TestMethod]
        [TestCategory("UnitTest")]
        public void TestUdfInOrderClause()
        {
            // prefSQL with UDF
            const string prefQuery = "SELECT c.id " +
                                     "FROM cars AS c " +
                                     "SKYLINE OF c.price LOW " +
                                     "ORDER BY c.price ASC, mySchema.myUdf(param1) DESC";
            const string expectedQuery = "SELECT c.id FROM cars AS c WHERE NOT EXISTS(SELECT c_INNER.id FROM cars AS c_INNER WHERE c_INNER.price <= c.price AND ( c_INNER.price < c.price) ) ORDER BY c.price ASC, mySchema.myUdf(param1) DESC";

            // build query
            var engine = new SQLCommon();
            var actualQuery = engine.ParsePreferenceSQL(prefQuery);

            // verify outcome
            Assert.AreEqual(expectedQuery, actualQuery);
        }

        [TestMethod]
        [TestCategory("UnitTest")]
        public void TestSortingWithUdfPrecededByCategorial()
        {
            // prefSQL with UDF
            const string prefQuery = "SELECT c.id " +
                                     "FROM cars AS c " +
                                     "LEFT OUTER JOIN colors AS cl ON c.color_id = cl.ID " +
                                     "SKYLINE OF c.price LOW " +
                                     "ORDER BY cl.name('pink' >> {'red','black'} >> 'beige'=='yellow'), mySchema.myUdf(param1) DESC";
            const string expectedQuery = "SELECT c.id FROM cars AS c LEFT OUTER JOIN colors AS cl ON c.color_id = cl.ID WHERE NOT EXISTS(SELECT c_INNER.id FROM cars AS c_INNER LEFT OUTER JOIN colors AS cl_INNER ON c_INNER.color_id = cl_INNER.ID WHERE c_INNER.price <= c.price AND ( c_INNER.price < c.price) ) ORDER BY CASE WHEN cl.name = 'pink' THEN 0 WHEN cl.name IN ('red','black') THEN 100 WHEN cl.name = 'beige' THEN 200 WHEN cl.name = 'yellow' THEN 200 END ASC, mySchema.myUdf(param1) DESC";

            // build query
            var engine = new SQLCommon();
            var actualQuery = engine.ParsePreferenceSQL(prefQuery);

            // verify outcome
            Assert.AreEqual(expectedQuery, actualQuery);
        }

        [TestMethod]
        [TestCategory("UnitTest")]
        public void TestSortingWithUdfFollowedByCategorial()
        {
            // prefSQL with UDF
            const string prefQuery = "SELECT c.id " +
                                     "FROM cars AS c " +
                                     "LEFT OUTER JOIN colors AS cl ON c.color_id = cl.ID " +
                                     "SKYLINE OF c.price LOW " +
                                     "ORDER BY mySchema.myUdf(param1) DESC, cl.name('pink' >> {'red','black'} >> 'beige'=='yellow')";
            const string expectedQuery = "SELECT c.id FROM cars AS c LEFT OUTER JOIN colors AS cl ON c.color_id = cl.ID WHERE NOT EXISTS(SELECT c_INNER.id FROM cars AS c_INNER LEFT OUTER JOIN colors AS cl_INNER ON c_INNER.color_id = cl_INNER.ID WHERE c_INNER.price <= c.price AND ( c_INNER.price < c.price) ) ORDER BY mySchema.myUdf(param1) DESC, CASE WHEN cl.name = 'pink' THEN 0 WHEN cl.name IN ('red','black') THEN 100 WHEN cl.name = 'beige' THEN 200 WHEN cl.name = 'yellow' THEN 200 END ASC";

            // build query
            var engine = new SQLCommon();
            var actualQuery = engine.ParsePreferenceSQL(prefQuery);

            // verify outcome
            Assert.AreEqual(expectedQuery, actualQuery);
        }

    }
}
