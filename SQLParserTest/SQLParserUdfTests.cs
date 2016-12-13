using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;
using prefSQL.SQLSkyline;

namespace prefSQL.SQLParserTest
{
    [TestClass]
    public class SQLParserUdfTests
    {
        [TestMethod]
        [TestCategory("UnitTest")]
        public void TestUdfExpressionsNative()
        {
            // prefSQL with UDF
            const string prefQuery = "SELECT c.id, dbo.someUDF(c.price, 1.5) AS SomeUDF1, someSchema.someUDF(c.price, 2.5) AS SomeUDF2 " + 
                                     "FROM cars AS c " +
                                     "SKYLINE OF co.Name ('pink' >> 'black' >> OTHERS INCOMPARABLE), c.price LOW " +
                                     "ORDER BY c.price ASC, someSchema.someUDF(c.price, 10) DESC";
            const string expectedQuery = "SELECT c.id, dbo.someUDF(c.price, 1.5) AS SomeUDF1, someSchema.someUDF(c.price, 2.5) AS SomeUDF2 FROM cars AS c " +
                                         "WHERE NOT EXISTS(SELECT c_INNER.id, dbo.someUDF(c_INNER.price, 1.5) AS SomeUDF1, someSchema.someUDF(c_INNER.price, 2.5) AS SomeUDF2 FROM cars AS c_INNER WHERE (CASE WHEN co_INNER.Name = 'pink' THEN 0 WHEN co_INNER.Name = 'black' THEN 100 ELSE 201 END <= CASE WHEN co.Name = 'pink' THEN 0 WHEN co.Name = 'black' THEN 100 ELSE 200 END OR co_INNER.Name = co.Name) AND c_INNER.price <= c.price AND ( CASE WHEN co_INNER.Name = 'pink' THEN 0 WHEN co_INNER.Name = 'black' THEN 100 ELSE 201 END < CASE WHEN co.Name = 'pink' THEN 0 WHEN co.Name = 'black' THEN 100 ELSE 200 END OR c_INNER.price < c.price) ) " +
                                         "ORDER BY c.price ASC, someSchema.someUDF(c.price, 10) DESC";

            // build query
            var engine = new SQLCommon {SkylineType = new SkylineSQL()};
            var actualQuery = engine.ParsePreferenceSQL(prefQuery);

            // verify outcome
            Assert.AreEqual(expectedQuery, actualQuery);
        }

        [TestMethod]
        [TestCategory("UnitTest")]
        public void TestUdfExpressionsClr()
        {
            // prefSQL with UDF
            const string prefQuery = "SELECT c.id, dbo.someUDF(c.price, 1.5) AS SomeUDF1, someSchema.someUDF(c.price, 2.5) AS SomeUDF2 " +
                                     "FROM cars AS c " +
                                     "SKYLINE OF co.Name ('pink' >> 'black' >> OTHERS INCOMPARABLE), c.price LOW " +
                                     "ORDER BY c.price ASC, someSchema.someUDF(c.price, 10) DESC";
            const string expectedQuery = "EXEC dbo.prefSQL_SkylineBNL " +
                                         "'SELECT  CAST(CASE WHEN co.Name = ''pink'' THEN 0 WHEN co.Name = ''black'' THEN 100 ELSE 200 END AS bigint) AS SkylineAttribute0, CASE WHEN co.Name = ''pink'' THEN '''' WHEN co.Name = ''black'' THEN '''' ELSE co.Name END, CAST(c.price AS bigint) AS SkylineAttribute1 , c.id, dbo.someUDF(c.price, 1.5) AS SomeUDF1, someSchema.someUDF(c.price, 2.5) AS SomeUDF2 " + 
                                         "FROM cars AS c " +
                                         "ORDER BY c.price ASC, someSchema.someUDF(c.price, 10) DESC', " + 
                                         "'LOW;INCOMPARABLE;LOW', 0, 4";

            // build query
            var engine = new SQLCommon { SkylineType = new SkylineBNL() };
            var actualQuery = engine.ParsePreferenceSQL(prefQuery);

            // verify outcome
            Assert.AreEqual(expectedQuery, actualQuery);
        }
        
        [TestMethod]
        [TestCategory("UnitTest")]
        public void TestUdfPreferenceMixedParam()
        {
            // prefSQL with UDF
            const string prefQuery = "SELECT c.id " +
                                     "FROM cars AS c " +
                                     "SKYLINE OF mySchema.myUdf(c.price, 0.77, 'fixedValue') LOW " +
                                     //"SKYLINE OF c.price LOW " +
                                     "ORDER BY c.price ASC";
            const string expectedNativeQuery = "SELECT c.id FROM cars AS c WHERE NOT EXISTS(SELECT c_INNER.id FROM cars AS c_INNER WHERE mySchema.myUdf(c_INNER.price, 0.77, 'fixedValue') <= mySchema.myUdf(c.price, 0.77, 'fixedValue') AND ( mySchema.myUdf(c_INNER.price, 0.77, 'fixedValue') < mySchema.myUdf(c.price, 0.77, 'fixedValue')) ) ORDER BY c.price ASC";
            const string expectedClrQuery = "EXEC dbo.prefSQL_SkylineBNLLevel 'SELECT  CAST(mySchema.myUdf(c.price, 0.77, ''fixedValue'') AS bigint) AS SkylineAttribute0 , c.id FROM cars AS c ORDER BY c.price ASC', 'LOW', 0, 4";

            // build query
            var engine = new SQLCommon();
            var actualNativeQuery = engine.ParsePreferenceSQL(prefQuery);
            engine.SkylineType = new SkylineBNL();
            var actualClrQuery = engine.ParsePreferenceSQL(prefQuery);

            // verify outcome
            Assert.AreEqual(expectedNativeQuery, actualNativeQuery);
            Assert.AreEqual(expectedClrQuery, actualClrQuery);
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
                                     "ORDER BY cl.name ('pink' >> {'red','black'} >> 'beige'=='yellow'), mySchema.myUdf(param1) DESC";
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
                                     "ORDER BY mySchema.myUdf(param1) DESC, cl.name ('pink' >> {'red','black'} >> 'beige'=='yellow')";
            const string expectedQuery = "SELECT c.id FROM cars AS c LEFT OUTER JOIN colors AS cl ON c.color_id = cl.ID WHERE NOT EXISTS(SELECT c_INNER.id FROM cars AS c_INNER LEFT OUTER JOIN colors AS cl_INNER ON c_INNER.color_id = cl_INNER.ID WHERE c_INNER.price <= c.price AND ( c_INNER.price < c.price) ) ORDER BY mySchema.myUdf(param1) DESC, CASE WHEN cl.name = 'pink' THEN 0 WHEN cl.name IN ('red','black') THEN 100 WHEN cl.name = 'beige' THEN 200 WHEN cl.name = 'yellow' THEN 200 END ASC";

            // build query
            var engine = new SQLCommon();
            var actualQuery = engine.ParsePreferenceSQL(prefQuery);

            // verify outcome
            Assert.AreEqual(expectedQuery, actualQuery);
        }

        [TestMethod]
        [TestCategory("UnitTest")]
        public void TestTableAliasBug()
        {
            // prefSQL: no overlap in schema name and table alias
            const string prefQuery1 = "SELECT o.id, someSchema.someUDF(o.id) AS Udf1 " +
                                     "FROM cars AS o " +
                                     "SKYLINE OF o.price LOW ";
            const string expectedQuery1 = "SELECT o.id, someSchema.someUDF(o.id) AS Udf1 FROM cars AS o " +
                                         "WHERE NOT EXISTS(SELECT o_INNER.id, someSchema.someUDF(o_INNER.id) AS Udf1 FROM cars AS o_INNER WHERE o_INNER.price <= o.price AND ( o_INNER.price < o.price) )";

            // prefSQL: table alias is part of schema name
            const string prefQuery2 = "SELECT o.id, dbo.someUDF(o.id) AS Udf1 " +
                                     "FROM cars AS o " +
                                     "SKYLINE OF o.price LOW ";
            const string expectedQuery2 = "SELECT o.id, dbo.someUDF(o.id) AS Udf1 FROM cars AS o " +
                                         "WHERE NOT EXISTS(SELECT o_INNER.id, dbo.someUDF(o_INNER.id) AS Udf1 FROM cars AS o_INNER WHERE o_INNER.price <= o.price AND ( o_INNER.price < o.price) )";

            // build query
            var engine = new SQLCommon();
            var actualQuery1 = engine.ParsePreferenceSQL(prefQuery1);
            var actualQuery2 = engine.ParsePreferenceSQL(prefQuery2);

            // verify outcome
            Assert.AreEqual(expectedQuery1, actualQuery1);
            Assert.AreEqual(expectedQuery2, actualQuery2);
        }

        [TestMethod]
        [TestCategory("UnitTest")]
        public void TestUdfWithStringParam()
        {
            // prefSQL with UDF
            const string prefQuery = "SELECT c.id, dbo.myUdf('actParam') AS udf1 " +
                                     "FROM cars AS c " +
                                     "LEFT OUTER JOIN colors AS cl ON c.color_id = cl.ID " +
                                     "SKYLINE OF c.price LOW";
            const string expectedQuery = "SELECT c.id, dbo.myUdf('actParam') AS udf1 FROM cars AS c LEFT OUTER JOIN colors AS cl ON c.color_id = cl.ID WHERE NOT EXISTS(SELECT c_INNER.id, dbo.myUdf('actParam') AS udf1 FROM cars AS c_INNER LEFT OUTER JOIN colors AS cl_INNER ON c_INNER.color_id = cl_INNER.ID WHERE c_INNER.price <= c.price AND ( c_INNER.price < c.price) )";

            // build query
            var engine = new SQLCommon();
            var actualQuery = engine.ParsePreferenceSQL(prefQuery);

            // verify outcome
            Assert.AreEqual(expectedQuery, actualQuery);
        }

        [TestMethod]
        [TestCategory("UnitTest")]
        public void TestUdfWithNumericParam()
        {
            // prefSQL with UDF
            const string prefQuery = "SELECT c.id, dbo.myUdf(1.77, 2.88) AS udf1 " +
                                     "FROM cars AS c " +
                                     "LEFT OUTER JOIN colors AS cl ON c.color_id = cl.ID " +
                                     "SKYLINE OF c.price LOW";
            const string expectedQuery = "SELECT c.id, dbo.myUdf(1.77, 2.88) AS udf1 FROM cars AS c LEFT OUTER JOIN colors AS cl ON c.color_id = cl.ID WHERE NOT EXISTS(SELECT c_INNER.id, dbo.myUdf(1.77, 2.88) AS udf1 FROM cars AS c_INNER LEFT OUTER JOIN colors AS cl_INNER ON c_INNER.color_id = cl_INNER.ID WHERE c_INNER.price <= c.price AND ( c_INNER.price < c.price) )";

            // build query
            var engine = new SQLCommon();
            var actualQuery = engine.ParsePreferenceSQL(prefQuery);

            // verify outcome
            Assert.AreEqual(expectedQuery, actualQuery);
        }

    }
}
