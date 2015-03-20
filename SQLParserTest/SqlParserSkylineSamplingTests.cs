namespace prefSQL.SQLParserTest
{
    using System;
    using System.Diagnostics;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using SQLParser;
    using SQLSkyline;

    [TestClass]
    public class SqlParserSkylineSamplingTests
    {
        public TestContext TestContext { get; set; }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SQLParserSkylineSamplingTests_CorrectSyntax.xml", "TestDataRow", DataAccessMethod.Sequential), DeploymentItem("SQLParserSkylineSamplingTests_CorrectSyntax.xml")]
        public void TestSyntaxValidityOfSyntacticallyCorrectSqlStatements()
        {
            var skylineSampleSql = TestContext.DataRow["skylineSampleSQL"].ToString();
            var testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);
            Debug.WriteLine(skylineSampleSql);

            var common = new SQLCommon { SkylineType = new SkylineSQL() };

            var parsedSql = string.Empty;
            try
            {
                parsedSql = common.parsePreferenceSQL(skylineSampleSql);
            }
            catch (Exception exception)
            {
                Assert.Fail("{0} - {1}", "Syntactically correct SQL Query should not have thrown an Exception.", exception.Message);
            }

            var parsedSqlExpected = TestContext.DataRow["parsePreferenceSQLSkylineSQLExpectedResult"].ToString();
            Debug.WriteLine(parsedSql);
            Debug.WriteLine(parsedSqlExpected);
            Assert.AreEqual(parsedSqlExpected.Trim(), parsedSql.Trim(), "SQL not built correctly");
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SQLParserSkylineSamplingTests_IncorrectSyntax.xml", "TestDataRow", DataAccessMethod.Sequential), DeploymentItem("SQLParserSkylineSamplingTests_IncorrectSyntax.xml")]
        public void TestSyntaxValidityOfSyntacticallyIncorrectSqlStatements()
        {
            var hasExceptionBeenRaised = false;

            var skylineSampleSql = TestContext.DataRow["skylineSampleSQL"].ToString();
            Console.WriteLine(skylineSampleSql);

            var common = new SQLCommon { SkylineType = new SkylineSQL() };

            try
            {
                common.parsePreferenceSQL(skylineSampleSql);
            }
            catch (Exception)
            {
                hasExceptionBeenRaised = true;
            }

            if (!hasExceptionBeenRaised)
            {
                Assert.Fail("Syntactically incorrect SQL Query should have thrown an Exception.");
            }
        }
    }
}
