namespace prefSQL.SQLParserTest
{
    using System;
    using System.Diagnostics;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using SQLParser;
    using SQLSkyline;

    [TestClass]
    public class SQLParserSkylineSamplingTests
    {
        public TestContext TestContext { get; set; }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SQLParserSkylineSamplingTests_CorrectSyntax.xml",
            "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SQLParserSkylineSamplingTests_CorrectSyntax.xml")]
        public void TestSyntaxValidityOfSyntacticallyCorrectSqlStatements()
        {
            var skylineSampleSQL = TestContext.DataRow["skylineSampleSQL"].ToString();
            var testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);
            Debug.WriteLine(skylineSampleSQL);

            var common = new SQLCommon {SkylineType = new SkylineSQL()};

            try
            {
                common.ParsePreferenceSQL(skylineSampleSQL);
            }
            catch (Exception exception)
            {
                Assert.Fail("{0} - {1}", "Syntactically correct SQL Query should not have thrown an Exception.",
                    exception.Message);
            }
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SQLParserSkylineSamplingTests_CorrectSyntax.xml",
            "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SQLParserSkylineSamplingTests_CorrectSyntax.xml")]
        public void TestParsedSkylineSqlCorrectness()
        {
            var skylineSampleSQL = TestContext.DataRow["skylineSampleSQL"].ToString();
            var testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);
            Debug.WriteLine(skylineSampleSQL);

            var common = new SQLCommon {SkylineType = new SkylineSQL()};

            var parsedSql = common.ParsePreferenceSQL(skylineSampleSQL);
            var parsedSqlExpected = TestContext.DataRow["parsePreferenceSQLSkylineSQLExpectedResult"].ToString();

            Assert.AreEqual(parsedSqlExpected.Trim(), parsedSql.Trim(), "SQL not built correctly");
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SQLParserSkylineSamplingTests_CorrectSyntax.xml",
            "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SQLParserSkylineSamplingTests_CorrectSyntax.xml")]
        public void TestParsedSkylineBnlCorrectness()
        {
            var skylineSampleSQL = TestContext.DataRow["skylineSampleSQL"].ToString();
            var testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);
            Debug.WriteLine(skylineSampleSQL);

            var common = new SQLCommon {SkylineType = new SkylineBNL()};

            var parsedSql = common.ParsePreferenceSQL(skylineSampleSQL);
            var parsedSqlExpected = TestContext.DataRow["parsePreferenceSQLSkylineBNLExpectedResult"].ToString();

            Assert.AreEqual(parsedSqlExpected.Trim(), parsedSql.Trim(), "SQL not built correctly");
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SQLParserSkylineSamplingTests_CorrectSyntax.xml",
            "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SQLParserSkylineSamplingTests_CorrectSyntax.xml")]
        public void TestParsedSkylineBnlSortCorrectness()
        {
            var skylineSampleSQL = TestContext.DataRow["skylineSampleSQL"].ToString();
            var testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);
            Debug.WriteLine(skylineSampleSQL);

            var common = new SQLCommon {SkylineType = new SkylineBNLSort()};

            var parsedSql = common.ParsePreferenceSQL(skylineSampleSQL);
            var parsedSqlExpected = TestContext.DataRow["parsePreferenceSQLSkylineBNLSortExpectedResult"].ToString();

            Assert.AreEqual(parsedSqlExpected.Trim(), parsedSql.Trim(), "SQL not built correctly");
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SQLParserSkylineSamplingTests_CorrectSyntax.xml",
            "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SQLParserSkylineSamplingTests_CorrectSyntax.xml")]
        public void TestParsedSkylineDqCorrectness()
        {
            var skylineSampleSQL = TestContext.DataRow["skylineSampleSQL"].ToString();
            var testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);
            Debug.WriteLine(skylineSampleSQL);

            var common = new SQLCommon {SkylineType = new SkylineDQ()};

            var parsedSql = common.ParsePreferenceSQL(skylineSampleSQL);
            var parsedSqlExpected = TestContext.DataRow["parsePreferenceSQLSkylineDQExpectedResult"].ToString();

            Assert.AreEqual(parsedSqlExpected.Trim(), parsedSql.Trim(), "SQL not built correctly");
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SQLParserSkylineSamplingTests_CorrectSyntax.xml",
            "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SQLParserSkylineSamplingTests_CorrectSyntax.xml")]
        public void TestParsedMultipleSkylineBnlCorrectness()
        {
            var skylineSampleSQL = TestContext.DataRow["skylineSampleSQL"].ToString();
            var testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);
            Debug.WriteLine(skylineSampleSQL);

            var common = new SQLCommon {SkylineType = new MultipleSkylineBNL()};

            var parsedSql = common.ParsePreferenceSQL(skylineSampleSQL);
            var parsedSqlExpected = TestContext.DataRow["parsePreferenceSQLMultipleSkylineBNLExpectedResult"].ToString();

            Assert.AreEqual(parsedSqlExpected.Trim(), parsedSql.Trim(), "SQL not built correctly");
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SQLParserSkylineSamplingTests_CorrectSyntax.xml",
            "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SQLParserSkylineSamplingTests_CorrectSyntax.xml")]
        public void TestParsedSkylineHexagonCorrectness()
        {
            var skylineSampleSQL = TestContext.DataRow["skylineSampleSQL"].ToString();
            var testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);
            Debug.WriteLine(skylineSampleSQL);

            var common = new SQLCommon {SkylineType = new SkylineHexagon()};

            var parsedSql = common.ParsePreferenceSQL(skylineSampleSQL);
            var parsedSqlExpected = TestContext.DataRow["parsePreferenceSQLSkylineHexagonExpectedResult"].ToString();

            Assert.AreEqual(parsedSqlExpected.Trim(), parsedSql.Trim(), "SQL not built correctly");
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML",
            "SQLParserSkylineSamplingTests_IncorrectSyntax.xml", "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SQLParserSkylineSamplingTests_IncorrectSyntax.xml")]
        public void TestSyntaxValidityOfSyntacticallyIncorrectSqlStatements()
        {
            var hasExceptionBeenRaised = false;

            var skylineSampleSQL = TestContext.DataRow["skylineSampleSQL"].ToString();
            Debug.WriteLine(skylineSampleSQL);

            var common = new SQLCommon {SkylineType = new SkylineSQL()};

            try
            {
                common.ParsePreferenceSQL(skylineSampleSQL);
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