using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;
using prefSQL.SQLSkyline;
using System;
using System.Diagnostics;
using System.Data;

namespace prefSQL.SQLParserTest
{
    [TestClass]
    public class SQLParserSkylineSamplingTests
    {
        private TestContext testContextInstance;
        public TestContext TestContext
        {
            get { return testContextInstance; }
            set { testContextInstance = value; }
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SQLParserSkylineSamplingTests_CorrectSyntax.xml", "TestDataRow", DataAccessMethod.Sequential), DeploymentItem("SQLParserSkylineSamplingTests_CorrectSyntax.xml")]
        public void TestSyntaxValidityOfSyntacticallyCorrectSQLStatements()
        {
            var skylineSampleSQL = testContextInstance.DataRow["skylineSampleSQL"].ToString();
            Console.WriteLine(skylineSampleSQL);

            var common = new SQLCommon();
            common.SkylineType = new SkylineSQL();

            try
            {
                var parsedSQL = common.parsePreferenceSQL(skylineSampleSQL);
                Console.WriteLine(parsedSQL);
            }
            catch (Exception exception)
            {
                Assert.Fail(String.Format("{0} - {1}", "Syntactically correct SQL Query should not have thrown an Exception.", exception.Message));
            }
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SQLParserSkylineSamplingTests_IncorrectSyntax.xml", "TestDataRow", DataAccessMethod.Sequential), DeploymentItem("SQLParserSkylineSamplingTests_IncorrectSyntax.xml")]
        public void TestSyntaxValidityOfSyntacticallyIncorrectSQLStatements()
        {
            var hasExceptionBeenRaised = false;

            var skylineSampleSQL = testContextInstance.DataRow["skylineSampleSQL"].ToString();
            Console.WriteLine(skylineSampleSQL);

            var common = new SQLCommon();
            common.SkylineType = new SkylineSQL();

            try
            {
                common.parsePreferenceSQL(skylineSampleSQL);
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
