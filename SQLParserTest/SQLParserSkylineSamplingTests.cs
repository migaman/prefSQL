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
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SQLParserSkylineSamplingTests_TestSyntaxValidity.xml", "TestDataRow", DataAccessMethod.Sequential), DeploymentItem("SQLParserSkylineSamplingTests_TestSyntaxValidity.xml")]
        public void TestSyntaxValidity()
        {
            var skylineSampleSQL = testContextInstance.DataRow["skylineSampleSQL"].ToString();
            Console.WriteLine(skylineSampleSQL);
            var isExpectingException = testContextInstance.DataRow["result"].ToString() == "Fail" ? true : false;
            Console.WriteLine(isExpectingException);
            var common = new SQLCommon();
            common.SkylineType = new SkylineSQL();
            try
            {
                common.parsePreferenceSQL(skylineSampleSQL);
                if (isExpectingException)
                {
                    Assert.Fail("Syntactically incorrect SQL Query should have thrown an Exception.");
                }
            }
            catch (AssertFailedException) { throw; }
            catch (Exception exception)
            {
                if (!isExpectingException)
                {
                    Assert.Fail(String.Format("{0} - {1}", "Syntactically correct SQL Query should not have thrown an Exception.", exception.Message));
                }
            }
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.CSV", "files.csv", "files#csv", DataAccessMethod.Sequential)]
        public void TestAsd(string asd)
        {

        }
    }
}
