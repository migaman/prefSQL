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
            var hasExceptionBeenRaised = false;
            var skylineSampleSQL = testContextInstance.DataRow["skylineSampleSQL"].ToString();
            var isExpectingException = testContextInstance.DataRow["result"].ToString() == "Fail" ? true : false;

            Console.WriteLine(skylineSampleSQL);
            Console.WriteLine(isExpectingException);
            
            var common = new SQLCommon();
            common.SkylineType = new SkylineSQL();

            Exception exceptionRaised = null;
            try
            {
                common.parsePreferenceSQL(skylineSampleSQL);
            }
            catch (Exception exception)
            {
                exceptionRaised = exception;
                hasExceptionBeenRaised = true;
            }

            if (hasExceptionBeenRaised && !isExpectingException)
            {
                Assert.Fail(String.Format("{0} - {1}", "Syntactically correct SQL Query should not have thrown an Exception.", exceptionRaised.Message));
            } else if (!hasExceptionBeenRaised && isExpectingException)
            {
                  Assert.Fail("Syntactically incorrect SQL Query should have thrown an Exception.");
            } 
        }
    }
}
