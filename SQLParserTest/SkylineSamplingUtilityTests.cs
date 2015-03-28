namespace prefSQL.SQLParserTest
{
    using System;
    using System.Diagnostics;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using prefSQL.SQLParser;
    using prefSQL.SQLParser.Models;
    using prefSQL.SQLSkyline;

    [TestClass]
    public class SkylineSamplingUtilityTests
    {
        public TestContext TestContext { get; set; }

        [TestMethod]
        public void TestGetAnsiSqlWithEmptyPrefSqlModel()
        {
            var emptyPrefSqlModel = new PrefSQLModel();
            var common = new SQLCommon {SkylineType = new SkylineBNL()};
            var subjectUnderTest = new SkylineSamplingUtility(emptyPrefSqlModel, common);
            var ansiSql = subjectUnderTest.GetAnsiSql();

            Assert.AreEqual(String.Empty, ansiSql, "AnsiSQL of empty PrefSQLModel is not empty.");
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SkylineSamplingUtilityTests.xml", "TestDataRow",
            DataAccessMethod.Sequential),
         DeploymentItem("SkylineSamplingUtilityTests.xml")]
        public void TestProducedSubspaceQueries()
        {
            var skylineSampleSql = TestContext.DataRow["skylineSampleSQL"].ToString();
            var testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);
            Debug.WriteLine(skylineSampleSql);

            var common = new SQLCommon {SkylineType = new SkylineBNL()};

            var prefSqlModel = common.GetPrefSqlModelFromPreferenceSql(skylineSampleSql);
            var subjectUnderTest = new SkylineSamplingUtility(prefSqlModel, common);

            var subspaceQueriesProduced = subjectUnderTest.GetSubspaceQueries();
            var subspaceQueriesExpected =
                TestContext.DataRow.GetChildRows("TestDataRow_useSubspaces")[0].GetChildRows("useSubspaces_subspace");

            Assert.AreEqual(subspaceQueriesExpected.Length, subspaceQueriesProduced.Count,
                "Number of expected subspace queries is not equal to number of actual subspace queries produced.");

            foreach (var subspaceQueryExpected in subspaceQueriesExpected)
            {
                var subspaceQueryPreferenceSql =
                    common.parsePreferenceSQL(subspaceQueryExpected["equivalentToSkyline"].ToString());
                Assert.IsTrue(subspaceQueriesProduced.Contains(subspaceQueryPreferenceSql),
                    String.Format("Expected ANSI SQL string not produced: {0}.", subspaceQueryPreferenceSql));
            }
        }
    }
}