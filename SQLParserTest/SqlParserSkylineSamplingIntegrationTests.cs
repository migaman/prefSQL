namespace prefSQL.SQLParserTest
{
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using prefSQL.SQLParser;
    using prefSQL.SQLSkyline;

    [TestClass]
    public class SqlParserSkylineSamplingIntegrationTests
    {
        public TestContext TestContext { get; set; }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML",
            "SQLParserSkylineSamplingIntegrationTests.xml", "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SQLParserSkylineSamplingIntegrationTests.xml")]
        public void TestNumberOfObjectsWithinEntireSkyline()
        {
            var entireSkylineSql = TestContext.DataRow["entireSkylineSQL"].ToString();
            var testComment = TestContext.DataRow["comment"].ToString();
            var expectedNumberOfEntireSkylineObjects =
                int.Parse(TestContext.DataRow["expectedNumberOfEntireSkylineObjects"].ToString());
            Debug.WriteLine(testComment);
            Debug.WriteLine(entireSkylineSql);

            var common = new SQLCommon {SkylineType = new SkylineBNL()};

            var skyline = common.parseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, entireSkylineSql);

            Assert.AreEqual(expectedNumberOfEntireSkylineObjects, skyline.Rows.Count,
                "Unexpected number of Skyline objects.");
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML",
            "SQLParserSkylineSamplingIntegrationTests.xml", "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SQLParserSkylineSamplingIntegrationTests.xml")]
        public void TestOnlyNonDominatedObjectsWithinSampleSkyline()
        {
            var skylineSampleSql = TestContext.DataRow["skylineSampleSQL"].ToString();
            var entireSkylineSql = TestContext.DataRow["entireSkylineSQL"].ToString();
            var testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);
            Debug.WriteLine(skylineSampleSql);
            
            var common = new SQLCommon {SkylineType = new SkylineBNL()};

            var entireSkyline = common.parseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, entireSkylineSql);
            var sampleSkyline = common.parseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, skylineSampleSql);

            var entireSkylineObjectsIds = GetHashSetOfIdsFromDataTable(entireSkyline);
            var sampleSkylineObjectsIds = GetHashSetOfIdsFromDataTable(sampleSkyline);

            Assert.IsTrue(sampleSkylineObjectsIds.IsSubsetOf(entireSkylineObjectsIds),
                "Dominated objects contained in Sample Skyline (i.e., objects which are not contained in the entire Skyline).");
        }

        private static HashSet<int> GetHashSetOfIdsFromDataTable(DataTable entireSkyline)
        {
            var entireSkylineObjectsIds = new HashSet<int>();
            foreach (DataRow entireSkylineRow in entireSkyline.Rows)
            {
                entireSkylineObjectsIds.Add(int.Parse(entireSkylineRow["id"].ToString()));
            }
            return entireSkylineObjectsIds;
        }
    }
}