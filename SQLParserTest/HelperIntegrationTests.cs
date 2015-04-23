namespace prefSQL.SQLParserTest
{
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using prefSQL.SQLParser;
    using prefSQL.SQLSkyline;

    [TestClass]
    public class HelperIntegrationTests
    {
        private const string DbConnection = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
        private const string DbProvider = "System.Data.SqlClient";

        public TestContext TestContext { get; set; }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "HelperIntegrationTests.xml",
            "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("HelperIntegrationTests.xml")]
        public void TestOnlyNonDominatedObjectsWithinSampleSkylineViaGetSkyline()
        {
            var skylineSampleSql = TestContext.DataRow["skylineSampleSQL"].ToString();
            var entireSkylineSql = TestContext.DataRow["entireSkylineSQL"].ToString();
            var testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);
            Debug.WriteLine(skylineSampleSql);

            var common = new SQLCommon {SkylineType = new SkylineBNL()};

            var prefSqlModelSkylineSample = common.GetPrefSqlModelFromPreferenceSql(skylineSampleSql);
            var prefSqlModelEntireSkyline = common.GetPrefSqlModelFromPreferenceSql(entireSkylineSql);
            var subjectUnderTest = new Helper
            {
                ConnectionString = DbConnection,
                DriverString = DbProvider
            };

            var sw = new Stopwatch();
            sw.Start();
            var entireSkyline = subjectUnderTest.getResults(
                common.GetAnsiSqlFromPrefSqlModel(prefSqlModelEntireSkyline), common.SkylineType,
                prefSqlModelEntireSkyline);
            sw.Stop();
            Debug.WriteLine("ORIG Elapsed={0}", sw.Elapsed);
            sw.Restart();
            var sampleSkyline = subjectUnderTest.getResults(
                common.GetAnsiSqlFromPrefSqlModel(prefSqlModelSkylineSample), common.SkylineType,
                prefSqlModelSkylineSample);
            sw.Stop();
            Debug.WriteLine("SMPL Elapsed={0}", sw.Elapsed);

            var entireSkylineObjectsIds = GetHashSetOfIdsFromDataTable(entireSkyline);
            var sampleSkylineObjectsIds = GetHashSetOfIdsFromDataTable(sampleSkyline);
            Debug.WriteLine("ORIG Count={0}", entireSkylineObjectsIds.Count);
            Debug.WriteLine("SMPL Count={0}", sampleSkylineObjectsIds.Count);

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

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "HelperIntegrationTests.xml",
            "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("HelperIntegrationTests.xml")]
        public void TestObjectsWithinEntireSkylineCount()
        {
            var skylineSampleSql = TestContext.DataRow["skylineSampleSQL"].ToString();
            var entireSkylineSql = TestContext.DataRow["entireSkylineSQL"].ToString();
            var testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);
            Debug.WriteLine(skylineSampleSql);

            var common = new SQLCommon {SkylineType = new SkylineBNL()};

            var prefSqlModelSkylineSample = common.GetPrefSqlModelFromPreferenceSql(skylineSampleSql);
            var prefSqlModelEntireSkyline = common.GetPrefSqlModelFromPreferenceSql(entireSkylineSql);
            var subjectUnderTest = new Helper
            {
                ConnectionString = DbConnection,
                DriverString = DbProvider
            };

            var entireSkyline = subjectUnderTest.getResults(
               common.GetAnsiSqlFromPrefSqlModel(prefSqlModelEntireSkyline), common.SkylineType,
               prefSqlModelEntireSkyline);
            var sampleSkyline = subjectUnderTest.getResults(
                common.GetAnsiSqlFromPrefSqlModel(prefSqlModelSkylineSample), common.SkylineType,
                prefSqlModelSkylineSample);

            var expected = TestContext.DataRow["entireCount"].ToString();
            var actual = entireSkyline.Rows.Count.ToString(CultureInfo.InvariantCulture);
            Assert.AreEqual(expected, actual, "Entire Skyline contains unexpected number of objects.");
        }
    }
}