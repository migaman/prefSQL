using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;
using prefSQL.SQLSkyline;

namespace prefSQL.SQLParserTest
{
    [TestClass]
    public class HelperIntegrationTests
    {
        public TestContext TestContext { get; set; }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "HelperIntegrationTests.xml",
            "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("HelperIntegrationTests.xml")]
        public void TestOnlyNonDominatedObjectsWithinSampleSkylineViaGetSkyline()
        {
            string skylineSampleSQL = TestContext.DataRow["skylineSampleSQL"].ToString();
            string entireSkylineSQL = TestContext.DataRow["entireSkylineSQL"].ToString();
            string testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);
            Debug.WriteLine(skylineSampleSQL);

            var common = new SQLCommon
            {
                SkylineType =
                    new SkylineBNL() { Provider = Helper.ProviderName, ConnectionString = Helper.ConnectionString }
            };

            var prefSqlModelSkylineSample = common.GetPrefSqlModelFromPreferenceSql(skylineSampleSQL);
            var prefSqlModelEntireSkyline = common.GetPrefSqlModelFromPreferenceSql(entireSkylineSQL);

            var subjectUnderTest = new SQLParser.Helper
            {
                ConnectionString = Helper.ConnectionString,
                DriverString = Helper.ProviderName
            };

            var sw = new Stopwatch();
            sw.Start();
            var entireSkyline = subjectUnderTest.GetResults(
                common.GetAnsiSqlFromPrefSqlModel(prefSqlModelEntireSkyline), common.SkylineType,
                prefSqlModelEntireSkyline);
            sw.Stop();
            Debug.WriteLine("ORIG ElapsedMilliseconds={0}", sw.ElapsedMilliseconds);
            Debug.WriteLine("ORIG Algorithm ElapsedMilliseconds={0}", subjectUnderTest.TimeInMilliseconds);
            sw.Restart();
            var sampleSkyline = subjectUnderTest.GetResults(
                common.GetAnsiSqlFromPrefSqlModel(prefSqlModelSkylineSample), common.SkylineType,
                prefSqlModelSkylineSample);
            sw.Stop();
            Debug.WriteLine("SMPL ElapsedMilliseconds={0}", sw.ElapsedMilliseconds);
            Debug.WriteLine("SMPL Algorithm ElapsedMilliseconds={0}", subjectUnderTest.TimeInMilliseconds);

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
            var skylineSampleSQL = TestContext.DataRow["skylineSampleSQL"].ToString();
            var entireSkylineSQL = TestContext.DataRow["entireSkylineSQL"].ToString();
            var testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);
            Debug.WriteLine(skylineSampleSQL);

            var common = new SQLCommon {SkylineType = new SkylineBNL()};

            var prefSqlModelSkylineSample = common.GetPrefSqlModelFromPreferenceSql(skylineSampleSQL);
            var prefSqlModelEntireSkyline = common.GetPrefSqlModelFromPreferenceSql(entireSkylineSQL);
            var subjectUnderTest = new SQLParser.Helper
            {
                ConnectionString = Helper.ConnectionString,
                DriverString = Helper.ProviderName
            };

            var entireSkyline = subjectUnderTest.GetResults(
                common.GetAnsiSqlFromPrefSqlModel(prefSqlModelEntireSkyline), common.SkylineType,
                prefSqlModelEntireSkyline);
            var sampleSkyline = subjectUnderTest.GetResults(
                common.GetAnsiSqlFromPrefSqlModel(prefSqlModelSkylineSample), common.SkylineType,
                prefSqlModelSkylineSample);

            var expected = TestContext.DataRow["entireCount"].ToString();
            var actual = entireSkyline.Rows.Count.ToString(CultureInfo.InvariantCulture);
            Assert.AreEqual(expected, actual, "Entire Skyline contains unexpected number of objects.");
        }
    }
}