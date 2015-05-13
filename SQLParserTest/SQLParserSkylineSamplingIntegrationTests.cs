using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;
using prefSQL.SQLParser.Models;
using prefSQL.SQLSkyline;
using prefSQL.SQLSkyline.SamplingSkyline;

namespace prefSQL.SQLParserTest
{
    [TestClass]
    public class SQLParserSkylineSamplingIntegrationTests
    {
        public TestContext TestContext { get; set; }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML",
            "SQLParserSkylineSamplingIntegrationTests.xml", "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SQLParserSkylineSamplingIntegrationTests.xml")]
        public void TestNumberOfObjectsWithinEntireSkyline()
        {
            var entireSkylineSQL = TestContext.DataRow["entireSkylineSQL"].ToString();
            var testComment = TestContext.DataRow["comment"].ToString();
            var expectedNumberOfEntireSkylineObjects =
                int.Parse(TestContext.DataRow["expectedNumberOfEntireSkylineObjects"].ToString());
            Debug.WriteLine(testComment);
            Debug.WriteLine(entireSkylineSQL);

            var common = new SQLCommon {SkylineType = new SkylineBNL()};

            var skyline = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, entireSkylineSQL);

            Assert.AreEqual(expectedNumberOfEntireSkylineObjects, skyline.Rows.Count,
                "Unexpected number of Skyline objects.");
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML",
            "SQLParserSkylineSamplingIntegrationTests.xml", "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SQLParserSkylineSamplingIntegrationTests.xml")]
        public void TestNumberOfObjectsWithinSampleSkyline()
        {
            var skylineSampleSQL = TestContext.DataRow["skylineSampleSQL"].ToString();
            var testComment = TestContext.DataRow["comment"].ToString();
            var expectedNumberOfSamplingSkylineObjects =
                int.Parse(TestContext.DataRow["expectedNumberOfSamplingSkylineObjects"].ToString());
            Debug.WriteLine(testComment);
            Debug.WriteLine(skylineSampleSQL);

            string baseQuery;
            string operators;
            int numberOfRecords;
            string[] parameter;

            var common = new SQLCommon {SkylineType = new SkylineBNL()};
            var prefSqlModelSkylineSample = common.GetPrefSqlModelFromPreferenceSql(skylineSampleSQL);
            var ansiSql = common.GetAnsiSqlFromPrefSqlModel(prefSqlModelSkylineSample);

            SQLParser.Helper.DetermineParameters(ansiSql, out parameter, out baseQuery, out operators,
                out numberOfRecords);

            var useSubspaces = UseSubspaces(prefSqlModelSkylineSample);
            var subspacesProducer = new FixedSamplingSkylineSubspacesProducer(useSubspaces);
            var utility = new SamplingSkylineUtility(subspacesProducer);
            var skylineSample = new SamplingSkyline(utility) {DbProvider = Helper.ProviderName};

            var skyline = skylineSample.GetSkylineTable(Helper.ConnectionString, baseQuery, operators, numberOfRecords,
                prefSqlModelSkylineSample.WithIncomparable, parameter, common.SkylineType,
                prefSqlModelSkylineSample.SkylineSampleCount, prefSqlModelSkylineSample.SkylineSampleDimension, 0);

            Assert.AreEqual(expectedNumberOfSamplingSkylineObjects, skyline.Rows.Count,
                "Unexpected number of Sample Skyline objects.");
        }

        private HashSet<HashSet<int>> UseSubspaces(PrefSQLModel prefSqlModelSkylineSample)
        {
            var preferencesInSubspacesExpected = new HashSet<HashSet<int>>();

            var subspacesExpected =
                TestContext.DataRow.GetChildRows("TestDataRow_useSubspaces")[0].GetChildRows("useSubspaces_subspace");

            foreach (var subspaceExpected in subspacesExpected)
            {
                var subspaceExpectedDimensions = subspaceExpected.GetChildRows("subspace_dimension");
                var preferencesInSingleSubspaceExpected = new HashSet<int>();
                foreach (var singleSubspaceExpectedDimension in subspaceExpectedDimensions)
                {
                    for (var i = 0; i < prefSqlModelSkylineSample.Skyline.Count; i++)
                    {
                        var attributeModel = prefSqlModelSkylineSample.Skyline[i];
                        if (attributeModel.FullColumnName == singleSubspaceExpectedDimension[0].ToString())
                        {
                            preferencesInSingleSubspaceExpected.Add(i);
                            break;
                        }
                    }
                }
                preferencesInSubspacesExpected.Add(preferencesInSingleSubspaceExpected);
            }

            return preferencesInSubspacesExpected;
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

            var entireSkyline = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName,
                entireSkylineSql);
            var sampleSkyline = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName,
                skylineSampleSql);

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