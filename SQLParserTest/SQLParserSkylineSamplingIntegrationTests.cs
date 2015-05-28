namespace prefSQL.SQLParserTest
{
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using SQLParser;
    using SQLParser.Models;
    using SQLSkyline;
    using SQLSkyline.SkylineSampling;

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
            string entireSkylineSQL = TestContext.DataRow["entireSkylineSQL"].ToString();
            string testComment = TestContext.DataRow["comment"].ToString();
            int expectedNumberOfEntireSkylineObjects =
                int.Parse(TestContext.DataRow["expectedNumberOfEntireSkylineObjects"].ToString());
            Debug.WriteLine(testComment);
            Debug.WriteLine(entireSkylineSQL);

            var common = new SQLCommon { SkylineType = new SkylineBNL() };

            DataTable skyline = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName,
                entireSkylineSQL);

            Assert.AreEqual(expectedNumberOfEntireSkylineObjects, skyline.Rows.Count,
                "Unexpected number of Skyline objects.");
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML",
            "SQLParserSkylineSamplingIntegrationTests.xml", "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SQLParserSkylineSamplingIntegrationTests.xml")]
        public void TestNumberOfObjectsWithinSampleSkylineWithCountOneEqualsEntireSkyline()
        {
            string entireSkylineSQL = TestContext.DataRow["entireSkylineSQL"].ToString();
            string testComment = TestContext.DataRow["comment"].ToString();         
            Debug.WriteLine(testComment);
     
            var common = new SQLCommon { SkylineType = new SkylineBNL() };

            DataTable entireSkyline = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName,
                entireSkylineSQL);

            PrefSQLModel entirePrefSqlModel = common.GetPrefSqlModelFromPreferenceSql(entireSkylineSQL);

            DataTable sampleSkyline = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName,
                entireSkylineSQL + " SAMPLE BY RANDOM_SUBSETS COUNT 1 DIMENSION " + entirePrefSqlModel.Skyline.Count);

            Assert.AreEqual(entireSkyline.Rows.Count, sampleSkyline.Rows.Count,
              "Unexpected number of Skyline objects.");
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML",
            "SQLParserSkylineSamplingIntegrationTests.xml", "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SQLParserSkylineSamplingIntegrationTests.xml")]
        public void TestNumberOfObjectsWithinSampleSkyline()
        {
            string skylineSampleSQL = TestContext.DataRow["skylineSampleSQL"].ToString();
            string testComment = TestContext.DataRow["comment"].ToString();
            int expectedNumberOfSkylineSampleObjects =
                int.Parse(TestContext.DataRow["expectedNumberOfSkylineSampleObjects"].ToString());
            Debug.WriteLine(testComment);
            Debug.WriteLine(skylineSampleSQL);

            string baseQuery;
            string operators;
            int numberOfRecords;
            string[] parameter;

            var common = new SQLCommon
            {
                SkylineType =
                    new SkylineBNL() {Provider = Helper.ProviderName, ConnectionString = Helper.ConnectionString}
            };
            PrefSQLModel prefSqlModelSkylineSample = common.GetPrefSqlModelFromPreferenceSql(skylineSampleSQL);
            string ansiSql = common.GetAnsiSqlFromPrefSqlModel(prefSqlModelSkylineSample);

            prefSQL.SQLParser.Helper.DetermineParameters(ansiSql, out parameter, out baseQuery, out operators,
                out numberOfRecords);

            HashSet<HashSet<int>> useSubspaces = UseSubspaces(prefSqlModelSkylineSample);
            var subspacesProducer = new FixedSkylineSamplingSubspacesProducer(useSubspaces);
            var utility = new SkylineSamplingUtility(subspacesProducer);
            var skylineSample = new SkylineSampling(utility)
            {
                SubspacesCount = prefSqlModelSkylineSample.SkylineSampleCount,
                SubspaceDimension = prefSqlModelSkylineSample.SkylineSampleDimension
            };

            DataTable skyline = skylineSample.GetSkylineTable(baseQuery, operators, common.SkylineType);

            Assert.AreEqual(expectedNumberOfSkylineSampleObjects, skyline.Rows.Count,
                "Unexpected number of Sample Skyline objects.");
        }

        private HashSet<HashSet<int>> UseSubspaces(PrefSQLModel prefSqlModelSkylineSample)
        {
            var preferencesInSubspacesExpected = new HashSet<HashSet<int>>();

            DataRow[] subspacesExpected =
                TestContext.DataRow.GetChildRows("TestDataRow_useSubspaces")[0].GetChildRows("useSubspaces_subspace");

            foreach (DataRow subspaceExpected in subspacesExpected)
            {
                DataRow[] subspaceExpectedDimensions = subspaceExpected.GetChildRows("subspace_dimension");
                var preferencesInSingleSubspaceExpected = new HashSet<int>();
                foreach (DataRow singleSubspaceExpectedDimension in subspaceExpectedDimensions)
                {
                    for (var i = 0; i < prefSqlModelSkylineSample.Skyline.Count; i++)
                    {
                        AttributeModel attributeModel = prefSqlModelSkylineSample.Skyline[i];
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
            string skylineSampleSql = TestContext.DataRow["skylineSampleSQL"].ToString();
            string entireSkylineSql = TestContext.DataRow["entireSkylineSQL"].ToString();
            string testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);
            Debug.WriteLine(skylineSampleSql);
        
            string baseQuery;
            string operators;
            int numberOfRecords;
            string[] parameter;

            var common = new SQLCommon
            {
                SkylineType =
                    new SkylineBNL() { Provider = Helper.ProviderName, ConnectionString = Helper.ConnectionString }
            };

            PrefSQLModel prefSqlModelSkylineSample = common.GetPrefSqlModelFromPreferenceSql(skylineSampleSql);
            string ansiSql = common.GetAnsiSqlFromPrefSqlModel(prefSqlModelSkylineSample);

            prefSQL.SQLParser.Helper.DetermineParameters(ansiSql, out parameter, out baseQuery, out operators,
                out numberOfRecords);

            HashSet<HashSet<int>> useSubspaces = UseSubspaces(prefSqlModelSkylineSample);
            var subspacesProducer = new FixedSkylineSamplingSubspacesProducer(useSubspaces);
            var utility = new SkylineSamplingUtility(subspacesProducer);
            var skylineSample = new SkylineSampling(utility)
            {
                SubspacesCount = prefSqlModelSkylineSample.SkylineSampleCount,
                SubspaceDimension = prefSqlModelSkylineSample.SkylineSampleDimension
            };

            DataTable entireSkyline = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName,
                entireSkylineSql);
            DataTable sampleSkyline = skylineSample.GetSkylineTable(baseQuery, operators, common.SkylineType);

            HashSet<int> entireSkylineObjectsIds = GetHashSetOfIdsFromDataTable(entireSkyline);
            HashSet<int> sampleSkylineObjectsIds = GetHashSetOfIdsFromDataTable(sampleSkyline);

            sampleSkylineObjectsIds.ExceptWith(entireSkylineObjectsIds);

            Debug.WriteLine("wrong objects:");
            foreach (int i in sampleSkylineObjectsIds)
            {
                Debug.WriteLine(i);
            }

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