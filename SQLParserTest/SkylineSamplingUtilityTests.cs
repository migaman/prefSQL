namespace prefSQL.SQLParserTest
{
    using System;
    using System.Collections.Generic;
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
        public void TestProducedSubspaces()
        {
            //var skylineSampleSql = TestContext.DataRow["skylineSampleSQL"].ToString();
            //var testComment = TestContext.DataRow["comment"].ToString();
            //Debug.WriteLine(testComment);
            //Debug.WriteLine(skylineSampleSql);

            //var common = new SQLCommon {SkylineType = new SkylineBNL()};

            //var prefSqlModel = common.GetPrefSqlModelFromPreferenceSql(skylineSampleSql);
            //var subjectUnderTest = new SkylineSamplingUtility(prefSqlModel, common);

            //var preferencesInProducedSubspaces = SubspacesAsStrings(subjectUnderTest.Subspaces);
            //var preferencesInExpectedSubspaces = ExpectedSubspacesAsStrings();

            //Assert.AreEqual(preferencesInExpectedSubspaces.Count, preferencesInProducedSubspaces.Count,
            //    "Number of expected subspaces is not equal to number of actual subspaces produced.");

            //foreach (var preferencesInSingleExpectedSubspace in preferencesInExpectedSubspaces)
            //{
            //    var expectedSubsetIsContainedInProducedSubpaces = false;
            //    foreach (var preferencesInSingleProducedSubspace in preferencesInProducedSubspaces)
            //    {
            //        if (preferencesInSingleProducedSubspace.SetEquals(preferencesInSingleExpectedSubspace))
            //        {
            //            expectedSubsetIsContainedInProducedSubpaces = true;
            //        }
            //    }

            //    Assert.IsTrue(expectedSubsetIsContainedInProducedSubpaces,
            //        String.Format("Expected subspace not produced: {0}.",
            //            string.Join(", ", preferencesInSingleExpectedSubspace)));
            //}
        }

        private HashSet<HashSet<string>> ExpectedSubspacesAsStrings()
        {
            var preferencesInSubspacesExpected = new HashSet<HashSet<string>>();

            var subspacesExpected =
                TestContext.DataRow.GetChildRows("TestDataRow_useSubspaces")[0].GetChildRows("useSubspaces_subspace");

            foreach (var subspaceExpected in subspacesExpected)
            {
                var subspaceExpectedDimensions = subspaceExpected.GetChildRows("subspace_dimension");
                var preferencesInSingleSubspaceExpected = new HashSet<string>();
                foreach (var singleSubspaceExpectedDimension in subspaceExpectedDimensions)
                {
                    preferencesInSingleSubspaceExpected.Add(singleSubspaceExpectedDimension[0].ToString());
                }
                preferencesInSubspacesExpected.Add(preferencesInSingleSubspaceExpected);
            }

            return preferencesInSubspacesExpected;
        }

        private static HashSet<HashSet<string>> SubspacesAsStrings(
            IEnumerable<HashSet<AttributeModel>> subspaces)
        {
            var preferencesInSubspaces = new HashSet<HashSet<string>>();
            foreach (var subspace in subspaces)
            {
                var preferencesInSingleSubspace = new HashSet<string>();
                foreach (var singlePreferenceInSubspace in subspace)
                {
                    preferencesInSingleSubspace.Add(singlePreferenceInSubspace.FullColumnName);
                }
                preferencesInSubspaces.Add(preferencesInSingleSubspace);
            }
            return preferencesInSubspaces;
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SkylineSamplingUtilityTests_Incorrect.xml",
            "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SkylineSamplingUtilityTests_Incorrect.xml")]
        [Timeout(5000)]
        public void TestIncorrectSubspaceQueries()
        {
            //var hasExceptionBeenRaised = false;

            //var skylineSampleSql = TestContext.DataRow["skylineSampleSQL"].ToString();
            //var testComment = TestContext.DataRow["comment"].ToString();
            //Debug.WriteLine(testComment);
            //Debug.WriteLine(skylineSampleSql);

            //var common = new SQLCommon {SkylineType = new SkylineBNL()};

            //var prefSqlModel = common.GetPrefSqlModelFromPreferenceSql(skylineSampleSql);
            //var subjectUnderTest = new SkylineSamplingUtility(prefSqlModel, common);

            //try
            //{
            //    var subspaceQueries = subjectUnderTest.SubspaceQueries;
            //}
            //catch (Exception exception)
            //{
            //    hasExceptionBeenRaised = true;
            //    Debug.WriteLine(exception.Message);
            //}

            //if (!hasExceptionBeenRaised)
            //{
            //    Assert.Fail("Syntactically incorrect SQL Query should have thrown an Exception.");
            //}
        }
    }
}