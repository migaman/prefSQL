namespace SQLSkylineTest
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using prefSQL.SQLSkyline;

    [TestClass]
    public class SkylineSampleUtilityTests
    {
        public TestContext TestContext { get; set; }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SkylineSampleUtilityTests.xml", "TestDataRow",
            DataAccessMethod.Sequential),
         DeploymentItem("SkylineSampleUtilityTests.xml")]
        public void TestProducedSubspaces()
        {
            var testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);

            var attributesCount = int.Parse(TestContext.DataRow["attributesCount"].ToString());
            var sampleCount = int.Parse(TestContext.DataRow["sampleCount"].ToString());
            var sampleDimension = int.Parse(TestContext.DataRow["sampleDimension"].ToString());

            var subjectUnderTest = new SkylineSampleUtility(attributesCount, sampleCount, sampleDimension);

            var preferencesInProducedSubspaces = subjectUnderTest.Subspaces;
            var preferencesInExpectedSubspaces = ExpectedSubspaces();

            Assert.AreEqual(preferencesInExpectedSubspaces.Count, preferencesInProducedSubspaces.Count,
                "Number of expected subspaces is not equal to number of actual subspaces produced.");

            foreach (var preferencesInSingleExpectedSubspace in preferencesInExpectedSubspaces)
            {
                var expectedSubsetIsContainedInProducedSubpaces = false;
                foreach (var preferencesInSingleProducedSubspace in preferencesInProducedSubspaces)
                {
                    if (preferencesInSingleProducedSubspace.SetEquals(preferencesInSingleExpectedSubspace))
                    {
                        expectedSubsetIsContainedInProducedSubpaces = true;
                    }
                }

                Assert.IsTrue(expectedSubsetIsContainedInProducedSubpaces,
                    string.Format("Expected subspace not produced: {0}.",
                        string.Join(", ", preferencesInSingleExpectedSubspace)));
            }
        }

        private HashSet<HashSet<int>> ExpectedSubspaces()
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
                    preferencesInSingleSubspaceExpected.Add(int.Parse(singleSubspaceExpectedDimension[0].ToString()));
                }
                preferencesInSubspacesExpected.Add(preferencesInSingleSubspaceExpected);
            }

            return preferencesInSubspacesExpected;
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SkylineSampleUtilityTests_Incorrect.xml",
            "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SkylineSampleUtilityTests_Incorrect.xml")]
        public void TestIncorrectSubspaceQueries()
        {
            var hasExceptionBeenRaised = false;

            var testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);

            var attributesCount = int.Parse(TestContext.DataRow["attributesCount"].ToString());
            var sampleCount = int.Parse(TestContext.DataRow["sampleCount"].ToString());
            var sampleDimension = int.Parse(TestContext.DataRow["sampleDimension"].ToString());

            var subjectUnderTest = new SkylineSampleUtility(attributesCount, sampleCount, sampleDimension);

            try
            {
                var subspaces = subjectUnderTest.Subspaces;
            }
            catch (Exception exception)
            {
                hasExceptionBeenRaised = true;
                Debug.WriteLine(exception.Message);
            }

            if (!hasExceptionBeenRaised)
            {
                Assert.Fail("Syntactically incorrect SQL Query should have thrown an Exception.");
            }
        }
    }
}