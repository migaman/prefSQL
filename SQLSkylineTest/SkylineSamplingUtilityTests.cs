namespace prefSQL.SQLSkylineTest
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using SQLSkyline;
    using SQLSkyline.SkylineSampling;

    [TestClass]
    public class SkylineSamplingUtilityTests
    {
        public TestContext TestContext { get; set; }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SkylineSamplingUtilityTests.xml", "TestDataRow",
            DataAccessMethod.Sequential),
         DeploymentItem("SkylineSamplingUtilityTests.xml")]
        public void TestProducedSubspaces()
        {
            string testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);

            int attributesCount = int.Parse(TestContext.DataRow["attributesCount"].ToString());
            int subspacesCount = int.Parse(TestContext.DataRow["subspacesCount"].ToString());
            int subspaceDimension = int.Parse(TestContext.DataRow["subspaceDimension"].ToString());

            var subjectUnderTest = new SkylineSamplingUtility
            {
                AllPreferencesCount = attributesCount,
                SubspacesCount = subspacesCount,
                SubspaceDimension = subspaceDimension
            };

            var preferencesInProducedSubspaces=new HashSet<HashSet<int>>();

            foreach (CLRSafeHashSet<int> subspace in subjectUnderTest.Subspaces)
            {
                preferencesInProducedSubspaces.Add(subspace.ToUnsafeForCLRHashSet());
            }

            HashSet<HashSet<int>> preferencesInExpectedSubspaces = ExpectedSubspaces();

            Assert.AreEqual(preferencesInExpectedSubspaces.Count, preferencesInProducedSubspaces.Count,
                "Number of expected subspaces is not equal to number of actual subspaces produced.");

            foreach (HashSet<int> preferencesInSingleExpectedSubspace in preferencesInExpectedSubspaces)
            {
                var expectedSubsetIsContainedInProducedSubpaces = false;
                foreach (HashSet<int> preferencesInSingleProducedSubspace in preferencesInProducedSubspaces)
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

            DataRow[] subspacesExpected =
                TestContext.DataRow.GetChildRows("TestDataRow_useSubspaces")[0].GetChildRows("useSubspaces_subspace");

            foreach (DataRow subspaceExpected in subspacesExpected)
            {
                DataRow[] subspaceExpectedDimensions = subspaceExpected.GetChildRows("subspace_dimension");
                var preferencesInSingleSubspaceExpected = new HashSet<int>();
                foreach (DataRow singleSubspaceExpectedDimension in subspaceExpectedDimensions)
                {
                    preferencesInSingleSubspaceExpected.Add(int.Parse(singleSubspaceExpectedDimension[0].ToString()));
                }
                preferencesInSubspacesExpected.Add(preferencesInSingleSubspaceExpected);
            }

            return preferencesInSubspacesExpected;
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SkylineSamplingUtilityTests_Incorrect.xml",
            "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SkylineSamplingUtilityTests_Incorrect.xml")]
        public void TestIncorrectSubspaceQueries()
        {
            var hasExceptionBeenRaised = false;

            string testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);

            int attributesCount = int.Parse(TestContext.DataRow["attributesCount"].ToString());
            int subspacesCount = int.Parse(TestContext.DataRow["subspacesCount"].ToString());
            int subspaceDimension = int.Parse(TestContext.DataRow["subspaceDimension"].ToString());

            var subjectUnderTest = new SkylineSamplingUtility
            {
                AllPreferencesCount = attributesCount,
                SubspacesCount = subspacesCount,
                SubspaceDimension = subspaceDimension
            };

            try
            {
                var subspaces = new HashSet<HashSet<int>>();

                foreach (CLRSafeHashSet<int> subspace in subjectUnderTest.Subspaces)
                {
                    subspaces.Add(subspace.ToUnsafeForCLRHashSet());
                }               
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

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML",
            "SkylineSamplingUtilityTests_BinomialCoefficient.xml", "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SkylineSamplingUtilityTests_BinomialCoefficient.xml")]
        public void TestBinomialCoefficient()
        {
            int n = int.Parse(TestContext.DataRow["n"].ToString());
            int[] coefficients = TestContext.DataRow["coefficients"].ToString().Split(',').Select(int.Parse).ToArray();

            for (var k = 0; k <= n; k++)
            {
                int expected = coefficients[k];
                int actual = SkylineSamplingUtility.BinomialCoefficient(n, k);
                Assert.AreEqual(expected, actual);
            }
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML",
            "SkylineSamplingUtilityTests_BinomialCoefficient.xml", "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SkylineSamplingUtilityTests_BinomialCoefficient.xml")]
        public void TestOutsideOfValidIntervalParametersForBinomialCoefficient()
        {
            int n = int.Parse(TestContext.DataRow["n"].ToString());
            int[] coefficients = TestContext.DataRow["coefficients"].ToString().Split(',').Select(int.Parse).ToArray();

            const int expected = 0;

            int actual = SkylineSamplingUtility.BinomialCoefficient(n, -1);
            Assert.AreEqual(expected, actual);

            actual = SkylineSamplingUtility.BinomialCoefficient(n, -2);
            Assert.AreEqual(expected, actual);

            actual = SkylineSamplingUtility.BinomialCoefficient(n, n + 1);
            Assert.AreEqual(expected, actual);

            actual = SkylineSamplingUtility.BinomialCoefficient(n, n + 2);
            Assert.AreEqual(expected, actual);
        }
    }
}