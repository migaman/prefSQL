namespace prefSQL.SQLSkylineTest
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using SQLSkyline.SamplingSkyline;

    [TestClass]
    public class SamplingSkylineUtilityTests
    {
        public TestContext TestContext { get; set; }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SamplingSkylineUtilityTests.xml", "TestDataRow",
            DataAccessMethod.Sequential),
         DeploymentItem("SamplingSkylineUtilityTests.xml")]
        public void TestProducedSubspaces()
        {
            string testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);

            int attributesCount = int.Parse(TestContext.DataRow["attributesCount"].ToString());
            int subspacesCount = int.Parse(TestContext.DataRow["subspacesCount"].ToString());
            int subspaceDimension = int.Parse(TestContext.DataRow["subspaceDimension"].ToString());

            var subjectUnderTest = new SamplingSkylineUtility
            {
                AllPreferencesCount = attributesCount,
                SubspacesCount = subspacesCount,
                SubspaceDimension = subspaceDimension
            };

            HashSet<HashSet<int>> preferencesInProducedSubspaces = subjectUnderTest.Subspaces;
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
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SamplingSkylineUtilityTests_Incorrect.xml",
            "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SamplingSkylineUtilityTests_Incorrect.xml")]
        public void TestIncorrectSubspaceQueries()
        {
            var hasExceptionBeenRaised = false;

            string testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);

            int attributesCount = int.Parse(TestContext.DataRow["attributesCount"].ToString());
            int subspacesCount = int.Parse(TestContext.DataRow["subspacesCount"].ToString());
            int subspaceDimension = int.Parse(TestContext.DataRow["subspaceDimension"].ToString());

            var subjectUnderTest = new SamplingSkylineUtility
            {
                AllPreferencesCount = attributesCount,
                SubspacesCount = subspacesCount,
                SubspaceDimension = subspaceDimension
            };

            try
            {
                HashSet<HashSet<int>> subspaces = subjectUnderTest.Subspaces;
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
            "SamplingSkylineUtilityTests_BinomialCoefficient.xml", "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SamplingSkylineUtilityTests_BinomialCoefficient.xml")]
        public void TestBinomialCoefficient()
        {
            int n = int.Parse(TestContext.DataRow["n"].ToString());
            int[] coefficients = TestContext.DataRow["coefficients"].ToString().Split(',').Select(int.Parse).ToArray();

            for (var k = 0; k <= n; k++)
            {
                int expected = coefficients[k];
                int actual = SamplingSkylineUtility.BinomialCoefficient(n, k);
                Assert.AreEqual(expected, actual);
            }
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML",
            "SamplingSkylineUtilityTests_BinomialCoefficient.xml", "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SamplingSkylineUtilityTests_BinomialCoefficient.xml")]
        public void TestOutsideOfValidIntervalParametersForBinomialCoefficient()
        {
            int n = int.Parse(TestContext.DataRow["n"].ToString());
            int[] coefficients = TestContext.DataRow["coefficients"].ToString().Split(',').Select(int.Parse).ToArray();

            const int expected = 0;

            int actual = SamplingSkylineUtility.BinomialCoefficient(n, -1);
            Assert.AreEqual(expected, actual);

            actual = SamplingSkylineUtility.BinomialCoefficient(n, -2);
            Assert.AreEqual(expected, actual);

            actual = SamplingSkylineUtility.BinomialCoefficient(n, n + 1);
            Assert.AreEqual(expected, actual);

            actual = SamplingSkylineUtility.BinomialCoefficient(n, n + 2);
            Assert.AreEqual(expected, actual);
        }
    }
}