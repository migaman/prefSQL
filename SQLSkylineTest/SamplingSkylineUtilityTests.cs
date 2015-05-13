using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLSkyline.SamplingSkyline;

namespace prefSQL.SQLSkylineTest
{
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
            var testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);

            var attributesCount = int.Parse(TestContext.DataRow["attributesCount"].ToString());
            var subspacesCount = int.Parse(TestContext.DataRow["subspacesCount"].ToString());
            var subspaceDimension = int.Parse(TestContext.DataRow["subspaceDimension"].ToString());

            var subjectUnderTest = new SamplingSkylineUtility
            {
                AllPreferencesCount = attributesCount,
                SubspacesCount = subspacesCount,
                SubspaceDimension = subspaceDimension
            };

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
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SamplingSkylineUtilityTests_Incorrect.xml",
            "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SamplingSkylineUtilityTests_Incorrect.xml")]
        public void TestIncorrectSubspaceQueries()
        {
            var hasExceptionBeenRaised = false;

            var testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);

            var attributesCount = int.Parse(TestContext.DataRow["attributesCount"].ToString());
            var subspacesCount = int.Parse(TestContext.DataRow["subspacesCount"].ToString());
            var subspaceDimension = int.Parse(TestContext.DataRow["subspaceDimension"].ToString());

            var subjectUnderTest = new SamplingSkylineUtility
            {
                AllPreferencesCount = attributesCount,
                SubspacesCount = subspacesCount,
                SubspaceDimension = subspaceDimension
            };

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