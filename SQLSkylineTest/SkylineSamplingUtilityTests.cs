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
        public void TestSamplingProducedSubsets()
        {
            string testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);

            int attributesCount = int.Parse(TestContext.DataRow["attributesCount"].ToString());
            int subsetsCount = int.Parse(TestContext.DataRow["subsetsCount"].ToString());
            int subsetDimension = int.Parse(TestContext.DataRow["subsetDimension"].ToString());

            var subjectUnderTest = new SkylineSamplingUtility
            {
                AllPreferencesCount = attributesCount,
                SubsetCount = subsetsCount,
                SubsetDimension = subsetDimension
            };

            var preferencesInProducedSubsets=new HashSet<HashSet<int>>();

            foreach (CLRSafeHashSet<int> subset in subjectUnderTest.Subsets)
            {
                preferencesInProducedSubsets.Add(subset.ToUnsafeForCLRHashSet());
            }

            HashSet<HashSet<int>> preferencesInExpectedSubsets = ExpectedSubsets();

            Assert.AreEqual(preferencesInExpectedSubsets.Count, preferencesInProducedSubsets.Count,
                "Number of expected subsets is not equal to number of actual subsets produced.");

            foreach (HashSet<int> preferencesInSingleExpectedSubset in preferencesInExpectedSubsets)
            {
                var expectedSubsetIsContainedInProducedSubsets = false;
                foreach (HashSet<int> preferencesInSingleProducedSubset in preferencesInProducedSubsets)
                {
                    if (preferencesInSingleProducedSubset.SetEquals(preferencesInSingleExpectedSubset))
                    {
                        expectedSubsetIsContainedInProducedSubsets = true;
                    }
                }

                Assert.IsTrue(expectedSubsetIsContainedInProducedSubsets,
                    string.Format("Expected subset not produced: {0}.",
                        string.Join(", ", preferencesInSingleExpectedSubset)));
            }
        }

        private HashSet<HashSet<int>> ExpectedSubsets()
        {
            var preferencesInSubsetsExpected = new HashSet<HashSet<int>>();

            DataRow[] subsetsExpected =
                TestContext.DataRow.GetChildRows("TestDataRow_useSubsets")[0].GetChildRows("useSubsets_subset");

            foreach (DataRow subsetExpected in subsetsExpected)
            {
                DataRow[] subsetExpectedDimensions = subsetExpected.GetChildRows("subset_dimension");
                var preferencesInSingleSubsetExpected = new HashSet<int>();
                foreach (DataRow singleSubsetExpectedDimension in subsetExpectedDimensions)
                {
                    preferencesInSingleSubsetExpected.Add(int.Parse(singleSubsetExpectedDimension[0].ToString()));
                }
                preferencesInSubsetsExpected.Add(preferencesInSingleSubsetExpected);
            }

            return preferencesInSubsetsExpected;
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SkylineSamplingUtilityTests_Incorrect.xml",
            "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SkylineSamplingUtilityTests_Incorrect.xml")]
        public void TestSamplingIncorrectSubsetQueries()
        {
            var hasExceptionBeenRaised = false;

            string testComment = TestContext.DataRow["comment"].ToString();
            Debug.WriteLine(testComment);

            int attributesCount = int.Parse(TestContext.DataRow["attributesCount"].ToString());
            int subsetsCount = int.Parse(TestContext.DataRow["subsetsCount"].ToString());
            int subsetDimension = int.Parse(TestContext.DataRow["subsetDimension"].ToString());

            var subjectUnderTest = new SkylineSamplingUtility
            {
                AllPreferencesCount = attributesCount,
                SubsetCount = subsetsCount,
                SubsetDimension = subsetDimension
            };

            try
            {
                var subsets = new HashSet<HashSet<int>>();

                foreach (CLRSafeHashSet<int> subset in subjectUnderTest.Subsets)
                {
                    subsets.Add(subset.ToUnsafeForCLRHashSet());
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
        public void TestSamplingBinomialCoefficient()
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
        public void TestSamplingOutsideOfValidIntervalParametersForBinomialCoefficient()
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