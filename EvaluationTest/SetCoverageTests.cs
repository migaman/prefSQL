namespace prefSQL.EvaluationTest
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Linq;
    using Evaluation;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class SetCoverageTests
    {
        public TestContext TestContext { get; set; }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SetCoverageTests_EuclideanDistance.xml",
            "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SetCoverageTests_EuclideanDistance.xml")]
        public void TestCalculateEuclideanDistance()
        {
            object[] item1 =
                TestContext.DataRow["item1"].ToString()
                    .Split(',')
                    .Select(item => Convert.ToDouble(item, CultureInfo.InvariantCulture))
                    .Select(item => (object) item)
                    .ToArray();
            object[] item2 =
                TestContext.DataRow["item2"].ToString()
                    .Split(',')
                    .Select(item => Convert.ToDouble(item, CultureInfo.InvariantCulture))
                    .Select(item => (object) item)
                    .ToArray();

            double expectedDistance = Convert.ToDouble(TestContext.DataRow["expectedDistance"].ToString(),
                CultureInfo.InvariantCulture);

            var useColumns = new int[item1.Length];

            for (var i = 0; i < useColumns.Length; i++)
            {
                useColumns[i] = i;
            }

            double actualDistance = SetCoverage.CalculateEuclideanDistance(item1, item2, useColumns);

            Assert.AreEqual(expectedDistance, actualDistance, 0.000000000001);
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SetCoverageTests_SetCoverage.xml",
            "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SetCoverageTests_SetCoverage.xml")]
        public void TestGetCoverage()
        {
            DataRow[] itemsToBeCovered =
               TestContext.DataRow.GetChildRows("TestDataRow_itemsToBeCovered")[0].GetChildRows("itemsToBeCovered_item");
            DataRow[] itemsCoveringDataToBeCovered =
              TestContext.DataRow.GetChildRows("TestDataRow_itemsCoveringDataToBeCovered")[0].GetChildRows("itemsCoveringDataToBeCovered_item");

            var normalizedDataToBeCovered = new Dictionary<long, object[]>();
            var normalizedDataCoveringDataToBeCovered= new Dictionary<long, object[]>();

            foreach (DataRow row in itemsToBeCovered)
            {
                object[] objectTemp = ((string)row[0]).Split(',')
                    .Select(item => Convert.ToDouble(item, CultureInfo.InvariantCulture))
                    .Select(item => (object) item)
                    .ToArray();
                normalizedDataToBeCovered.Add(((string)row[0]).GetHashCode(), objectTemp);
            }

            foreach (DataRow row in itemsCoveringDataToBeCovered)
            {
                object[] objectTemp = ((string)row[0]).Split(',')
                    .Select(item => Convert.ToDouble(item, CultureInfo.InvariantCulture))
                    .Select(item => (object)item)
                    .ToArray();
                normalizedDataCoveringDataToBeCovered.Add(((string)row[0]).GetHashCode(), objectTemp);
            }

            double expectedCoverage = Convert.ToDouble(TestContext.DataRow["expectedCoverage"].ToString(),
                CultureInfo.InvariantCulture);

            var useColumns = new int[((string)itemsToBeCovered[0][0]).Split(',').ToArray().Length];

            for (var i = 0; i < useColumns.Length; i++)
            {
                useColumns[i] = i;
            }

            double actualCoverage = SetCoverage.GetCoverage(normalizedDataToBeCovered, normalizedDataCoveringDataToBeCovered, useColumns);

            Assert.AreEqual(expectedCoverage, actualCoverage, 0.000000000001);
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SetCoverageTests_SetCoverage.xml",
            "TestDataRow", DataAccessMethod.Sequential),
         DeploymentItem("SetCoverageTests_SetCoverage.xml")]
        public void TestGetMaxDistanceOfCoveredObjectsToTheirCoveringObjects()
        {
            DataRow[] itemsToBeCovered =
               TestContext.DataRow.GetChildRows("TestDataRow_itemsToBeCovered")[0].GetChildRows("itemsToBeCovered_item");
            DataRow[] itemsCoveringDataToBeCovered =
              TestContext.DataRow.GetChildRows("TestDataRow_itemsCoveringDataToBeCovered")[0].GetChildRows("itemsCoveringDataToBeCovered_item");

            var normalizedDataToBeCovered = new Dictionary<long, object[]>();
            var normalizedDataCoveringDataToBeCovered = new Dictionary<long, object[]>();

            foreach (DataRow row in itemsToBeCovered)
            {
                object[] objectTemp = ((string)row[0]).Split(',')
                    .Select(item => Convert.ToDouble(item, CultureInfo.InvariantCulture))
                    .Select(item => (object)item)
                    .ToArray();
                normalizedDataToBeCovered.Add(((string)row[0]).GetHashCode(), objectTemp);
            }

            foreach (DataRow row in itemsCoveringDataToBeCovered)
            {
                object[] objectTemp = ((string)row[0]).Split(',')
                    .Select(item => Convert.ToDouble(item, CultureInfo.InvariantCulture))
                    .Select(item => (object)item)
                    .ToArray();
                normalizedDataCoveringDataToBeCovered.Add(((string)row[0]).GetHashCode(), objectTemp);
            }

            double expectedRepresentationError = Convert.ToDouble(TestContext.DataRow["expectedRepresentationError"].ToString(),
                CultureInfo.InvariantCulture);

            var useColumns = new int[((string)itemsToBeCovered[0][0]).Split(',').ToArray().Length];

            for (var i = 0; i < useColumns.Length; i++)
            {
                useColumns[i] = i;
            }

            double actualRepresentationError = SetCoverage.GetRepresentationError(normalizedDataToBeCovered, normalizedDataCoveringDataToBeCovered, useColumns).Max();

            Assert.AreEqual(expectedRepresentationError, actualRepresentationError, 0.000000000001);
        }
    }
}