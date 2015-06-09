namespace Utility
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Numerics;
    using System.Text;
    using prefSQL.Evaluation;
    using prefSQL.SQLParser;
    using prefSQL.SQLParser.Models;
    using prefSQL.SQLParserTest;
    using prefSQL.SQLSkyline;
    using prefSQL.SQLSkyline.SkylineSampling;

    internal sealed class PerformanceSampling
    {
        private enum ClusterAnalysis
        {
            EntireDb,
            EntireSkyline,
            SampleSkyline,
            BestRank,
            SumRank
        }

        private enum Reports
        {
            TimeMin,
            TimeMax,
            TimeVar,
            TimeStdDev,
            TimeMed,
            TimeQ1,
            TimeQ3,
            SizeMin,
            SizeMax,
            SizeVar,
            SizeStdDev,
            SizeMed,
            SizeQ1,
            SizeQ3,
        }

        private enum SkylineTypes
        {
            RandomAvg,
            RandomMin,
            RandomMax,
            RandomVar,
            RandomStdDev,
            RandomMed,
            RandomQ1,
            RandomQ3,
            SampleAvg,
            SampleMin,
            SampleMax,
            SampleVar,
            SampleStdDev,
            SampleMed,
            SampleQ1,
            SampleQ3,
            BestRankAvg,
            BestRankMin,
            BestRankMax,
            BestRankVar,
            BestRankStdDev,
            BestRankMed,
            BestRankQ1,
            BestRankQ3,
            SumRankAvg,
            SumRankMin,
            SumRankMax,
            SumRankVar,
            SumRankStdDev,
            SumRankMed,
            SumRankQ1,
            SumRankQ3
        }

        private enum SkylineTypesSingle
        {
            Random,
            Sample,
            BestRank,
            SumRank
        }

        private static readonly Mathematic MyMathematic = new Mathematic();
        private Dictionary<ClusterAnalysis, List<List<double>>> _clusterAnalysis;
        private Dictionary<ClusterAnalysis, List<List<double>>> _clusterAnalysisMedian;
        private Dictionary<ClusterAnalysis, Dictionary<BigInteger, List<double>>> _clusterAnalysisMedianTopBuckets;
        private Dictionary<ClusterAnalysis, Dictionary<BigInteger, List<double>>> _clusterAnalysisTopBuckets;
        private Dictionary<SkylineTypes, List<double>> _dominatedObjectsCount;
        private Dictionary<SkylineTypes, List<double>> _dominatedObjectsOfBestObject;
        private Dictionary<Reports, List<double>> _reportsDouble;
        private Dictionary<Reports, List<long>> _reportsLong;
        private Dictionary<SkylineTypes, List<double>> _representationError;
        private Dictionary<SkylineTypes, List<double>> _representationErrorSum;
        private Dictionary<SkylineTypes, List<double>> _setCoverage;
        private bool _excessiveTests = true;
        internal int SubsetsCount { get; set; }
        internal int SubsetDimension { get; set; }
        internal int SamplesCount { get; set; }

        public PerformanceSampling(int subsetsCount, int subsetDimension, int samplesCount, bool excessiveTests)
        {
            SubsetsCount = subsetsCount;
            SubsetDimension = subsetDimension;
            SamplesCount = samplesCount;
            ExcessiveTests = excessiveTests;

            InitSamplingDataStructures();
        }

        public bool ExcessiveTests
        {
            get { return _excessiveTests; }
            set { _excessiveTests = value; }
        }

        internal string MeasurePerformance(int iTrial, int iPreferenceIndex, ArrayList listPreferences,
            ArrayList preferences,
            SQLCommon parser, Stopwatch sw, List<long> reportDimensions, List<long> reportSkylineSize,
            List<long> reportTimeTotal,
            List<long> reportTimeAlgorithm, List<double> reportCorrelation, double correlation,
            List<double> reportCardinality, double cardinality,
            string strSQL, string strPreferenceSet, string strTrial)
        {
            Dictionary<ClusterAnalysis, List<List<double>>> clusterAnalysis;
            Dictionary<ClusterAnalysis, List<List<double>>> clusterAnalysisMedian;
            Dictionary<ClusterAnalysis, Dictionary<BigInteger, List<double>>> clusterAnalysisTopBuckets;
            Dictionary<ClusterAnalysis, Dictionary<BigInteger, List<double>>> clusterAnalysisMedianTopBuckets;

            List<IEnumerable<CLRSafeHashSet<int>>> producedSubsets =
                ProduceSubsets(preferences);

            InitClusterAnalysisDataStructures(out clusterAnalysis);
            InitClusterAnalysisDataStructures(out clusterAnalysisMedian);
            InitClusterAnalysisTopBucketsDataStructures(
                out clusterAnalysisTopBuckets);
            InitClusterAnalysisTopBucketsDataStructures(
                out clusterAnalysisMedianTopBuckets);

            var entireSkylineDataTable = new DataTable();
            if (ExcessiveTests)
            {
                entireSkylineDataTable =
                parser.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName,
                    strSQL);
            }
            else
            {
                entireSkylineDataTable =
                parser.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName,
                    strSQL+ " SAMPLE BY RANDOM_SUBSETS COUNT " + SubsetsCount +
                      " DIMENSION " + SubsetDimension);
            }
            
            List<long[]> entireDataTableSkylineValues =
                parser.SkylineType.Strategy.SkylineValues;

            int[] skylineAttributeColumns =
                SkylineSamplingHelper.GetSkylineAttributeColumns(entireSkylineDataTable);

            IReadOnlyDictionary<long, object[]> entireSkylineDatabase =
                prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(
                    entireSkylineDataTable, 0);
            IReadOnlyDictionary<long, object[]> entireSkylineNormalized =
                prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(
                    entireSkylineDataTable, 0);
            SkylineSamplingHelper.NormalizeColumns(entireSkylineNormalized,
                skylineAttributeColumns);

            DataTable entireDataTable;
            IReadOnlyDictionary<long, object[]> entireDatabaseNormalized =
                GetEntireDatabaseNormalized(parser, strSQL, skylineAttributeColumns,
                    out entireDataTable);
            IReadOnlyDictionary<long, object[]> entireDatabase =
                prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(
                    entireDataTable, 0);

            IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                entireDatabaseBuckets =
                    prefSQL.Evaluation.ClusterAnalysis.GetBuckets(entireDatabaseNormalized,
                        skylineAttributeColumns);

            IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                aggregatedEntireDatabaseBuckets =
                    prefSQL.Evaluation.ClusterAnalysis.GetAggregatedBuckets(entireDatabaseBuckets);

            foreach (
                KeyValuePair<BigInteger, List<IReadOnlyDictionary<long, object[]>>> s in
                    entireDatabaseBuckets.OrderByDescending(l => l.Value.Count)
                        .ThenBy(l => l.Key).Take(5))
            {
                double percent = (double) s.Value.Count / entireDatabaseNormalized.Count;
                clusterAnalysisTopBuckets[ClusterAnalysis.EntireDb].Add(s.Key,
                    new List<double>());

                for (var i = 0; i < producedSubsets.Count; i++)
                    // to enable generalized average calculation
                {
                    clusterAnalysisTopBuckets[ClusterAnalysis.EntireDb][s.Key]
                        .Add(percent);
                }
            }

            IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                entireSkylineBuckets =
                    prefSQL.Evaluation.ClusterAnalysis.GetBuckets(entireSkylineNormalized,
                        skylineAttributeColumns);

            IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                aggregatedEntireSkylineBuckets =
                    prefSQL.Evaluation.ClusterAnalysis.GetAggregatedBuckets(entireSkylineBuckets);

            FillTopBuckets(clusterAnalysisTopBuckets,
                ClusterAnalysis.EntireSkyline, entireSkylineBuckets,
                entireSkylineNormalized.Count, entireDatabaseNormalized.Count,
                entireSkylineNormalized.Count);
            foreach (
                KeyValuePair<BigInteger, List<double>> bucket in
                    clusterAnalysisTopBuckets[ClusterAnalysis.EntireSkyline])
            {
                double percent =
                    clusterAnalysisTopBuckets[ClusterAnalysis.EntireSkyline][
                        bucket.Key][0];

                for (var i = 1; i < producedSubsets.Count; i++)
                    // to enable generalized average calculation
                {
                    clusterAnalysisTopBuckets[ClusterAnalysis.EntireSkyline][
                        bucket.Key].Add(percent);
                }
            }

            var clusterAnalysisForMedian = new prefSQL.Evaluation.ClusterAnalysis(entireDatabaseNormalized,
                skylineAttributeColumns);

            IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                entireDatabaseMedianBuckets =
                    clusterAnalysisForMedian.GetBuckets(entireDatabaseNormalized,
                        skylineAttributeColumns, true);

            IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                aggregatedEntireDatabaseMedianBuckets =
                    prefSQL.Evaluation.ClusterAnalysis.GetAggregatedBuckets(entireDatabaseMedianBuckets);

            foreach (
                KeyValuePair<BigInteger, List<IReadOnlyDictionary<long, object[]>>> s in
                    entireDatabaseMedianBuckets.OrderByDescending(l => l.Value.Count)
                        .ThenBy(l => l.Key).Take(5))
            {
                double percent = (double) s.Value.Count / entireDatabaseNormalized.Count;
                clusterAnalysisMedianTopBuckets[ClusterAnalysis.EntireDb].Add(
                    s.Key,
                    new List<double>());

                for (var i = 0; i < producedSubsets.Count; i++)
                    // to enable generalized average calculation
                {
                    clusterAnalysisMedianTopBuckets[ClusterAnalysis.EntireDb][
                        s.Key]
                        .Add(percent);
                }
            }

            IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                entireSkylineMedianBuckets =
                    clusterAnalysisForMedian.GetBuckets(entireSkylineNormalized,
                        skylineAttributeColumns, true);

            IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                aggregatedEntireSkylineMedianBuckets =
                    prefSQL.Evaluation.ClusterAnalysis.GetAggregatedBuckets(entireSkylineMedianBuckets);

            FillTopBuckets(clusterAnalysisMedianTopBuckets,
                ClusterAnalysis.EntireSkyline, entireSkylineMedianBuckets,
                entireSkylineNormalized.Count, entireDatabaseNormalized.Count,
                entireSkylineNormalized.Count);

            foreach (
                KeyValuePair<BigInteger, List<double>> bucket in
                    clusterAnalysisMedianTopBuckets[
                        ClusterAnalysis.EntireSkyline])
            {
                double percent =
                    clusterAnalysisMedianTopBuckets[
                        ClusterAnalysis.EntireSkyline][bucket.Key][0];

                for (var i = 1; i < producedSubsets.Count; i++)
                    // to enable generalized average calculation
                {
                    clusterAnalysisMedianTopBuckets[
                        ClusterAnalysis.EntireSkyline][bucket.Key].Add(percent);
                }
            }
            strSQL += " SAMPLE BY RANDOM_SUBSETS COUNT " + SubsetsCount +
                      " DIMENSION " + SubsetDimension;

            string strQuery;
            string operators;
            int numberOfRecords;
            string[] parameter;

            PrefSQLModel prefSqlModel = parser.GetPrefSqlModelFromPreferenceSql(strSQL);
            string ansiSql = parser.GetAnsiSqlFromPrefSqlModel(prefSqlModel);
            prefSQL.SQLParser.Helper.DetermineParameters(ansiSql, out parameter,
                out strQuery, out operators,
                out numberOfRecords);

            var subsetObjects = new List<long>();
            var subsetTime = new List<long>();
            var subsetTimeElapsed = new List<long>();
            var setCoverageSecondRandom = new List<double>();
            var setCoverageSample = new List<double>();
            var setCoverageBestRank = new List<double>();
            var setCoverageSumRank = new List<double>();

            var representationErrorSecondRandom = new List<double>();
            var representationErrorSample = new List<double>();
            var representationErrorBestRank = new List<double>();
            var representationErrorSumRank = new List<double>();

            var representationErrorSumSecondRandom = new List<double>();
            var representationErrorSumSample = new List<double>();
            var representationErrorSumBestRank = new List<double>();
            var representationErrorSumSumRank = new List<double>();

            var dominatedObjectsCountSecondRandom = new List<double>();
            var dominatedObjectsCountSample = new List<double>();
            var dominatedObjectsCountBestRank = new List<double>();
            var dominatedObjectsCountSumRank = new List<double>();

            var dominatedObjectsOfBestObjectSecondRandom = new List<double>();
            var dominatedObjectsOfBestObjectSample = new List<double>();
            var dominatedObjectsOfBestObjectBestRank = new List<double>();
            var dominatedObjectsOfBestObjectSumRank = new List<double>();

            var subsetCount = 1;
            foreach (IEnumerable<CLRSafeHashSet<int>> subset in producedSubsets)
            {
                Console.WriteLine(strPreferenceSet + " (" + subsetCount + " / " +
                                  producedSubsets.Count + ")");

                sw.Restart();
                var subsetsProducer = new FixedSkylineSamplingSubsetsProducer(subset);
                var utility = new SkylineSamplingUtility(subsetsProducer);
                var skylineSample = new SkylineSampling(utility)
                {
                    SubsetCount = prefSqlModel.SkylineSampleCount,
                    SubsetDimension = prefSqlModel.SkylineSampleDimension,
                    SelectedStrategy = parser.SkylineType
                };

                DataTable sampleSkylineDataTable = skylineSample.GetSkylineTable(strQuery,
                    operators);

                sw.Stop();

                subsetObjects.Add(sampleSkylineDataTable.Rows.Count);
                subsetTime.Add(skylineSample.TimeMilliseconds);
                subsetTimeElapsed.Add(sw.ElapsedMilliseconds);

                IReadOnlyDictionary<long, object[]> sampleSkylineDatabase =
                    prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(
                        sampleSkylineDataTable, 0);
                IReadOnlyDictionary<long, object[]> sampleSkylineNormalized =
                    prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(
                        sampleSkylineDataTable, 0);
                SkylineSamplingHelper.NormalizeColumns(sampleSkylineNormalized,
                    skylineAttributeColumns);

                IReadOnlyDictionary<long, object[]> secondRandomSampleDatabase = new Dictionary<long, object[]>();
                IReadOnlyDictionary<long, object[]> secondRandomSampleNormalized = new Dictionary<long, object[]>();
                      IReadOnlyDictionary<long, object[]> entireSkylineDataTableBestRankDatabase = new Dictionary<long, object[]>();
                IReadOnlyDictionary<long, object[]> entireSkylineDataTableSumRankDatabase = new Dictionary<long, object[]>();
                IReadOnlyDictionary<long, object[]> entireSkylineDataTableBestRankNormalized = new Dictionary<long, object[]>();
                IReadOnlyDictionary<long, object[]> entireSkylineDataTableSumRankNormalized = new Dictionary<long, object[]>();

               if (ExcessiveTests)
                {
               secondRandomSampleDatabase =
                    SkylineSamplingHelper.GetRandomSample(entireSkylineDatabase,
                        sampleSkylineDataTable.Rows.Count);
                var secondRandomSampleNormalizedToBeCreated = new Dictionary<long, object[]>();
                foreach (KeyValuePair<long, object[]> k in secondRandomSampleDatabase)
                {
                    var newValue = new object[k.Value.Length];
                    k.Value.CopyTo(newValue, 0);
                    secondRandomSampleNormalizedToBeCreated.Add(k.Key, newValue);
                }
                secondRandomSampleNormalized =
                    new ReadOnlyDictionary<long, object[]>(
                        secondRandomSampleNormalizedToBeCreated);
                SkylineSamplingHelper.NormalizeColumns(secondRandomSampleNormalized,
                    skylineAttributeColumns);

                    entireSkylineDataTableBestRankNormalized =
                        GetEntireSkylineDataTableRankNormalized(entireSkylineDataTable.Copy(),
                            entireDataTableSkylineValues, skylineAttributeColumns,
                            sampleSkylineDataTable.Rows.Count, 1,
                            out entireSkylineDataTableBestRankDatabase);

                    entireSkylineDataTableSumRankNormalized =
                        GetEntireSkylineDataTableRankNormalized(entireSkylineDataTable.Copy(),
                            entireDataTableSkylineValues, skylineAttributeColumns,
                            sampleSkylineDataTable.Rows.Count, 2,
                            out entireSkylineDataTableSumRankDatabase);
              
                var tempList = new List<List<double>>
                {
                    new List<double>(),
                    new List<double>(),
                    new List<double>(),
                    new List<double>()
                };

                    for (var ii = 0; ii < 100; ii++)
                    {
                        IReadOnlyDictionary<long, object[]> baseRandomSampleNormalized =
                            SkylineSamplingHelper.GetRandomSample(entireSkylineNormalized,
                                sampleSkylineDataTable.Rows.Count);

                        var tempList2 = new List<double>();

                        for (var jj = 0; jj < 100; jj++)
                        {
                            tempList2.Add(SetCoverage.GetCoverage(
                                baseRandomSampleNormalized,
                                secondRandomSampleNormalized, skylineAttributeColumns) * 100.0);
                        }

                        tempList[0].Add(tempList2.Average());

                        tempList[1].Add(SetCoverage.GetCoverage(
                            baseRandomSampleNormalized,
                            sampleSkylineNormalized, skylineAttributeColumns) * 100.0);

                        tempList[2].Add(SetCoverage.GetCoverage(
                            baseRandomSampleNormalized,
                            sampleSkylineNormalized, skylineAttributeColumns) * 100.0);

                        tempList[3].Add(SetCoverage.GetCoverage(baseRandomSampleNormalized,
                            entireSkylineDataTableSumRankNormalized, skylineAttributeColumns) *
                                        100.0);
                    }

                    double setCoverageCoveredBySecondRandomSample = tempList[0].Average();
                    double setCoverageCoveredBySkylineSample = tempList[1].Average();
                    double setCoverageCoveredByEntireBestRank = tempList[2].Average();
                    double setCoverageCoveredByEntireSumRank = tempList[3].Average();

                    setCoverageSecondRandom.Add(setCoverageCoveredBySecondRandomSample);
                    setCoverageSample.Add(setCoverageCoveredBySkylineSample);
                    setCoverageBestRank.Add(setCoverageCoveredByEntireBestRank);
                    setCoverageSumRank.Add(setCoverageCoveredByEntireSumRank);

                    double representationErrorSecondRandomSample = SetCoverage
                        .GetRepresentationError(
                            GetReducedSkyline(entireSkylineNormalized, secondRandomSampleNormalized),
                            secondRandomSampleNormalized, skylineAttributeColumns).Max() * 100.0;
                    double representationErrorSkylineSample = SetCoverage.GetRepresentationError(
                        GetReducedSkyline(entireSkylineNormalized, sampleSkylineNormalized),
                        sampleSkylineNormalized, skylineAttributeColumns).Max() * 100.0;
                    double representationErrorEntireBestRank =
                        SetCoverage.GetRepresentationError(
                            GetReducedSkyline(entireSkylineNormalized,
                                entireSkylineDataTableBestRankNormalized),
                            entireSkylineDataTableBestRankNormalized, skylineAttributeColumns).Max() *
                        100.0;
                    double representationErrorEntireSumRank =
                        SetCoverage.GetRepresentationError(
                            GetReducedSkyline(entireSkylineNormalized,
                                entireSkylineDataTableSumRankNormalized),
                            entireSkylineDataTableSumRankNormalized, skylineAttributeColumns).Max() *
                        100.0;

                    representationErrorSecondRandom.Add(representationErrorSecondRandomSample);
                    representationErrorSample.Add(representationErrorSkylineSample);
                    representationErrorBestRank.Add(representationErrorEntireBestRank);
                    representationErrorSumRank.Add(representationErrorEntireSumRank);

                    double representationErrorSumSecondRandomSample = SetCoverage
                        .GetRepresentationError(
                            GetReducedSkyline(entireSkylineNormalized, secondRandomSampleNormalized),
                            secondRandomSampleNormalized, skylineAttributeColumns).Sum() * 100.0;
                    double representationErrorSumSkylineSample = SetCoverage.GetRepresentationError(
                        GetReducedSkyline(entireSkylineNormalized, sampleSkylineNormalized),
                        sampleSkylineNormalized, skylineAttributeColumns).Sum() * 100.0;
                    double representationErrorSumEntireBestRank =
                        SetCoverage.GetRepresentationError(
                            GetReducedSkyline(entireSkylineNormalized,
                                entireSkylineDataTableBestRankNormalized),
                            entireSkylineDataTableBestRankNormalized, skylineAttributeColumns).Sum() *
                        100.0;
                    double representationErrorSumEntireSumRank =
                        SetCoverage.GetRepresentationError(
                            GetReducedSkyline(entireSkylineNormalized,
                                entireSkylineDataTableSumRankNormalized),
                            entireSkylineDataTableSumRankNormalized, skylineAttributeColumns)
                            .Sum() * 100.0;

                    representationErrorSumSecondRandom.Add(
                        representationErrorSumSecondRandomSample);
                    representationErrorSumSample.Add(representationErrorSumSkylineSample);
                    representationErrorSumBestRank.Add(representationErrorSumEntireBestRank);
                    representationErrorSumSumRank.Add(representationErrorSumEntireSumRank);

                    var dominatedObjectsCountRandomSample =
                        new DominatedObjects(entireDatabase,
                            secondRandomSampleDatabase,
                            skylineAttributeColumns);
                    var dominatedObjectsCountSampleSkyline =
                        new DominatedObjects(entireDatabase, sampleSkylineDatabase,
                            skylineAttributeColumns);
                    var dominatedObjectsCountEntireSkylineBestRank =
                        new DominatedObjects(entireDatabase,
                            entireSkylineDataTableBestRankDatabase, skylineAttributeColumns);
                    var dominatedObjectsCountEntireSkylineSumRank =
                        new DominatedObjects(entireDatabase,
                            entireSkylineDataTableSumRankDatabase, skylineAttributeColumns);

                    dominatedObjectsCountSecondRandom.Add(
                        dominatedObjectsCountRandomSample.NumberOfDistinctDominatedObjects);
                    dominatedObjectsCountSample.Add(
                        dominatedObjectsCountSampleSkyline.NumberOfDistinctDominatedObjects);
                    dominatedObjectsCountBestRank.Add(
                        dominatedObjectsCountEntireSkylineBestRank.NumberOfDistinctDominatedObjects);
                    dominatedObjectsCountSumRank.Add(
                        dominatedObjectsCountEntireSkylineSumRank.NumberOfDistinctDominatedObjects);

                    dominatedObjectsOfBestObjectSecondRandom.Add(
                        dominatedObjectsCountRandomSample
                            .NumberOfObjectsDominatedByEachObjectOrderedByDescCount.First().Value);
                    dominatedObjectsOfBestObjectSample.Add(
                        dominatedObjectsCountSampleSkyline
                            .NumberOfObjectsDominatedByEachObjectOrderedByDescCount.First().Value);
                    dominatedObjectsOfBestObjectBestRank.Add(
                        dominatedObjectsCountEntireSkylineBestRank
                            .NumberOfObjectsDominatedByEachObjectOrderedByDescCount.First().Value);
                    dominatedObjectsOfBestObjectSumRank.Add(
                        dominatedObjectsCountEntireSkylineSumRank
                            .NumberOfObjectsDominatedByEachObjectOrderedByDescCount.First().Value);

                    IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                        sampleBuckets =
                            prefSQL.Evaluation.ClusterAnalysis.GetBuckets(sampleSkylineNormalized,
                                skylineAttributeColumns);
                    IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                        aggregatedSampleBuckets =
                            prefSQL.Evaluation.ClusterAnalysis.GetAggregatedBuckets(sampleBuckets);
                    IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                        bestRankBuckets =
                            prefSQL.Evaluation.ClusterAnalysis.GetBuckets(
                                entireSkylineDataTableBestRankNormalized,
                                skylineAttributeColumns);
                    IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                        aggregatedBestRankBuckets =
                            prefSQL.Evaluation.ClusterAnalysis.GetAggregatedBuckets(
                                bestRankBuckets);
                    IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                        sumRankBuckets =
                            prefSQL.Evaluation.ClusterAnalysis.GetBuckets(
                                entireSkylineDataTableSumRankNormalized,
                                skylineAttributeColumns);
                    IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                        aggregatedSumRankBuckets =
                            prefSQL.Evaluation.ClusterAnalysis.GetAggregatedBuckets(
                                sumRankBuckets);

                    FillTopBuckets(clusterAnalysisTopBuckets,
                        ClusterAnalysis.SampleSkyline, sampleBuckets,
                        sampleSkylineNormalized.Count, entireDatabaseNormalized.Count,
                        entireSkylineNormalized.Count);
                    FillTopBuckets(clusterAnalysisTopBuckets,
                        ClusterAnalysis.BestRank, bestRankBuckets,
                        entireSkylineDataTableBestRankNormalized.Count,
                        entireDatabaseNormalized.Count, entireSkylineNormalized.Count);
                    FillTopBuckets(clusterAnalysisTopBuckets,
                        ClusterAnalysis.SumRank, sumRankBuckets,
                        entireSkylineDataTableSumRankNormalized.Count,
                        entireDatabaseNormalized.Count, entireSkylineNormalized.Count);

                    IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                        sampleMedianBuckets =
                            clusterAnalysisForMedian.GetBuckets(sampleSkylineNormalized,
                                skylineAttributeColumns, true);
                    IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                        aggregatedSampleMedianBuckets =
                            prefSQL.Evaluation.ClusterAnalysis.GetAggregatedBuckets(sampleMedianBuckets);
                    IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                        bestRankMedianBuckets =
                            clusterAnalysisForMedian.GetBuckets(
                                entireSkylineDataTableBestRankNormalized,
                                skylineAttributeColumns, true);
                    IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                        aggregatedBestRankMedianBuckets =
                            prefSQL.Evaluation.ClusterAnalysis.GetAggregatedBuckets(
                                bestRankMedianBuckets);
                    IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                        sumRankMedianBuckets =
                            clusterAnalysisForMedian.GetBuckets(
                                entireSkylineDataTableSumRankNormalized,
                                skylineAttributeColumns, true);
                    IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                        aggregatedSumRankMedianBuckets =
                            prefSQL.Evaluation.ClusterAnalysis.GetAggregatedBuckets(
                                sumRankMedianBuckets);

                    FillTopBuckets(clusterAnalysisMedianTopBuckets,
                        ClusterAnalysis.SampleSkyline, sampleMedianBuckets,
                        sampleSkylineNormalized.Count, entireDatabaseNormalized.Count,
                        entireSkylineNormalized.Count);
                    FillTopBuckets(clusterAnalysisMedianTopBuckets,
                        ClusterAnalysis.BestRank, bestRankMedianBuckets,
                        entireSkylineDataTableBestRankNormalized.Count,
                        entireDatabaseNormalized.Count, entireSkylineNormalized.Count);
                    FillTopBuckets(clusterAnalysisMedianTopBuckets,
                        ClusterAnalysis.SumRank, sumRankMedianBuckets,
                        entireSkylineDataTableSumRankNormalized.Count,
                        entireDatabaseNormalized.Count, entireSkylineNormalized.Count);

                    var caEntireDbNew = new List<double>();
                    var caEntireSkylineNew = new List<double>();
                    var caSampleSkylineNew = new List<double>();
                    var caBestRankNew = new List<double>();
                    var caSumRankNew = new List<double>();

                    for (var ii = 0; ii < skylineAttributeColumns.Length; ii++)
                    {
                        int entireSkyline = aggregatedEntireSkylineBuckets.ContainsKey(ii)
                            ? aggregatedEntireSkylineBuckets[ii].Count
                            : 0;
                        int sampleSkyline = aggregatedSampleBuckets.ContainsKey(ii)
                            ? aggregatedSampleBuckets[ii].Count
                            : 0;
                        double entireSkylinePercent = (double) entireSkyline /
                                                      entireSkylineNormalized.Count;
                        double sampleSkylinePercent = (double) sampleSkyline /
                                                      sampleSkylineNormalized.Count;
                        int entireDb = aggregatedEntireDatabaseBuckets.ContainsKey(ii)
                            ? aggregatedEntireDatabaseBuckets[ii].Count
                            : 0;
                        double entireDbPercent = (double) entireDb /
                                                 entireDatabaseNormalized.Count;

                        int bestRank = aggregatedBestRankBuckets.ContainsKey(ii)
                            ? aggregatedBestRankBuckets[ii].Count
                            : 0;
                        int sumRank = aggregatedSumRankBuckets.ContainsKey(ii)
                            ? aggregatedSumRankBuckets[ii].Count
                            : 0;

                        double bestRankPercent = (double) bestRank /
                                                 entireSkylineDataTableBestRankNormalized.Count;
                        double sumRankPercent = (double) sumRank /
                                                entireSkylineDataTableSumRankNormalized.Count;
                        caEntireDbNew.Add(entireDbPercent);
                        caEntireSkylineNew.Add(entireSkylinePercent);
                        caSampleSkylineNew.Add(sampleSkylinePercent);
                        caBestRankNew.Add(bestRankPercent);
                        caSumRankNew.Add(sumRankPercent);
                    }

                    var caMedianEntireDbNew = new List<double>();
                    var caMedianEntireSkylineNew = new List<double>();
                    var caMedianSampleSkylineNew = new List<double>();
                    var caMedianBestRankNew = new List<double>();
                    var caMedianSumRankNew = new List<double>();

                    for (var ii = 0; ii < skylineAttributeColumns.Length; ii++)
                    {
                        int entireSkyline = aggregatedEntireSkylineMedianBuckets.ContainsKey(ii)
                            ? aggregatedEntireSkylineMedianBuckets[ii].Count
                            : 0;
                        int sampleSkyline = aggregatedSampleMedianBuckets.ContainsKey(ii)
                            ? aggregatedSampleMedianBuckets[ii].Count
                            : 0;
                        double entireSkylinePercent = (double) entireSkyline /
                                                      entireSkylineNormalized.Count;
                        double sampleSkylinePercent = (double) sampleSkyline /
                                                      sampleSkylineNormalized.Count;
                        int entireDb = aggregatedEntireDatabaseMedianBuckets.ContainsKey(ii)
                            ? aggregatedEntireDatabaseMedianBuckets[ii].Count
                            : 0;
                        double entireDbPercent = (double) entireDb /
                                                 entireDatabaseNormalized.Count;

                        int bestRank = aggregatedBestRankMedianBuckets.ContainsKey(ii)
                            ? aggregatedBestRankMedianBuckets[ii].Count
                            : 0;
                        int sumRank = aggregatedSumRankMedianBuckets.ContainsKey(ii)
                            ? aggregatedSumRankMedianBuckets[ii].Count
                            : 0;

                        double bestRankPercent = (double) bestRank /
                                                 entireSkylineDataTableBestRankNormalized.Count;
                        double sumRankPercent = (double) sumRank /
                                                entireSkylineDataTableSumRankNormalized.Count;
                        caMedianEntireDbNew.Add(entireDbPercent);
                        caMedianEntireSkylineNew.Add(entireSkylinePercent);
                        caMedianSampleSkylineNew.Add(sampleSkylinePercent);
                        caMedianBestRankNew.Add(bestRankPercent);
                        caMedianSumRankNew.Add(sumRankPercent);
                    }

                clusterAnalysis[ClusterAnalysis.EntireDb].Add(caEntireDbNew);
                clusterAnalysis[ClusterAnalysis.EntireSkyline].Add(
                    caEntireSkylineNew);
                clusterAnalysis[ClusterAnalysis.SampleSkyline].Add(
                    caSampleSkylineNew);
                clusterAnalysis[ClusterAnalysis.BestRank].Add(
                    caBestRankNew);
                clusterAnalysis[ClusterAnalysis.SumRank].Add(
                    caSumRankNew);

                clusterAnalysisMedian[ClusterAnalysis.EntireDb].Add(
                    caMedianEntireDbNew);
                clusterAnalysisMedian[ClusterAnalysis.EntireSkyline].Add(
                    caMedianEntireSkylineNew);
                clusterAnalysisMedian[ClusterAnalysis.SampleSkyline].Add(
                    caMedianSampleSkylineNew);
                clusterAnalysisMedian[ClusterAnalysis.BestRank].Add(
                    caMedianBestRankNew);
                clusterAnalysisMedian[ClusterAnalysis.SumRank].Add(
                    caMedianSumRankNew);
                }

                subsetCount++;
            }

            Dictionary<ClusterAnalysis, string> clusterAnalysisStrings =
                GetClusterAnalysisStrings(skylineAttributeColumns, clusterAnalysis);
            Dictionary<ClusterAnalysis, string> clusterAnalysisMedianStrings =
                GetClusterAnalysisStrings(skylineAttributeColumns, clusterAnalysisMedian);
            Dictionary<ClusterAnalysis, string> clusterAnalysisTopBucketsStrings =
                GetClusterAnalysisTopBucketsStrings(clusterAnalysisTopBuckets, ExcessiveTests);
            Dictionary<ClusterAnalysis, string> clusterAnalysisMedianTopBucketsStrings =
                GetClusterAnalysisTopBucketsStrings(clusterAnalysisMedianTopBuckets, ExcessiveTests);

            var time = (long) (subsetTime.Average() + .5);
            var objects = (long) (subsetObjects.Average() + .5);
            var elapsed = (long) (subsetTimeElapsed.Average() + .5);

            Console.WriteLine("subsetTime");
            foreach (var i in subsetTime)
            {
                Console.WriteLine(i);
            }
            Console.WriteLine("");

            Console.WriteLine("subsetObjects");
            foreach (var i in subsetObjects)
            {
                Console.WriteLine(i);
            }
            Console.WriteLine("");

            reportDimensions.Add(preferences.Count);
            reportSkylineSize.Add(objects);
            reportTimeTotal.Add(elapsed);
            reportTimeAlgorithm.Add(time);
            reportCorrelation.Add(correlation);
            reportCardinality.Add(cardinality);

            var setCoverageSingle =
                new Dictionary<SkylineTypesSingle, List<double>>
                {
                    {SkylineTypesSingle.Random, setCoverageSecondRandom},
                    {SkylineTypesSingle.Sample, setCoverageSample},
                    {SkylineTypesSingle.BestRank, setCoverageBestRank},
                    {SkylineTypesSingle.SumRank, setCoverageSumRank}
                };

            var representationErrorSingle =
                new Dictionary<SkylineTypesSingle, List<double>>
                {
                    {SkylineTypesSingle.Random, representationErrorSecondRandom},
                    {SkylineTypesSingle.Sample, representationErrorSample},
                    {SkylineTypesSingle.BestRank, representationErrorBestRank},
                    {SkylineTypesSingle.SumRank, representationErrorSumRank}
                };

            var representationErrorSumSingle =
                new Dictionary<SkylineTypesSingle, List<double>>
                {
                    {SkylineTypesSingle.Random, representationErrorSumSecondRandom},
                    {SkylineTypesSingle.Sample, representationErrorSumSample},
                    {SkylineTypesSingle.BestRank, representationErrorSumBestRank},
                    {SkylineTypesSingle.SumRank, representationErrorSumSumRank}
                };

            var dominatedObjectsCountSingle =
                new Dictionary<SkylineTypesSingle, List<double>>()
                {
                    {
                        SkylineTypesSingle.Random,
                        dominatedObjectsCountSecondRandom
                    },
                    {
                        SkylineTypesSingle.Sample,
                        dominatedObjectsCountSample
                    },
                    {
                        SkylineTypesSingle.BestRank,
                        dominatedObjectsCountBestRank
                    },
                    {
                        SkylineTypesSingle.SumRank,
                        dominatedObjectsCountSumRank
                    }
                };

            var dominatedObjectsOfBestObjectSingle =
                new Dictionary<SkylineTypesSingle, List<double>>
                {
                    {
                        SkylineTypesSingle.Random,
                        dominatedObjectsOfBestObjectSecondRandom
                    },
                    {
                        SkylineTypesSingle.Sample,
                        dominatedObjectsOfBestObjectSample
                    },
                    {
                        SkylineTypesSingle.BestRank,
                        dominatedObjectsOfBestObjectBestRank
                    },
                    {
                        SkylineTypesSingle.SumRank,
                        dominatedObjectsOfBestObjectSumRank
                    }
                };

            AddToReports(_reportsLong, subsetObjects, subsetTime,
                _reportsDouble);
            if (ExcessiveTests)
            {
            AddToSetCoverage(_setCoverage, setCoverageSingle);
            AddToSetCoverage(_representationError,
                representationErrorSingle);
            AddToSetCoverage(_representationErrorSum,
                representationErrorSumSingle);
            AddToSetCoverage(_dominatedObjectsCount,
                dominatedObjectsCountSingle);
            AddToSetCoverage(_dominatedObjectsOfBestObject,
                dominatedObjectsOfBestObjectSingle);
            }

            string strLine = FormatLineString(strPreferenceSet, strTrial,
                preferences.Count, objects,
                elapsed, time, subsetTime.Min(), subsetTime.Max(),
                MyMathematic.GetSampleVariance(subsetTime),
                MyMathematic.GetSampleStdDeviation(subsetTime),
                Mathematic.Median(subsetTime), Mathematic.LowerQuartile(subsetTime),
                Mathematic.UpperQuartile(subsetTime), subsetObjects.Min(),
                subsetObjects.Max(), MyMathematic.GetSampleVariance(subsetObjects),
                MyMathematic.GetSampleStdDeviation(subsetObjects),
                Mathematic.Median(subsetObjects), Mathematic.LowerQuartile(subsetObjects),
                Mathematic.UpperQuartile(subsetObjects),
                setCoverageSingle, representationErrorSingle,
                representationErrorSumSingle, dominatedObjectsCountSingle,
                dominatedObjectsOfBestObjectSingle,
                clusterAnalysisStrings, clusterAnalysisMedianStrings,
                clusterAnalysisTopBucketsStrings, clusterAnalysisMedianTopBucketsStrings,
                correlation,
                cardinality);
            return strLine;
        }

        internal static string GetSeparatorLine(bool excessiveTests)
        {
            return FormatLineString('-', "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
                new Dictionary<SkylineTypesSingle, List<double>>(), new Dictionary<SkylineTypesSingle, List<double>>(),
                new Dictionary<SkylineTypesSingle, List<double>>(), new Dictionary<SkylineTypesSingle, List<double>>(),
                new Dictionary<SkylineTypesSingle, List<double>>(), new Dictionary<ClusterAnalysis, string>(),
                new Dictionary<ClusterAnalysis, string>(), new Dictionary<ClusterAnalysis, string>(),
                new Dictionary<ClusterAnalysis, string>(), "", "",excessiveTests);
        }

        private static string FormatLineString(char paddingChar, string strTitle, string strTrial,
            string strDimension, string strSkyline, string strTimeTotal, string strTimeAlgo, string minTime,
            string maxTime, string varianceTime, string stdDevTime, string medTime, string q1Time, string q3Time,
            string minSize, string maxSize, string varianceSize,
            string stdDevSize, string medSize, string q1Size, string q3Size, string[] setCoverageSampling,
            string[] representationErrorSampling,
            string[] representationErrorSumSampling,
            string[] dominatedObjectsCountSampling,
            string[] dominatedObjectsOfBestObjectSampling, string[] clusterAnalysisStrings,
            string[] clusterAnalysisMedianStrings, string[] clusterAnalysisTopBucketsStrings,
            string[] clusterAnalysisMedianTopBucketsStrings, string strCorrelation,
            string strCardinality)
        {
            var sb = new StringBuilder();

            sb.Append(strTitle.PadLeft(19, paddingChar));
            sb.Append("|");
            sb.Append(strTrial.PadLeft(11, paddingChar));
            sb.Append("|");
            sb.Append(strDimension.PadLeft(10, paddingChar));
            sb.Append("|");
            sb.Append(strSkyline.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strTimeTotal.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strTimeAlgo.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(minTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(maxTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(varianceTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(stdDevTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(medTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(q1Time.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(q3Time.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(minSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(maxSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(varianceSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(stdDevSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(medSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(q1Size.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(q3Size.PadLeft(20, paddingChar));
            sb.Append("|");
            foreach (string s in setCoverageSampling)
            {
                sb.Append(s.PadLeft(20, paddingChar));
                sb.Append("|");
            }
            foreach (string s in representationErrorSampling)
            {
                sb.Append(s.PadLeft(20, paddingChar));
                sb.Append("|");
            }
            foreach (string s in representationErrorSumSampling)
            {
                sb.Append(s.PadLeft(20, paddingChar));
                sb.Append("|");
            }
            foreach (string s in dominatedObjectsCountSampling)
            {
                sb.Append(s.PadLeft(20, paddingChar));
                sb.Append("|");
            }
            foreach (string s in dominatedObjectsOfBestObjectSampling)
            {
                sb.Append(s.PadLeft(20, paddingChar));
                sb.Append("|");
            }
            foreach (string s in clusterAnalysisStrings)
            {
                sb.Append(s.PadLeft(130, paddingChar));
                sb.Append("|");
            }
            foreach (string s in clusterAnalysisMedianStrings)
            {
                sb.Append(s.PadLeft(130, paddingChar));
                sb.Append("|");
            }
            foreach (string s in clusterAnalysisTopBucketsStrings)
            {
                sb.Append(s.PadLeft(250, paddingChar));
                sb.Append("|");
            }
            foreach (string s in clusterAnalysisMedianTopBucketsStrings)
            {
                sb.Append(s.PadLeft(250, paddingChar));
                sb.Append("|");
            }
            sb.Append(strCorrelation.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strCardinality.PadLeft(25, paddingChar));
            sb.Append("|");

            return sb.ToString();
        }

        private static string FormatLineString(char paddingChar, string strTitle, string strTrial, string strDimension,
            string strSkyline, string strTimeTotal, string strTimeAlgo, string minTime, string maxTime,
            string varianceTime, string stdDevTime, string medTime, string q1Time, string q3Time, string minSize,
            string maxSize, string varianceSize, string stdDevSize, string medSize, string q1Size, string q3Size,
            Dictionary<SkylineTypesSingle, List<double>> setCoverageSampling,
            Dictionary<SkylineTypesSingle, List<double>> representationErrorSampling,
            Dictionary<SkylineTypesSingle, List<double>> representationErrorSumSampling,
            Dictionary<SkylineTypesSingle, List<double>> dominatedObjectsCountSampling,
            Dictionary<SkylineTypesSingle, List<double>> dominatedObjectsOfBestObjectSampling,
            Dictionary<ClusterAnalysis, string> clusterAnalysisStrings,
            Dictionary<ClusterAnalysis, string> clusterAnalysisMedianStrings,
            Dictionary<ClusterAnalysis, string> clusterAnalysisTopBucketsStrings,
            Dictionary<ClusterAnalysis, string> clusterAnalysisMedianTopBucketsStrings, string strCorrelation,
            string strCardinality, bool excessiveTests)
        {
            var sb = new StringBuilder();

            sb.Append(strTitle.PadLeft(19, paddingChar));
            sb.Append("|");
            sb.Append(strTrial.PadLeft(11, paddingChar));
            sb.Append("|");
            sb.Append(strDimension.PadLeft(10, paddingChar));
            sb.Append("|");
            sb.Append(strSkyline.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strTimeTotal.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strTimeAlgo.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(minTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(maxTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(varianceTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(stdDevTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(medTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(q1Time.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(q3Time.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(minSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(maxSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(varianceSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(stdDevSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(medSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(q1Size.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(q3Size.PadLeft(20, paddingChar));
            sb.Append("|");
            if (excessiveTests)
            {
                AppendSetCoverageValues(sb, paddingChar, setCoverageSampling);
                AppendSetCoverageValues(sb, paddingChar, representationErrorSampling);
                AppendSetCoverageValues(sb, paddingChar, representationErrorSumSampling);
                AppendSetCoverageValues(sb, paddingChar, dominatedObjectsCountSampling);
                AppendSetCoverageValues(sb, paddingChar, dominatedObjectsOfBestObjectSampling);
            }
            else
            {
                AppendSetCoverageValues(sb, paddingChar, new Dictionary<SkylineTypesSingle, List<double>>());
                AppendSetCoverageValues(sb, paddingChar, new Dictionary<SkylineTypesSingle, List<double>>());
                AppendSetCoverageValues(sb, paddingChar, new Dictionary<SkylineTypesSingle, List<double>>());
                AppendSetCoverageValues(sb, paddingChar, new Dictionary<SkylineTypesSingle, List<double>>());
                AppendSetCoverageValues(sb, paddingChar, new Dictionary<SkylineTypesSingle, List<double>>());
            }
            AppendClusterAnalysisValues(sb, paddingChar, 130, clusterAnalysisStrings);
               AppendClusterAnalysisValues(sb, paddingChar, 130, clusterAnalysisMedianStrings);
                AppendClusterAnalysisValues(sb, paddingChar, 250, clusterAnalysisTopBucketsStrings);
                AppendClusterAnalysisValues(sb, paddingChar, 250, clusterAnalysisMedianTopBucketsStrings);
            sb.Append(strCorrelation.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strCardinality.PadLeft(25, paddingChar));
            sb.Append("|");

            return sb.ToString();
        }

        private static void AppendSetCoverageValues(StringBuilder sb, char paddingChar,
            Dictionary<SkylineTypesSingle, List<double>> setCoverageSampling)
        {
            if (setCoverageSampling.Count == 0)
            {
                for (var i = 0; i < 32; i++)
                {
                    sb.Append("".PadLeft(20, paddingChar));
                    sb.Append("|");
                }

                return;
            }

            foreach (
                SkylineTypesSingle skylineTypesSingleSamplingType in
                    Enum.GetValues(typeof (SkylineTypesSingle)).Cast<SkylineTypesSingle>())
            {
                sb.Append(
                    Math.Round(setCoverageSampling[skylineTypesSingleSamplingType].Average(), 2)
                        .ToString(CultureInfo.InvariantCulture)
                        .PadLeft(20, paddingChar));
                sb.Append("|");
                sb.Append(
                    Math.Round(setCoverageSampling[skylineTypesSingleSamplingType].Min(), 2)
                        .ToString(CultureInfo.InvariantCulture)
                        .PadLeft(20, paddingChar));
                sb.Append("|");
                sb.Append(
                    Math.Round(setCoverageSampling[skylineTypesSingleSamplingType].Max(), 2)
                        .ToString(CultureInfo.InvariantCulture)
                        .PadLeft(20, paddingChar));
                sb.Append("|");
                sb.Append(
                    Math.Round(MyMathematic.GetSampleVariance(setCoverageSampling[skylineTypesSingleSamplingType]), 2)
                        .ToString(CultureInfo.InvariantCulture)
                        .PadLeft(20, paddingChar));
                sb.Append("|");
                sb.Append(
                    Math.Round(MyMathematic.GetSampleStdDeviation(setCoverageSampling[skylineTypesSingleSamplingType]),
                        2)
                        .ToString(CultureInfo.InvariantCulture)
                        .PadLeft(20, paddingChar));
                sb.Append("|");
                sb.Append(
                    Math.Round(Mathematic.Median(setCoverageSampling[skylineTypesSingleSamplingType]), 2)
                        .ToString(CultureInfo.InvariantCulture)
                        .PadLeft(20, paddingChar));
                sb.Append("|");
                sb.Append(
                    Math.Round(Mathematic.LowerQuartile(setCoverageSampling[skylineTypesSingleSamplingType]), 2)
                        .ToString(CultureInfo.InvariantCulture)
                        .PadLeft(20, paddingChar));
                sb.Append("|");
                sb.Append(
                    Math.Round(Mathematic.UpperQuartile(setCoverageSampling[skylineTypesSingleSamplingType]), 2)
                        .ToString(CultureInfo.InvariantCulture)
                        .PadLeft(20, paddingChar));
                sb.Append("|");
            }
        }

        private static void AppendClusterAnalysisValues(StringBuilder sb, char paddingChar, int paddingCount,
            Dictionary<ClusterAnalysis, string> clusterAnalysisStrings)
        {
            if (clusterAnalysisStrings.Count == 0)
            {
                for (var i = 0; i < 5; i++)
                {
                    sb.Append("".PadLeft(paddingCount, paddingChar));
                    sb.Append("|");
                }

                return;
            }

            sb.Append(clusterAnalysisStrings[ClusterAnalysis.EntireDb].PadLeft(paddingCount, paddingChar));
            sb.Append("|");
            sb.Append(clusterAnalysisStrings[ClusterAnalysis.EntireSkyline].PadLeft(paddingCount, paddingChar));
            sb.Append("|");
            sb.Append(clusterAnalysisStrings[ClusterAnalysis.SampleSkyline].PadLeft(paddingCount, paddingChar));
            sb.Append("|");
            sb.Append(clusterAnalysisStrings[ClusterAnalysis.BestRank].PadLeft(paddingCount, paddingChar));
            sb.Append("|");
            sb.Append(clusterAnalysisStrings[ClusterAnalysis.SumRank].PadLeft(paddingCount, paddingChar));
            sb.Append("|");
        }

        private string FormatLineString(string strTitle, string strTrial, double dimension, double skyline,
            double timeTotal, double timeAlgo, double minTime, double maxTime, double varianceTime,
            double stddeviationTime, double medTime, double q1Time, double q3Time, double minSize, double maxSize,
            double varianceSize, double stddeviationSize, double medSize, double q1Size, double q3Size,
            Dictionary<SkylineTypesSingle, List<double>> setCoverageSampling,
            Dictionary<SkylineTypesSingle, List<double>> representationErrorSampling,
            Dictionary<SkylineTypesSingle, List<double>> representationErrorSumSampling,
            Dictionary<SkylineTypesSingle, List<double>> dominatedObjectsCountSampling,
            Dictionary<SkylineTypesSingle, List<double>> dominatedObjectsOfBestObjectSampling,
            Dictionary<ClusterAnalysis, string> clusterAnalysisStrings,
            Dictionary<ClusterAnalysis, string> clusterAnalysisMedianStrings,
            Dictionary<ClusterAnalysis, string> clusterAnalysisTopBucketsStrings,
            Dictionary<ClusterAnalysis, string> clusterAnalysisMedianTopBucketsStrings, double correlation,
            double cardinality)
        {
            return FormatLineString(' ', strTitle, strTrial,
                Math.Round(dimension, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(skyline, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(timeTotal, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(timeAlgo, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(minTime, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(maxTime, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(varianceTime, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(stddeviationTime, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(medTime, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(q1Time, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(q3Time, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(minSize, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(maxSize, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(varianceSize, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(stddeviationSize, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(medSize, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(q1Size, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(q3Size, 2).ToString(CultureInfo.InvariantCulture), setCoverageSampling,
                representationErrorSampling, representationErrorSumSampling, dominatedObjectsCountSampling,
                dominatedObjectsOfBestObjectSampling, clusterAnalysisStrings, clusterAnalysisMedianStrings,
                clusterAnalysisTopBucketsStrings, clusterAnalysisMedianTopBucketsStrings,
                Math.Round(correlation, 2).ToString(CultureInfo.InvariantCulture),
                Performance.ToLongString(Math.Round(cardinality, 2)), ExcessiveTests);
        }

        private string FormatLineString(string strTitle, string strTrial, double dimension, double skyline,
            double timeTotal, double timeAlgo, double minTime, double maxTime, double varianceTime,
            double stddeviationTime, double medTime, double q1Time, double q3Time, double minSize, double maxSize,
            double varianceSize, double stddeviationSize, double medSize, double q1Size, double q3Size,
            string[] setCoverageSampling, string[] representationErrorSampling, string[] representationErrorSumSampling,
            string[] dominatedObjectsCountSampling, string[] dominatedObjectsOfBestObjectSampling,
            string[] clusterAnalysisStrings, string[] clusterAnalysisMedianStrings,
            string[] clusterAnalysisTopBucketsStrings, string[] clusterAnalysisMedianTopBucketsStrings,
            double correlation, double cardinality)
        {
            return FormatLineString(' ', strTitle, strTrial,
                Math.Round(dimension, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(skyline, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(timeTotal, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(timeAlgo, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(minTime, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(maxTime, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(varianceTime, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(stddeviationTime, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(medTime, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(q1Time, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(q3Time, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(minSize, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(maxSize, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(varianceSize, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(stddeviationSize, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(medSize, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(q1Size, 2).ToString(CultureInfo.InvariantCulture),
                Math.Round(q3Size, 2).ToString(CultureInfo.InvariantCulture), setCoverageSampling,
                representationErrorSampling, representationErrorSumSampling, dominatedObjectsCountSampling,
                dominatedObjectsOfBestObjectSampling, clusterAnalysisStrings, clusterAnalysisMedianStrings,
                clusterAnalysisTopBucketsStrings, clusterAnalysisMedianTopBucketsStrings,
                Math.Round(correlation, 2).ToString(CultureInfo.InvariantCulture),
                Performance.ToLongString(Math.Round(cardinality, 2)));
        }

        private static void AddToSetCoverage(IReadOnlyDictionary<SkylineTypes, List<double>> setCoverageSampling,
            IReadOnlyDictionary<SkylineTypesSingle, List<double>> setCoverageSingleSampling)
        {
            setCoverageSampling[SkylineTypes.RandomAvg].Add(
                setCoverageSingleSampling[SkylineTypesSingle.Random].Average());
            setCoverageSampling[SkylineTypes.RandomMin].Add(setCoverageSingleSampling[SkylineTypesSingle.Random].Min());
            setCoverageSampling[SkylineTypes.RandomMax].Add(setCoverageSingleSampling[SkylineTypesSingle.Random].Max());
            setCoverageSampling[SkylineTypes.RandomVar].Add(
                MyMathematic.GetSampleVariance(setCoverageSingleSampling[SkylineTypesSingle.Random]));
            setCoverageSampling[SkylineTypes.RandomStdDev].Add(
                MyMathematic.GetSampleStdDeviation(setCoverageSingleSampling[SkylineTypesSingle.Random]));
            setCoverageSampling[SkylineTypes.RandomMed].Add(
                Mathematic.Median(setCoverageSingleSampling[SkylineTypesSingle.Random]));
            setCoverageSampling[SkylineTypes.RandomQ1].Add(
                Mathematic.LowerQuartile(setCoverageSingleSampling[SkylineTypesSingle.Random]));
            setCoverageSampling[SkylineTypes.RandomQ3].Add(
                Mathematic.UpperQuartile(setCoverageSingleSampling[SkylineTypesSingle.Random]));

            setCoverageSampling[SkylineTypes.SampleAvg].Add(
                setCoverageSingleSampling[SkylineTypesSingle.Sample].Average());
            setCoverageSampling[SkylineTypes.SampleMin].Add(setCoverageSingleSampling[SkylineTypesSingle.Sample].Min());
            setCoverageSampling[SkylineTypes.SampleMax].Add(setCoverageSingleSampling[SkylineTypesSingle.Sample].Max());
            setCoverageSampling[SkylineTypes.SampleVar].Add(
                MyMathematic.GetSampleVariance(setCoverageSingleSampling[SkylineTypesSingle.Sample]));
            setCoverageSampling[SkylineTypes.SampleStdDev].Add(
                MyMathematic.GetSampleStdDeviation(setCoverageSingleSampling[SkylineTypesSingle.Sample]));
            setCoverageSampling[SkylineTypes.SampleMed].Add(
                Mathematic.Median(setCoverageSingleSampling[SkylineTypesSingle.Sample]));
            setCoverageSampling[SkylineTypes.SampleQ1].Add(
                Mathematic.LowerQuartile(setCoverageSingleSampling[SkylineTypesSingle.Sample]));
            setCoverageSampling[SkylineTypes.SampleQ3].Add(
                Mathematic.UpperQuartile(setCoverageSingleSampling[SkylineTypesSingle.Sample]));

            setCoverageSampling[SkylineTypes.BestRankAvg].Add(
                setCoverageSingleSampling[SkylineTypesSingle.BestRank].Average());
            setCoverageSampling[SkylineTypes.BestRankMin].Add(
                setCoverageSingleSampling[SkylineTypesSingle.BestRank].Min());
            setCoverageSampling[SkylineTypes.BestRankMax].Add(
                setCoverageSingleSampling[SkylineTypesSingle.BestRank].Max());
            setCoverageSampling[SkylineTypes.BestRankVar].Add(
                MyMathematic.GetSampleVariance(setCoverageSingleSampling[SkylineTypesSingle.BestRank]));
            setCoverageSampling[SkylineTypes.BestRankStdDev].Add(
                MyMathematic.GetSampleStdDeviation(setCoverageSingleSampling[SkylineTypesSingle.BestRank]));
            setCoverageSampling[SkylineTypes.BestRankMed].Add(
                Mathematic.Median(setCoverageSingleSampling[SkylineTypesSingle.BestRank]));
            setCoverageSampling[SkylineTypes.BestRankQ1].Add(
                Mathematic.LowerQuartile(setCoverageSingleSampling[SkylineTypesSingle.BestRank]));
            setCoverageSampling[SkylineTypes.BestRankQ3].Add(
                Mathematic.UpperQuartile(setCoverageSingleSampling[SkylineTypesSingle.BestRank]));

            setCoverageSampling[SkylineTypes.SumRankAvg].Add(
                setCoverageSingleSampling[SkylineTypesSingle.SumRank].Average());
            setCoverageSampling[SkylineTypes.SumRankMin].Add(setCoverageSingleSampling[SkylineTypesSingle.SumRank].Min());
            setCoverageSampling[SkylineTypes.SumRankMax].Add(setCoverageSingleSampling[SkylineTypesSingle.SumRank].Max());
            setCoverageSampling[SkylineTypes.SumRankVar].Add(
                MyMathematic.GetSampleVariance(setCoverageSingleSampling[SkylineTypesSingle.SumRank]));
            setCoverageSampling[SkylineTypes.SumRankStdDev].Add(
                MyMathematic.GetSampleStdDeviation(setCoverageSingleSampling[SkylineTypesSingle.SumRank]));
            setCoverageSampling[SkylineTypes.SumRankMed].Add(
                Mathematic.Median(setCoverageSingleSampling[SkylineTypesSingle.SumRank]));
            setCoverageSampling[SkylineTypes.SumRankQ1].Add(
                Mathematic.LowerQuartile(setCoverageSingleSampling[SkylineTypesSingle.SumRank]));
            setCoverageSampling[SkylineTypes.SumRankQ3].Add(
                Mathematic.UpperQuartile(setCoverageSingleSampling[SkylineTypesSingle.SumRank]));
        }

        private static void AddToReports(IReadOnlyDictionary<Reports, List<long>> reportsSamplingLong,
            List<long> subsetObjects, List<long> subsetTime,
            IReadOnlyDictionary<Reports, List<double>> reportsSamplingDouble)
        {
            reportsSamplingLong[Reports.SizeMin].Add(subsetObjects.Min());
            reportsSamplingLong[Reports.TimeMin].Add(subsetTime.Min());
            reportsSamplingLong[Reports.SizeMax].Add(subsetObjects.Max());
            reportsSamplingLong[Reports.TimeMax].Add(subsetTime.Max());
            reportsSamplingLong[Reports.SizeMed].Add(Mathematic.Median(subsetObjects));
            reportsSamplingLong[Reports.TimeMed].Add(Mathematic.Median(subsetTime));
            reportsSamplingLong[Reports.SizeQ1].Add(Mathematic.LowerQuartile(subsetObjects));
            reportsSamplingLong[Reports.TimeQ1].Add(Mathematic.LowerQuartile(subsetTime));
            reportsSamplingLong[Reports.SizeQ3].Add(Mathematic.UpperQuartile(subsetObjects));
            reportsSamplingLong[Reports.TimeQ3].Add(Mathematic.UpperQuartile(subsetTime));
            reportsSamplingDouble[Reports.SizeVar].Add(MyMathematic.GetSampleVariance(subsetObjects));
            reportsSamplingDouble[Reports.TimeVar].Add(MyMathematic.GetSampleVariance(subsetTime));
            reportsSamplingDouble[Reports.SizeStdDev].Add(MyMathematic.GetSampleStdDeviation(subsetObjects));
            reportsSamplingDouble[Reports.TimeStdDev].Add(MyMathematic.GetSampleStdDeviation(subsetTime));
        }

        private static void InitClusterAnalysisDataStructures(
            out Dictionary<ClusterAnalysis, List<List<double>>> clusterAnalysisSampling)
        {
            clusterAnalysisSampling = new Dictionary<ClusterAnalysis, List<List<double>>>()
            {
                {ClusterAnalysis.EntireDb, new List<List<double>>()},
                {ClusterAnalysis.EntireSkyline, new List<List<double>>()},
                {ClusterAnalysis.SampleSkyline, new List<List<double>>()},
                {ClusterAnalysis.BestRank, new List<List<double>>()},
                {ClusterAnalysis.SumRank, new List<List<double>>()}
            };
        }

        private static void InitClusterAnalysisTopBucketsDataStructures(
            out Dictionary<ClusterAnalysis, Dictionary<BigInteger, List<double>>> clusterAnalysisTopBucketsSampling)
        {
            clusterAnalysisTopBucketsSampling = new Dictionary<ClusterAnalysis, Dictionary<BigInteger, List<double>>>()
            {
                {ClusterAnalysis.EntireDb, new Dictionary<BigInteger, List<double>>()},
                {ClusterAnalysis.EntireSkyline, new Dictionary<BigInteger, List<double>>()},
                {ClusterAnalysis.SampleSkyline, new Dictionary<BigInteger, List<double>>()},
                {ClusterAnalysis.BestRank, new Dictionary<BigInteger, List<double>>()},
                {ClusterAnalysis.SumRank, new Dictionary<BigInteger, List<double>>()}
            };
        }

        private void InitSamplingDataStructures()
        {
            _reportsLong = new Dictionary<Reports, List<long>>
            {
                {Reports.SizeMin, new List<long>()},
                {Reports.TimeMin, new List<long>()},
                {Reports.SizeMax, new List<long>()},
                {Reports.TimeMax, new List<long>()},
                {Reports.SizeMed, new List<long>()},
                {Reports.TimeMed, new List<long>()},
                {Reports.SizeQ1, new List<long>()},
                {Reports.TimeQ1, new List<long>()},
                {Reports.SizeQ3, new List<long>()},
                {Reports.TimeQ3, new List<long>()}
            };
            _reportsDouble = new Dictionary<Reports, List<double>>
            {
                {Reports.SizeVar, new List<double>()},
                {Reports.TimeVar, new List<double>()},
                {Reports.SizeStdDev, new List<double>()},
                {Reports.TimeStdDev, new List<double>()}
            };

            _setCoverage = new Dictionary<SkylineTypes, List<double>>();
            _representationError = new Dictionary<SkylineTypes, List<double>>();
            _representationErrorSum = new Dictionary<SkylineTypes, List<double>>();
            _dominatedObjectsCount = new Dictionary<SkylineTypes, List<double>>();
            _dominatedObjectsOfBestObject = new Dictionary<SkylineTypes, List<double>>();

            foreach (
                SkylineTypes skylineTypesSamplingType in
                    Enum.GetValues(typeof (SkylineTypes)).Cast<SkylineTypes>())
            {
                _setCoverage.Add(skylineTypesSamplingType, new List<double>());
            }

            foreach (
                SkylineTypes skylineTypesSamplingType in
                    Enum.GetValues(typeof (SkylineTypes)).Cast<SkylineTypes>())
            {
                _representationError.Add(skylineTypesSamplingType, new List<double>());
            }

            foreach (
                SkylineTypes skylineTypesSamplingType in
                    Enum.GetValues(typeof (SkylineTypes)).Cast<SkylineTypes>())
            {
                _representationErrorSum.Add(skylineTypesSamplingType, new List<double>());
            }

            foreach (
                SkylineTypes skylineTypesSamplingType in
                    Enum.GetValues(typeof (SkylineTypes)).Cast<SkylineTypes>())
            {
                _dominatedObjectsCount.Add(skylineTypesSamplingType, new List<double>());
            }

            foreach (
                SkylineTypes skylineTypesSamplingType in
                    Enum.GetValues(typeof (SkylineTypes)).Cast<SkylineTypes>())
            {
                _dominatedObjectsOfBestObject.Add(skylineTypesSamplingType, new List<double>());
            }

            InitClusterAnalysisDataStructures(out _clusterAnalysis);
            InitClusterAnalysisDataStructures(out _clusterAnalysisMedian);
            InitClusterAnalysisTopBucketsDataStructures(out _clusterAnalysisTopBuckets);
            InitClusterAnalysisTopBucketsDataStructures(out _clusterAnalysisMedianTopBuckets);
        }

        internal static string GetHeaderLine()
        {
            return FormatLineString(' ', "preference set", "trial", "dimensions", "avg skyline size", "avg time total",
                "avg time algorithm", "min time", "max time", "variance time", "stddeviation time", "median time",
                "q1 time", "q3 time", "min size", "max size", "variance size", "stddeviation size", "median size",
                "q1 size", "q3 size",
                new[]
                {
                    "avg sc random", "min sc random", "max sc random", "var sc random", "stddev sc random",
                    "med sc random",
                    "q1 sc random", "q3 sc random", "avg sc sample", "min sc sample", "max sc sample", "var sc sample",
                    "stddev sc sample", "med sc sample", "q1 sc sample", "q3 sc sample", "avg sc Best", "min sc Best",
                    "max sc Best", "var sc Best", "stddev sc Best", "med sc Best", "q1 sc Best", "q3 sc Best",
                    "avg sc Sum", "min sc Sum", "max sc Sum", "var sc Sum", "stddev sc Sum", "med sc Sum", "q1 sc Sum",
                    "q3 sc Sum"
                },
                new[]
                {
                    "avg re random", "min re random", "max re random", "var re random", "stddev re random",
                    "med re random",
                    "q1 re random", "q3 re random", "avg re sample", "min re sample", "max re sample", "var re sample",
                    "stddev re sample", "med re sample", "q1 re sample", "q3 re sample", "avg re Best", "min re Best",
                    "max re Best", "var re Best", "stddev re Best", "med re Best", "q1 re Best", "q3 re Best",
                    "avg re Sum", "min re Sum", "max re Sum", "var re Sum", "stddev re Sum", "med re Sum", "q1 re Sum",
                    "q3 re Sum"
                },
                new[]
                {
                    "avg reSum random", "min reSum random", "max reSum random", "var reSum random",
                    "stddev reSum random",
                    "med reSum random", "q1 reSum random", "q3 reSum random", "avg reSum sample", "min reSum sample",
                    "max reSum sample", "var reSum sample", "stddev reSum sample", "med reSum sample", "q1 reSum sample",
                    "q3 reSum sample", "avg reSum Best", "min reSum Best", "max reSum Best", "var reSum Best",
                    "stddev reSum Best", "med reSum Best", "q1 reSum Best", "q3 reSum Best", "avg reSum Sum",
                    "min reSum Sum", "max reSum Sum", "var reSum Sum", "stddev reSum Sum", "med reSum Sum",
                    "q1 reSum Sum", "q3 reSum Sum"
                },
                new[]
                {
                    "avg domCnt random", "min domCnt random", "max domCnt random", "var domCnt random",
                    "stddev domCnt random", "med domCnt random", "q1 domCnt random", "q3 domCnt random",
                    "avg domCnt sample", "min domCnt sample", "max domCnt sample", "var domCnt sample",
                    "stddev domCnt sample", "med domCnt sample", "q1 domCnt sample", "q3 domCnt sample",
                    "avg domCnt Best", "min domCnt Best", "max domCnt Best", "var domCnt Best", "stddev domCnt Best",
                    "med domCnt Best", "q1 domCnt Best", "q3 domCnt Best", "avg domCnt Sum", "min domCnt Sum",
                    "max domCnt Sum", "var domCnt Sum", "stddev domCnt Sum", "med domCnt Sum", "q1 domCnt Sum",
                    "q3 domCnt Sum"
                },
                new[]
                {
                    "avg domBst random", "min domBst random", "max domBst random", "var domBst random",
                    "stddev domBst random", "med domBst random", "q1 domBst random", "q3 domBst random",
                    "avg domBst sample", "min domBst sample", "max domBst sample", "var domBst sample",
                    "stddev domBst sample", "med domBst sample", "q1 domBst sample", "q3 domBst sample",
                    "avg domBst Best", "min domBst Best", "max domBst Best", "var domBst Best", "stddev domBst Best",
                    "med domBst Best", "q1 domBst Best", "q3 domBst Best", "avg domBst Sum", "min domBst Sum",
                    "max domBst Sum", "var domBst Sum", "stddev domBst Sum", "med domBst Sum", "q1 domBst Sum",
                    "q3 domBst Sum"
                },
                new[] {"ca entire db", "ca entire skyline", "ca sample skyline", "ca best rank", "ca sum rank"},
                new[]
                {"caMed entire db", "caMed entire skyline", "caMed sample skyline", "caMed best rank", "caMed sum rank"},
                new[]
                {
                    "caTopB entire db", "caTopB entire skyline", "caTopB sample skyline", "caTopB best rank",
                    "caTopB sum rank"
                },
                new[]
                {
                    "caMedTopB entire db", "caMedTopB entire skyline", "caMedTopB sample skyline", "caMedTopB best rank",
                    "caMedTopB sum rank"
                }, "sum correlation*", "product cardinality");
        }

        private List<IEnumerable<CLRSafeHashSet<int>>> ProduceSubsets(ArrayList preferences)
        {
            var randomSubsetsProducer = new RandomSkylineSamplingSubsetsProducer
            {
                AllPreferencesCount = preferences.Count,
                SubsetsCount = SubsetsCount,
                SubsetDimension = SubsetDimension
            };

            var producedSubsets = new List<IEnumerable<CLRSafeHashSet<int>>>();
            for (var ii = 0; ii < SamplesCount; ii++)
            {
                producedSubsets.Add(randomSubsetsProducer.GetSubsets());
            }
            return producedSubsets;
        }

        private static IReadOnlyDictionary<long, object[]> GetEntireDatabaseNormalized(SQLCommon parser, string strSQL,
            int[] skylineAttributeColumns, out DataTable dtEntire)
        {
            DbProviderFactory factory = DbProviderFactories.GetFactory(Helper.ProviderName);

            // use the factory object to create Data access objects.
            DbConnection connection = factory.CreateConnection();
            // will return the connection object (i.e. SqlConnection ...)
            connection.ConnectionString = Helper.ConnectionString;

            dtEntire = new DataTable();

            connection.Open();

            DbDataAdapter dap = factory.CreateDataAdapter();
            DbCommand selectCommand = connection.CreateCommand();
            selectCommand.CommandTimeout = 0; //infinite timeout

            string strQueryEntire;
            string operatorsEntire;
            int numberOfRecordsEntire;
            string[] parameterEntire;

            string ansiSqlEntire =
                parser.GetAnsiSqlFromPrefSqlModel(
                    parser.GetPrefSqlModelFromPreferenceSql(strSQL));
            prefSQL.SQLParser.Helper.DetermineParameters(ansiSqlEntire, out parameterEntire,
                out strQueryEntire, out operatorsEntire,
                out numberOfRecordsEntire);

            selectCommand.CommandText = strQueryEntire;
            dap.SelectCommand = selectCommand;
            dtEntire = new DataTable();

            dap.Fill(dtEntire);

            for (var ii = 0; ii < skylineAttributeColumns.Length; ii++)
            {
                dtEntire.Columns.RemoveAt(0);
            }

            IReadOnlyDictionary<long, object[]> entireDatabaseNormalized =
                prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(dtEntire, 0);
            SkylineSamplingHelper.NormalizeColumns(entireDatabaseNormalized, skylineAttributeColumns);

            return entireDatabaseNormalized;
        }

        internal void AddSummary(StringBuilder sb, string strSeparatorLine, List<long> reportDimensions,
            List<long> reportSkylineSize, List<long> reportTimeTotal, List<long> reportTimeAlgorithm,
            List<double> reportCorrelation, List<double> reportCardinality)
        {
            //Separator Line
            Debug.WriteLine(strSeparatorLine);
            sb.AppendLine(strSeparatorLine);

            string[] setCoverageSamplingAverage = GetSummaryAverage(_setCoverage, ExcessiveTests);
            string[] representationErrorSamplingAverage = GetSummaryAverage(_representationError, ExcessiveTests);
            string[] representationErrorSumSamplingAverage = GetSummaryAverage(_representationErrorSum, ExcessiveTests);
            string[] dominatedObjectsCountSamplingAverage = GetSummaryAverage(_dominatedObjectsCount, ExcessiveTests);
            string[] dominatedObjectsOfBestObjectSamplingAverage = GetSummaryAverage(_dominatedObjectsOfBestObject, ExcessiveTests);

            string[] setCoverageSamplingMin = GetSummaryMin(_setCoverage, ExcessiveTests);
            string[] representationErrorSamplingMin = GetSummaryMin(_representationError, ExcessiveTests);
            string[] representationErrorSumSamplingMin = GetSummaryMin(_representationErrorSum, ExcessiveTests);
            string[] dominatedObjectsCountSamplingMin = GetSummaryMin(_dominatedObjectsCount, ExcessiveTests);
            string[] dominatedObjectsOfBestObjectSamplingMin = GetSummaryMin(_dominatedObjectsOfBestObject, ExcessiveTests);

            string[] setCoverageSamplingMax = GetSummaryMax(_setCoverage, ExcessiveTests);
            string[] representationErrorSamplingMax = GetSummaryMax(_representationError, ExcessiveTests);
            string[] representationErrorSumSamplingMax = GetSummaryMax(_representationErrorSum, ExcessiveTests);
            string[] dominatedObjectsCountSamplingMax = GetSummaryMax(_dominatedObjectsCount, ExcessiveTests);
            string[] dominatedObjectsOfBestObjectSamplingMax = GetSummaryMax(_dominatedObjectsOfBestObject, ExcessiveTests);

            string[] setCoverageSamplingVariance = GetSummaryVariance(_setCoverage, ExcessiveTests);
            string[] representationErrorSamplingVariance = GetSummaryVariance(_representationError, ExcessiveTests);
            string[] representationErrorSumSamplingVariance = GetSummaryVariance(_representationErrorSum, ExcessiveTests);
            string[] dominatedObjectsCountSamplingVariance = GetSummaryVariance(_dominatedObjectsCount, ExcessiveTests);
            string[] dominatedObjectsOfBestObjectSamplingVariance = GetSummaryVariance(_dominatedObjectsOfBestObject, ExcessiveTests);

            string[] setCoverageSamplingStdDev = GetSummaryStdDev(_setCoverage, ExcessiveTests);
            string[] representationErrorSamplingStdDev = GetSummaryStdDev(_representationError, ExcessiveTests);
            string[] representationErrorSumSamplingStdDev = GetSummaryStdDev(_representationErrorSum, ExcessiveTests);
            string[] dominatedObjectsCountSamplingStdDev = GetSummaryStdDev(_dominatedObjectsCount, ExcessiveTests);
            string[] dominatedObjectsOfBestObjectSamplingStdDev = GetSummaryStdDev(_dominatedObjectsOfBestObject, ExcessiveTests);

            string[] setCoverageSamplingMedian = GetSummaryMedian(_setCoverage, ExcessiveTests);
            string[] representationErrorSamplingMedian = GetSummaryMedian(_representationError, ExcessiveTests);
            string[] representationErrorSumSamplingMedian = GetSummaryMedian(_representationErrorSum, ExcessiveTests);
            string[] dominatedObjectsCountSamplingMedian = GetSummaryMedian(_dominatedObjectsCount, ExcessiveTests);
            string[] dominatedObjectsOfBestObjectSamplingMedian = GetSummaryMedian(_dominatedObjectsOfBestObject, ExcessiveTests);

            string[] setCoverageSamplingQ1 = GetSummaryQ1(_setCoverage, ExcessiveTests);
            string[] representationErrorSamplingQ1 = GetSummaryQ1(_representationError, ExcessiveTests);
            string[] representationErrorSumSamplingQ1 = GetSummaryQ1(_representationErrorSum, ExcessiveTests);
            string[] dominatedObjectsCountSamplingQ1 = GetSummaryQ1(_dominatedObjectsCount, ExcessiveTests);
            string[] dominatedObjectsOfBestObjectSamplingQ1 = GetSummaryQ1(_dominatedObjectsOfBestObject, ExcessiveTests);

            string[] setCoverageSamplingQ3 = GetSummaryQ3(_setCoverage, ExcessiveTests);
            string[] representationErrorSamplingQ3 = GetSummaryQ3(_representationError, ExcessiveTests);
            string[] representationErrorSumSamplingQ3 = GetSummaryQ3(_representationErrorSum, ExcessiveTests);
            string[] dominatedObjectsCountSamplingQ3 = GetSummaryQ3(_dominatedObjectsCount, ExcessiveTests);
            string[] dominatedObjectsOfBestObjectSamplingQ3 = GetSummaryQ3(_dominatedObjectsOfBestObject, ExcessiveTests);

            string strAverage = FormatLineString("average", "", reportDimensions.Average(), reportSkylineSize.Average(),
                reportTimeTotal.Average(), reportTimeAlgorithm.Average(), _reportsLong[Reports.TimeMin].Average(),
                _reportsLong[Reports.TimeMax].Average(), _reportsDouble[Reports.TimeVar].Average(),
                _reportsDouble[Reports.TimeStdDev].Average(), _reportsLong[Reports.TimeMed].Average(),
                _reportsLong[Reports.TimeQ1].Average(), _reportsLong[Reports.TimeQ3].Average(),
                _reportsLong[Reports.SizeMin].Average(), _reportsLong[Reports.SizeMax].Average(),
                _reportsDouble[Reports.SizeVar].Average(), _reportsDouble[Reports.SizeStdDev].Average(),
                _reportsLong[Reports.SizeMed].Average(), _reportsLong[Reports.SizeQ1].Average(),
                _reportsLong[Reports.SizeQ3].Average(), setCoverageSamplingAverage, representationErrorSamplingAverage,
                representationErrorSumSamplingAverage, dominatedObjectsCountSamplingAverage,
                dominatedObjectsOfBestObjectSamplingAverage, new[] {"", "", "", "", ""}, new[] {"", "", "", "", ""},
                new[] {"", "", "", "", ""}, new[] {"", "", "", "", ""}, reportCorrelation.Average(),
                reportCardinality.Average());
            string strMin = FormatLineString("minimum", "", reportDimensions.Min(), reportSkylineSize.Min(),
                reportTimeTotal.Min(), reportTimeAlgorithm.Min(), _reportsLong[Reports.TimeMin].Min(),
                _reportsLong[Reports.TimeMax].Min(), _reportsDouble[Reports.TimeVar].Min(),
                _reportsDouble[Reports.TimeStdDev].Min(), _reportsLong[Reports.TimeMed].Min(),
                _reportsLong[Reports.TimeQ1].Min(), _reportsLong[Reports.TimeQ3].Min(),
                _reportsLong[Reports.SizeMin].Min(), _reportsLong[Reports.SizeMax].Min(),
                _reportsDouble[Reports.SizeVar].Min(), _reportsDouble[Reports.SizeStdDev].Min(),
                _reportsLong[Reports.SizeMed].Min(), _reportsLong[Reports.SizeQ1].Min(),
                _reportsLong[Reports.SizeQ3].Min(), setCoverageSamplingMin, representationErrorSamplingMin,
                representationErrorSumSamplingMin, dominatedObjectsCountSamplingMin,
                dominatedObjectsOfBestObjectSamplingMin, new[] {"", "", "", "", ""}, new[] {"", "", "", "", ""},
                new[] {"", "", "", "", ""}, new[] {"", "", "", "", ""}, reportCorrelation.Min(), reportCardinality.Min());
            string strMax = FormatLineString("maximum", "", reportDimensions.Max(), reportSkylineSize.Max(),
                reportTimeTotal.Max(), reportTimeAlgorithm.Max(), _reportsLong[Reports.TimeMin].Max(),
                _reportsLong[Reports.TimeMax].Max(), _reportsDouble[Reports.TimeVar].Max(),
                _reportsDouble[Reports.TimeStdDev].Max(), _reportsLong[Reports.TimeMed].Max(),
                _reportsLong[Reports.TimeQ1].Max(), _reportsLong[Reports.TimeQ3].Max(),
                _reportsLong[Reports.SizeMin].Max(), _reportsLong[Reports.SizeMax].Max(),
                _reportsDouble[Reports.SizeVar].Max(), _reportsDouble[Reports.SizeStdDev].Max(),
                _reportsLong[Reports.SizeMed].Max(), _reportsLong[Reports.SizeQ1].Max(),
                _reportsLong[Reports.SizeQ3].Max(), setCoverageSamplingMax, representationErrorSamplingMax,
                representationErrorSumSamplingMax, dominatedObjectsCountSamplingMax,
                dominatedObjectsOfBestObjectSamplingMax, new[] {"", "", "", "", ""}, new[] {"", "", "", "", ""},
                new[] {"", "", "", "", ""}, new[] {"", "", "", "", ""}, reportCorrelation.Max(), reportCardinality.Max());
            string strVar = FormatLineString("variance", "", MyMathematic.GetSampleVariance(reportDimensions),
                MyMathematic.GetSampleVariance(reportSkylineSize), MyMathematic.GetSampleVariance(reportTimeTotal),
                MyMathematic.GetSampleVariance(reportTimeAlgorithm),
                MyMathematic.GetSampleVariance(_reportsLong[Reports.TimeMin]),
                MyMathematic.GetSampleVariance(_reportsLong[Reports.TimeMax]),
                MyMathematic.GetSampleVariance(_reportsDouble[Reports.TimeVar]),
                MyMathematic.GetSampleVariance(_reportsDouble[Reports.TimeStdDev]),
                MyMathematic.GetSampleVariance(_reportsLong[Reports.TimeMed]),
                MyMathematic.GetSampleVariance(_reportsLong[Reports.TimeQ1]),
                MyMathematic.GetSampleVariance(_reportsLong[Reports.TimeQ3]),
                MyMathematic.GetSampleVariance(_reportsLong[Reports.SizeMin]),
                MyMathematic.GetSampleVariance(_reportsLong[Reports.SizeMax]),
                MyMathematic.GetSampleVariance(_reportsDouble[Reports.SizeVar]),
                MyMathematic.GetSampleVariance(_reportsDouble[Reports.SizeStdDev]),
                MyMathematic.GetSampleVariance(_reportsLong[Reports.SizeMed]),
                MyMathematic.GetSampleVariance(_reportsLong[Reports.SizeQ1]),
                MyMathematic.GetSampleVariance(_reportsLong[Reports.SizeQ3]), setCoverageSamplingVariance,
                representationErrorSamplingVariance, representationErrorSumSamplingVariance,
                dominatedObjectsCountSamplingVariance, dominatedObjectsOfBestObjectSamplingVariance,
                new[] {"", "", "", "", ""}, new[] {"", "", "", "", ""}, new[] {"", "", "", "", ""},
                new[] {"", "", "", "", ""}, MyMathematic.GetSampleVariance(reportCorrelation),
                MyMathematic.GetSampleVariance(reportCardinality));
            string strStd = FormatLineString("stddeviation", "", MyMathematic.GetSampleStdDeviation(reportDimensions),
                MyMathematic.GetSampleStdDeviation(reportSkylineSize),
                MyMathematic.GetSampleStdDeviation(reportTimeTotal),
                MyMathematic.GetSampleStdDeviation(reportTimeAlgorithm),
                MyMathematic.GetSampleStdDeviation(_reportsLong[Reports.TimeMin]),
                MyMathematic.GetSampleStdDeviation(_reportsLong[Reports.TimeMax]),
                MyMathematic.GetSampleStdDeviation(_reportsDouble[Reports.TimeVar]),
                MyMathematic.GetSampleStdDeviation(_reportsDouble[Reports.TimeStdDev]),
                MyMathematic.GetSampleStdDeviation(_reportsLong[Reports.TimeMed]),
                MyMathematic.GetSampleStdDeviation(_reportsLong[Reports.TimeQ1]),
                MyMathematic.GetSampleStdDeviation(_reportsLong[Reports.TimeQ3]),
                MyMathematic.GetSampleStdDeviation(_reportsLong[Reports.SizeMin]),
                MyMathematic.GetSampleStdDeviation(_reportsLong[Reports.SizeMax]),
                MyMathematic.GetSampleStdDeviation(_reportsDouble[Reports.SizeVar]),
                MyMathematic.GetSampleStdDeviation(_reportsDouble[Reports.SizeStdDev]),
                MyMathematic.GetSampleStdDeviation(_reportsLong[Reports.SizeMed]),
                MyMathematic.GetSampleStdDeviation(_reportsLong[Reports.SizeQ1]),
                MyMathematic.GetSampleStdDeviation(_reportsLong[Reports.SizeQ3]), setCoverageSamplingStdDev,
                representationErrorSamplingStdDev, representationErrorSumSamplingStdDev,
                dominatedObjectsCountSamplingStdDev, dominatedObjectsOfBestObjectSamplingStdDev,
                new[] {"", "", "", "", ""}, new[] {"", "", "", "", ""}, new[] {"", "", "", "", ""},
                new[] {"", "", "", "", ""}, MyMathematic.GetSampleStdDeviation(reportCorrelation),
                MyMathematic.GetSampleStdDeviation(reportCardinality));
            string strMed = FormatLineString("median", "", Mathematic.Median(reportDimensions),
                Mathematic.Median(reportSkylineSize), Mathematic.Median(reportTimeTotal),
                Mathematic.Median(reportTimeAlgorithm), Mathematic.Median(_reportsLong[Reports.TimeMin]),
                Mathematic.Median(_reportsLong[Reports.TimeMax]), Mathematic.Median(_reportsDouble[Reports.TimeVar]),
                Mathematic.Median(_reportsDouble[Reports.TimeStdDev]), Mathematic.Median(_reportsLong[Reports.TimeMed]),
                Mathematic.Median(_reportsLong[Reports.TimeQ1]), Mathematic.Median(_reportsLong[Reports.TimeQ3]),
                Mathematic.Median(_reportsLong[Reports.SizeMin]), Mathematic.Median(_reportsLong[Reports.SizeMax]),
                Mathematic.Median(_reportsDouble[Reports.SizeVar]),
                Mathematic.Median(_reportsDouble[Reports.SizeStdDev]), Mathematic.Median(_reportsLong[Reports.SizeMed]),
                Mathematic.Median(_reportsLong[Reports.SizeQ1]), Mathematic.Median(_reportsLong[Reports.SizeQ3]),
                setCoverageSamplingMedian, representationErrorSamplingMedian, representationErrorSumSamplingMedian,
                dominatedObjectsCountSamplingMedian, dominatedObjectsOfBestObjectSamplingMedian,
                new[] {"", "", "", "", ""}, new[] {"", "", "", "", ""}, new[] {"", "", "", "", ""},
                new[] {"", "", "", "", ""}, Mathematic.Median(reportCorrelation), Mathematic.Median(reportCardinality));
            string strQ1 = FormatLineString("quartile 1", "", Mathematic.LowerQuartile(reportDimensions),
                Mathematic.LowerQuartile(reportSkylineSize), Mathematic.LowerQuartile(reportTimeTotal),
                Mathematic.LowerQuartile(reportTimeAlgorithm), Mathematic.LowerQuartile(_reportsLong[Reports.TimeMin]),
                Mathematic.LowerQuartile(_reportsLong[Reports.TimeMax]),
                Mathematic.LowerQuartile(_reportsDouble[Reports.TimeVar]),
                Mathematic.LowerQuartile(_reportsDouble[Reports.TimeStdDev]),
                Mathematic.LowerQuartile(_reportsLong[Reports.TimeMed]),
                Mathematic.LowerQuartile(_reportsLong[Reports.TimeQ1]),
                Mathematic.LowerQuartile(_reportsLong[Reports.TimeQ3]),
                Mathematic.LowerQuartile(_reportsLong[Reports.SizeMin]),
                Mathematic.LowerQuartile(_reportsLong[Reports.SizeMax]),
                Mathematic.LowerQuartile(_reportsDouble[Reports.SizeVar]),
                Mathematic.LowerQuartile(_reportsDouble[Reports.SizeStdDev]),
                Mathematic.LowerQuartile(_reportsLong[Reports.SizeMed]),
                Mathematic.LowerQuartile(_reportsLong[Reports.SizeQ1]),
                Mathematic.LowerQuartile(_reportsLong[Reports.SizeQ3]), setCoverageSamplingQ1,
                representationErrorSamplingQ1, representationErrorSumSamplingQ1, dominatedObjectsCountSamplingQ1,
                dominatedObjectsOfBestObjectSamplingQ1, new[] {"", "", "", "", ""}, new[] {"", "", "", "", ""},
                new[] {"", "", "", "", ""}, new[] {"", "", "", "", ""}, Mathematic.LowerQuartile(reportCorrelation),
                Mathematic.LowerQuartile(reportCardinality));
            string strQ3 = FormatLineString("quartile 3", "", Mathematic.UpperQuartile(reportDimensions),
                Mathematic.UpperQuartile(reportSkylineSize), Mathematic.UpperQuartile(reportTimeTotal),
                Mathematic.UpperQuartile(reportTimeAlgorithm), Mathematic.UpperQuartile(_reportsLong[Reports.TimeMin]),
                Mathematic.UpperQuartile(_reportsLong[Reports.TimeMax]),
                Mathematic.UpperQuartile(_reportsDouble[Reports.TimeVar]),
                Mathematic.UpperQuartile(_reportsDouble[Reports.TimeStdDev]),
                Mathematic.UpperQuartile(_reportsLong[Reports.TimeMed]),
                Mathematic.UpperQuartile(_reportsLong[Reports.TimeQ1]),
                Mathematic.UpperQuartile(_reportsLong[Reports.TimeQ3]),
                Mathematic.UpperQuartile(_reportsLong[Reports.SizeMin]),
                Mathematic.UpperQuartile(_reportsLong[Reports.SizeMax]),
                Mathematic.UpperQuartile(_reportsDouble[Reports.SizeVar]),
                Mathematic.UpperQuartile(_reportsDouble[Reports.SizeStdDev]),
                Mathematic.UpperQuartile(_reportsLong[Reports.SizeMed]),
                Mathematic.UpperQuartile(_reportsLong[Reports.SizeQ1]),
                Mathematic.UpperQuartile(_reportsLong[Reports.SizeQ3]), setCoverageSamplingQ3,
                representationErrorSamplingQ3, representationErrorSumSamplingQ3, dominatedObjectsCountSamplingQ3,
                dominatedObjectsOfBestObjectSamplingQ3, new[] {"", "", "", "", ""}, new[] {"", "", "", "", ""},
                new[] {"", "", "", "", ""}, new[] {"", "", "", "", ""}, Mathematic.UpperQuartile(reportCorrelation),
                Mathematic.UpperQuartile(reportCardinality));

            sb.AppendLine(strAverage);
            sb.AppendLine(strMin);
            sb.AppendLine(strMax);
            sb.AppendLine(strVar);
            sb.AppendLine(strStd);
            sb.AppendLine(strMed);
            sb.AppendLine(strQ1);
            sb.AppendLine(strQ3);
            Debug.WriteLine(strAverage);
            Debug.WriteLine(strMin);
            Debug.WriteLine(strMax);
            Debug.WriteLine(strVar);
            Debug.WriteLine(strStd);
            Debug.WriteLine(strMed);
            Debug.WriteLine(strQ1);
            Debug.WriteLine(strQ3);

            //Separator Line
            sb.AppendLine(strSeparatorLine);
            Debug.WriteLine(strSeparatorLine);
        }

        private static string[] GetSummaryAverage(Dictionary<SkylineTypes, List<double>> list, bool excessiveTests)
        {
            var array = new string[Enum.GetValues(typeof (SkylineTypes)).Length];

            var count = 0;
            if (!excessiveTests)
            {
                foreach (
            SkylineTypes skylineTypesSamplingType in
                Enum.GetValues(typeof(SkylineTypes)).Cast<SkylineTypes>())
                {
                    array[count] = "";
                    count++;
                }

                return array;
            }

            count = 0;

            foreach (
                SkylineTypes skylineTypesSamplingType in
                    Enum.GetValues(typeof (SkylineTypes)).Cast<SkylineTypes>())
            {
                {
                    array[count] =
                        Math.Round(list[skylineTypesSamplingType].Average(), 2)
                            .ToString(CultureInfo.InvariantCulture);
                    count++;
                }
            }

            return array;
        }

        private static string[] GetSummaryMin(Dictionary<SkylineTypes, List<double>> list, bool excessiveTests)
        {
            var array = new string[Enum.GetValues(typeof (SkylineTypes)).Length];

            var count = 0;
            if (!excessiveTests)
            {
                foreach (
            SkylineTypes skylineTypesSamplingType in
                Enum.GetValues(typeof(SkylineTypes)).Cast<SkylineTypes>())
                {
                    array[count] = "";
                    count++;
                }

                return array;
            }

            count = 0;

            foreach (
                SkylineTypes skylineTypesSamplingType in
                    Enum.GetValues(typeof (SkylineTypes)).Cast<SkylineTypes>())
            {
                {
                    array[count] =
                        Math.Round(list[skylineTypesSamplingType].Min(), 2)
                            .ToString(CultureInfo.InvariantCulture);
                    count++;
                }
            }

            return array;
        }

        private static string[] GetSummaryMax(Dictionary<SkylineTypes, List<double>> list, bool excessiveTests)
        {
            var array = new string[Enum.GetValues(typeof (SkylineTypes)).Length];

            var count = 0;
            if (!excessiveTests)
            {
                foreach (
            SkylineTypes skylineTypesSamplingType in
                Enum.GetValues(typeof(SkylineTypes)).Cast<SkylineTypes>())
                {
                    array[count] = "";
                    count++;
                }

                return array;
            }

            count = 0;

            foreach (
                SkylineTypes skylineTypesSamplingType in
                    Enum.GetValues(typeof (SkylineTypes)).Cast<SkylineTypes>())
            {
                {
                    array[count] =
                        Math.Round(list[skylineTypesSamplingType].Max(), 2)
                            .ToString(CultureInfo.InvariantCulture);
                    count++;
                }
            }

            return array;
        }

        private static string[] GetSummaryVariance(Dictionary<SkylineTypes, List<double>> list, bool excessiveTests)
        {
            var array = new string[Enum.GetValues(typeof (SkylineTypes)).Length];
            var count = 0;
            if (!excessiveTests)
            {
                foreach (
            SkylineTypes skylineTypesSamplingType in
                Enum.GetValues(typeof(SkylineTypes)).Cast<SkylineTypes>())
                {
                    array[count] = "";
                    count++;
                }

                return array;
            }

            count = 0;

            foreach (
                SkylineTypes skylineTypesSamplingType in
                    Enum.GetValues(typeof (SkylineTypes)).Cast<SkylineTypes>())
            {
                {
                    array[count] =
                        Math.Round(MyMathematic.GetSampleVariance(list[skylineTypesSamplingType]), 2)
                            .ToString(CultureInfo.InvariantCulture);
                    count++;
                }
            }

            return array;
        }

        private static string[] GetSummaryStdDev(Dictionary<SkylineTypes, List<double>> list, bool excessiveTests)
        {
            var array = new string[Enum.GetValues(typeof (SkylineTypes)).Length];

            var count = 0;
            if (!excessiveTests)
            {
                foreach (
            SkylineTypes skylineTypesSamplingType in
                Enum.GetValues(typeof(SkylineTypes)).Cast<SkylineTypes>())
                {
                    array[count] = "";
                    count++;
                }

                return array;
            }

            count = 0;

            foreach (
                SkylineTypes skylineTypesSamplingType in
                    Enum.GetValues(typeof (SkylineTypes)).Cast<SkylineTypes>())
            {
                {
                    array[count] =
                        Math.Round(MyMathematic.GetSampleStdDeviation(list[skylineTypesSamplingType]), 2)
                            .ToString(CultureInfo.InvariantCulture);
                    count++;
                }
            }

            return array;
        }

        private static string[] GetSummaryMedian(Dictionary<SkylineTypes, List<double>> list, bool excessiveTests)
        {
            var array = new string[Enum.GetValues(typeof (SkylineTypes)).Length];

            var count = 0;
            if (!excessiveTests)
            {
                foreach (
            SkylineTypes skylineTypesSamplingType in
                Enum.GetValues(typeof(SkylineTypes)).Cast<SkylineTypes>())
                {
                    array[count] = "";
                    count++;
                }

                return array;
            }

            count = 0;

            foreach (
                SkylineTypes skylineTypesSamplingType in
                    Enum.GetValues(typeof (SkylineTypes)).Cast<SkylineTypes>())
            {
                {
                    array[count] =
                        Math.Round(Mathematic.Median(list[skylineTypesSamplingType]), 2)
                            .ToString(CultureInfo.InvariantCulture);
                    count++;
                }
            }

            return array;
        }

        private static string[] GetSummaryQ1(Dictionary<SkylineTypes, List<double>> list, bool excessiveTests)
        {
            var array = new string[Enum.GetValues(typeof (SkylineTypes)).Length];

            var count = 0;
            if (!excessiveTests)
            {
                foreach (
            SkylineTypes skylineTypesSamplingType in
                Enum.GetValues(typeof(SkylineTypes)).Cast<SkylineTypes>())
                {
                    array[count] = "";
                    count++;
                }

                return array;
            }

            count = 0;

            foreach (
                SkylineTypes skylineTypesSamplingType in
                    Enum.GetValues(typeof (SkylineTypes)).Cast<SkylineTypes>())
            {
                {
                    array[count] =
                        Math.Round(Mathematic.LowerQuartile(list[skylineTypesSamplingType]), 2)
                            .ToString(CultureInfo.InvariantCulture);
                    count++;
                }
            }

            return array;
        }

        private static string[] GetSummaryQ3(Dictionary<SkylineTypes, List<double>> list, bool excessiveTests)
        {
            var array = new string[Enum.GetValues(typeof (SkylineTypes)).Length];

            var count = 0;
            if (!excessiveTests)
            {
                foreach (
            SkylineTypes skylineTypesSamplingType in
                Enum.GetValues(typeof(SkylineTypes)).Cast<SkylineTypes>())
                {
                    array[count] = "";
                    count++;
                }

                return array;
            }

            count = 0;
            foreach (
                SkylineTypes skylineTypesSamplingType in
                    Enum.GetValues(typeof (SkylineTypes)).Cast<SkylineTypes>())
            {
                {
                    array[count] =
                        Math.Round(Mathematic.UpperQuartile(list[skylineTypesSamplingType]), 2)
                            .ToString(CultureInfo.InvariantCulture);
                    count++;
                }
            }

            return array;
        }

        private static void FillTopBuckets(
            Dictionary<ClusterAnalysis, Dictionary<BigInteger, List<double>>> clusterAnalysisTopBuckets,
            ClusterAnalysis sampleSkylineType,
            IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>> skylineTypeBuckets,
            int skylineCount, int entireDbCount, int entireSkylineCount)
        {
            List<KeyValuePair<BigInteger, List<IReadOnlyDictionary<long, object[]>>>> sortedTop5 =
                skylineTypeBuckets.OrderByDescending(l => l.Value.Count).ThenBy(l => l.Key).Take(5).ToList();
            // track top 5 buckets

            foreach (KeyValuePair<BigInteger, List<IReadOnlyDictionary<long, object[]>>> skylineTypeBucket in sortedTop5
                )
            {
                if (!clusterAnalysisTopBuckets[sampleSkylineType].ContainsKey(skylineTypeBucket.Key))
                {
                    clusterAnalysisTopBuckets[sampleSkylineType].Add(skylineTypeBucket.Key, new List<double>());
                }
                double percent = (double) skylineTypeBucket.Value.Count / skylineCount;
                clusterAnalysisTopBuckets[sampleSkylineType][skylineTypeBucket.Key].Add(percent);
            }

            foreach (
                KeyValuePair<BigInteger, List<double>> entireDbBucket in
                    clusterAnalysisTopBuckets[ClusterAnalysis.EntireDb])
                // additionally track top 5 buckets of entire db
            {
                if (!clusterAnalysisTopBuckets[sampleSkylineType].ContainsKey(entireDbBucket.Key))
                {
                    clusterAnalysisTopBuckets[sampleSkylineType].Add(entireDbBucket.Key, new List<double>());
                }

                if (!skylineTypeBuckets.ContainsKey(entireDbBucket.Key)) // not contained => percentage = 0
                {
                    clusterAnalysisTopBuckets[sampleSkylineType][entireDbBucket.Key].Add(0);
                }
                else if (sortedTop5.All(item => item.Key != entireDbBucket.Key))
                    // else: already added in previous foreach => no need to add again
                {
                    double percent = (double) skylineTypeBuckets[entireDbBucket.Key].Count / entireDbCount;
                    clusterAnalysisTopBuckets[sampleSkylineType][entireDbBucket.Key].Add(percent);
                }
            }

            foreach (
                KeyValuePair<BigInteger, List<double>> entireSkylineBucket in
                    clusterAnalysisTopBuckets[ClusterAnalysis.EntireSkyline])
                // additionally track top 5 buckets of entire skyline
            {
                if (!clusterAnalysisTopBuckets[sampleSkylineType].ContainsKey(entireSkylineBucket.Key))
                {
                    clusterAnalysisTopBuckets[sampleSkylineType].Add(entireSkylineBucket.Key, new List<double>());
                }

                if (!skylineTypeBuckets.ContainsKey(entireSkylineBucket.Key)) // not contained => percentage = 0
                {
                    clusterAnalysisTopBuckets[sampleSkylineType][entireSkylineBucket.Key].Add(0);
                }
                else if (sortedTop5.All(item => item.Key != entireSkylineBucket.Key))
                    // else: already added in previous foreach => no need to add again
                {
                    double percent = (double) skylineTypeBuckets[entireSkylineBucket.Key].Count / entireSkylineCount;
                    clusterAnalysisTopBuckets[sampleSkylineType][entireSkylineBucket.Key].Add(percent);
                }
            }
        }

        private IReadOnlyDictionary<long, object[]> GetReducedSkyline(
            IReadOnlyDictionary<long, object[]> baseSkylineNormalized,
            IReadOnlyDictionary<long, object[]> subtractSkylineNormalized)
        {
            var ret = new Dictionary<long, object[]>();

            foreach (KeyValuePair<long, object[]> baseObject in baseSkylineNormalized)
            {
                if (!subtractSkylineNormalized.ContainsKey(baseObject.Key))
                {
                    ret.Add(baseObject.Key, baseObject.Value);
                }
            }

            return new ReadOnlyDictionary<long, object[]>(ret);
        }

        private static Dictionary<ClusterAnalysis, string> GetClusterAnalysisTopBucketsStrings(
            Dictionary<ClusterAnalysis, Dictionary<BigInteger, List<double>>> clusterAnalysisTopBuckets, bool excessiveTests)
        {
            var clusterAnalysisStrings =
                new Dictionary<ClusterAnalysis, string>()
                {
                    {ClusterAnalysis.EntireDb, ""},
                    {ClusterAnalysis.EntireSkyline, ""},
                    {ClusterAnalysis.SampleSkyline, ""},
                    {ClusterAnalysis.BestRank, ""},
                    {ClusterAnalysis.SumRank, ""}
                };

            if (!excessiveTests)
            {
                return clusterAnalysisStrings;
            }

            int subsetsCount = clusterAnalysisTopBuckets[ClusterAnalysis.EntireDb].Values.First().Count;

            IOrderedEnumerable<KeyValuePair<BigInteger, List<double>>> allEntireDbBuckets = clusterAnalysisTopBuckets[
                ClusterAnalysis.EntireDb].OrderByDescending(
                    l => l.Value.Sum() / subsetsCount).ThenBy(l => l.Key);
            IOrderedEnumerable<KeyValuePair<BigInteger, List<double>>> allEntireSkylineBuckets = clusterAnalysisTopBuckets
                [ClusterAnalysis.EntireSkyline].OrderByDescending(
                    l => l.Value.Sum() / subsetsCount).ThenBy(l => l.Key);

            foreach (
                ClusterAnalysis clusterAnalysisType in
                    Enum.GetValues(typeof (ClusterAnalysis)).Cast<ClusterAnalysis>())
            {
                foreach (KeyValuePair<BigInteger, List<double>> bucket in allEntireDbBuckets)
                {
                    double percent = clusterAnalysisTopBuckets[clusterAnalysisType][bucket.Key].Sum() / subsetsCount;
                    clusterAnalysisStrings[clusterAnalysisType] += "EB-" + bucket.Key + ":" +
                                                                   string.Format("{0:0.00},", percent * 100);
                }

                clusterAnalysisStrings[clusterAnalysisType] = clusterAnalysisStrings[clusterAnalysisType].TrimEnd(',');

                if (clusterAnalysisType != ClusterAnalysis.EntireDb)
                {
                    clusterAnalysisStrings[clusterAnalysisType] += ";";

                    foreach (KeyValuePair<BigInteger, List<double>> bucket in allEntireSkylineBuckets.Take(5))
                    {
                        double percent = clusterAnalysisTopBuckets[clusterAnalysisType][bucket.Key].Sum() /
                                         subsetsCount;
                        clusterAnalysisStrings[clusterAnalysisType] += "ESB-" + bucket.Key + ":" +
                                                                       string.Format("{0:0.00},", percent * 100);
                    }

                    clusterAnalysisStrings[clusterAnalysisType] =
                        clusterAnalysisStrings[clusterAnalysisType].TrimEnd(',');
                    if (clusterAnalysisType != ClusterAnalysis.EntireSkyline)
                    {
                        clusterAnalysisStrings[clusterAnalysisType] += ";";

                        foreach (
                            KeyValuePair<BigInteger, List<double>> bucket in
                                clusterAnalysisTopBuckets[clusterAnalysisType].OrderByDescending(
                                    l => l.Value.Sum() / subsetsCount).ThenBy(l => l.Key).Take(5))
                        {
                            double percent = clusterAnalysisTopBuckets[clusterAnalysisType][bucket.Key].Sum() /
                                             subsetsCount;
                            clusterAnalysisStrings[clusterAnalysisType] += "SB-" + bucket.Key + ":" +
                                                                           string.Format("{0:0.00},", percent * 100);
                        }

                        clusterAnalysisStrings[clusterAnalysisType] =
                            clusterAnalysisStrings[clusterAnalysisType].TrimEnd(',');
                    }
                }
            }

            return clusterAnalysisStrings;
        }

        private static Dictionary<ClusterAnalysis, string> GetClusterAnalysisStrings(int[] skylineAttributeColumns,
            Dictionary<ClusterAnalysis, List<List<double>>> clusterAnalysisSampling)
        {
            var clusterAnalysisAverages =
                new Dictionary<ClusterAnalysis, List<double>>()
                {
                    {ClusterAnalysis.EntireDb, new List<double>()},
                    {ClusterAnalysis.EntireSkyline, new List<double>()},
                    {ClusterAnalysis.SampleSkyline, new List<double>()},
                    {ClusterAnalysis.BestRank, new List<double>()},
                    {ClusterAnalysis.SumRank, new List<double>()}
                };

            var clusterAnalysisStrings =
                new Dictionary<ClusterAnalysis, string>()
                {
                    {ClusterAnalysis.EntireDb, ""},
                    {ClusterAnalysis.EntireSkyline, ""},
                    {ClusterAnalysis.SampleSkyline, ""},
                    {ClusterAnalysis.BestRank, ""},
                    {ClusterAnalysis.SumRank, ""}
                };

            for (var bucket = 0; bucket < skylineAttributeColumns.Length; bucket++)
            {
                clusterAnalysisAverages[ClusterAnalysis.EntireDb].Add(0);
                clusterAnalysisAverages[ClusterAnalysis.EntireSkyline].Add(0);
                clusterAnalysisAverages[ClusterAnalysis.SampleSkyline].Add(0);
                clusterAnalysisAverages[ClusterAnalysis.BestRank].Add(0);
                clusterAnalysisAverages[ClusterAnalysis.SumRank].Add(0);
            }

            foreach (
                ClusterAnalysis clusterAnalysisType in
                    Enum.GetValues(typeof (ClusterAnalysis)).Cast<ClusterAnalysis>())
            {
                foreach (List<double> row in clusterAnalysisSampling[clusterAnalysisType])
                {
                    for (var bucket = 0; bucket < row.Count; bucket++)
                    {
                        clusterAnalysisAverages[clusterAnalysisType][bucket] += row[bucket];
                    }
                }

                for (var bucket = 0; bucket < skylineAttributeColumns.Length; bucket++)
                {
                    clusterAnalysisAverages[clusterAnalysisType][bucket] /=
                        clusterAnalysisSampling[clusterAnalysisType].Count;
                }

                foreach (double averageValue in clusterAnalysisAverages[clusterAnalysisType])
                {
                    clusterAnalysisStrings[clusterAnalysisType] += string.Format("{0:0.00};", averageValue * 100);
                }

                clusterAnalysisStrings[clusterAnalysisType] = clusterAnalysisStrings[clusterAnalysisType].TrimEnd(';');
            }
            return clusterAnalysisStrings;
        }

        private static IReadOnlyDictionary<long, object[]> GetEntireSkylineDataTableRankNormalized(
            DataTable entireSkyline, List<long[]> skylineValues, int[] skylineAttributeColumns, int numberOfRecords,
            int sortType, out IReadOnlyDictionary<long, object[]> skylineDatabase)
        {
            var sortedDataTable = new DataTable();

            if (sortType == 1)
            {
                sortedDataTable = prefSQL.SQLSkyline.Helper.SortByRank(entireSkyline, skylineValues);
            }
            else if (sortType == 2)
            {
                sortedDataTable = prefSQL.SQLSkyline.Helper.SortBySum(entireSkyline, skylineValues);
            }

            prefSQL.SQLSkyline.Helper.GetAmountOfTuples(sortedDataTable, numberOfRecords);

            skylineDatabase = prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(sortedDataTable, 0);

            IReadOnlyDictionary<long, object[]> sortedDataTableNormalized =
                prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(sortedDataTable, 0);
            SkylineSamplingHelper.NormalizeColumns(sortedDataTableNormalized, skylineAttributeColumns);
            return sortedDataTableNormalized;
        }

        public void AddPreferenceSetInformation(StringBuilder sb, ArrayList listPreferences, string strSeparatorLine)
        {
            Debug.WriteLine("");
            sb.AppendLine("");

            for (var iPreferenceIndex = 0; iPreferenceIndex < listPreferences.Count; iPreferenceIndex++)
            {
                string strPreferenceSet = iPreferenceIndex + 1 + " / " + listPreferences.Count;

                var subPreferences = (ArrayList)listPreferences[iPreferenceIndex];

                Debug.WriteLine(strPreferenceSet.PadLeft(19, ' ') + ": SKYLINE OF " +
                                string.Join(",", (string[]) subPreferences.ToArray(Type.GetType("System.String"))));
                sb.AppendLine(strPreferenceSet.PadLeft(19, ' ') + ": SKYLINE OF " +
                              string.Join(",", (string[]) subPreferences.ToArray(Type.GetType("System.String"))));
            }
        }
    }
}