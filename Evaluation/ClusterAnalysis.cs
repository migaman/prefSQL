namespace prefSQL.Evaluation
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Numerics;

    public sealed class ClusterAnalysis
    {
        private readonly IReadOnlyDictionary<long, object[]> _entireDatabaseNormalized;
        private readonly double[] _medians;

        private double[] Medians
        {
            get { return _medians; }
        }

        private IReadOnlyDictionary<long, object[]> EntireDatabaseNormalized
        {
            get { return _entireDatabaseNormalized; }
        }

        public ClusterAnalysis(IReadOnlyDictionary<long, object[]> entireDatabaseNormalized, int[] useColumns)
        {
            _entireDatabaseNormalized = entireDatabaseNormalized;
            _medians = CalcMedians(_entireDatabaseNormalized,useColumns);
        }

        public static double[] CalcMedians(IReadOnlyDictionary<long, object[]> normalizedBaseData, int[] useColumns)
        {
            var medians = new double[useColumns.Length];

            for (var i = 0; i < useColumns.Length; i++)
            {
                var values = new List<double>();

                foreach (KeyValuePair<long, object[]> row in normalizedBaseData)
                {
                    int column = useColumns[i];
                    values.Add((double) row.Value[column]);
                }

                medians[i] = Median(values);
            }

            return medians;
        }

        public IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>> GetBuckets(
            IReadOnlyDictionary<long, object[]> normalizedBaseData, int[] useColumns, bool withEntireDatabaseMedians)
        {
            return GetBuckets(normalizedBaseData, useColumns, Medians);
        }

        public static IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>> GetBuckets(
            IReadOnlyDictionary<long, object[]> normalizedBaseData, int[] useColumns)
        {
            var medians = new double[useColumns.Length];

            for (var i = 0; i < medians.Length; i++)
            {
                medians[i] = .5;
            }

            return GetBuckets(normalizedBaseData, useColumns, medians);
        }

        /// <summary>
        ///     TODO W.-T. Balke, J. X. Zheng, and U. Güntzer (2005).
        /// </summary>
        /// <remarks>
        ///     Publication:
        ///     W.-T. Balke, J. X. Zheng, and U. Güntzer, “Approaching the Efficient Frontier: Cooperative Database Retrieval Using
        ///     High-Dimensional Skylines,” in Lecture Notes in Computer Science, Database Systems for Advanced Applications, D.
        ///     Hutchison, T. Kanade, J. Kittler, J. M. Kleinberg, F. Mattern, J. C. Mitchell, M. Naor, O. Nierstrasz, C. Pandu
        ///     Rangan, B. Steffen, M. Sudan, D. Terzopoulos, D. Tygar, M. Y. Vardi, G. Weikum, L. Zhou, B. C. Ooi, and X. Meng,
        ///     Eds, Berlin, Heidelberg: Springer Berlin Heidelberg, 2005, pp. 410–421.
        /// </remarks>
        /// <param name="normalizedBaseData"></param>
        /// <param name="useColumns"></param>
        /// <param name="medians"></param>
        /// <returns></returns>
        public static IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>> GetBuckets(
            IReadOnlyDictionary<long, object[]> normalizedBaseData, int[] useColumns, double[] medians)
        {
            var ret = new Dictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>();

            foreach (KeyValuePair<long, object[]> row in normalizedBaseData)
            {
                BigInteger bucket = BigInteger.Zero;

                for (var i = 0; i < useColumns.Length; i++)
                {
                    int column = useColumns[i];
                    if ((double) row.Value[column] >= medians[i])
                    {
                        bucket += BigInteger.Pow(2, i);
                    }
                }

                if (!ret.ContainsKey(bucket))
                {
                    ret.Add(bucket, new List<IReadOnlyDictionary<long, object[]>>());
                }

                List<IReadOnlyDictionary<long, object[]>> list = ret[bucket];
                list.Add(new Dictionary<long, object[]> {{row.Key, row.Value}});
            }

            return ret;
        }

        /// <summary>
        ///     TODO W.-T. Balke, J. X. Zheng, and U. Güntzer (2005).
        /// </summary>
        /// <remarks>
        ///     Publication:
        ///     W.-T. Balke, J. X. Zheng, and U. Güntzer, “Approaching the Efficient Frontier: Cooperative Database Retrieval Using
        ///     High-Dimensional Skylines,” in Lecture Notes in Computer Science, Database Systems for Advanced Applications, D.
        ///     Hutchison, T. Kanade, J. Kittler, J. M. Kleinberg, F. Mattern, J. C. Mitchell, M. Naor, O. Nierstrasz, C. Pandu
        ///     Rangan, B. Steffen, M. Sudan, D. Terzopoulos, D. Tygar, M. Y. Vardi, G. Weikum, L. Zhou, B. C. Ooi, and X. Meng,
        ///     Eds, Berlin, Heidelberg: Springer Berlin Heidelberg, 2005, pp. 410–421.
        /// </remarks>
        /// <param name="normalizedBaseData"></param>
        /// <param name="useColumns"></param>
        /// <returns></returns>
        public static IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>> GetAggregatedBuckets(
            IReadOnlyDictionary<long, object[]> normalizedBaseData, int[] useColumns)
        {
            return GetAggregatedBuckets(GetBuckets(normalizedBaseData, useColumns));
        }

        public static IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>> GetAggregatedBuckets(
            IReadOnlyDictionary<long, object[]> normalizedBaseData, int[] useColumns, double[] medians)
        {
            return GetAggregatedBuckets(GetBuckets(normalizedBaseData, useColumns, medians));
        }

        public IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>> GetAggregatedBuckets(
            IReadOnlyDictionary<long, object[]> normalizedBaseData, int[] useColumns, bool withEntireDatabaseMedians)
        {
            return GetAggregatedBuckets(GetBuckets(normalizedBaseData, useColumns, withEntireDatabaseMedians));
        }

        /// <summary>
        ///     TODO W.-T. Balke, J. X. Zheng, and U. Güntzer (2005).
        /// </summary>
        /// <remarks>
        ///     Publication:
        ///     W.-T. Balke, J. X. Zheng, and U. Güntzer, “Approaching the Efficient Frontier: Cooperative Database Retrieval Using
        ///     High-Dimensional Skylines,” in Lecture Notes in Computer Science, Database Systems for Advanced Applications, D.
        ///     Hutchison, T. Kanade, J. Kittler, J. M. Kleinberg, F. Mattern, J. C. Mitchell, M. Naor, O. Nierstrasz, C. Pandu
        ///     Rangan, B. Steffen, M. Sudan, D. Terzopoulos, D. Tygar, M. Y. Vardi, G. Weikum, L. Zhou, B. C. Ooi, and X. Meng,
        ///     Eds, Berlin, Heidelberg: Springer Berlin Heidelberg, 2005, pp. 410–421.
        /// </remarks>
        /// <param name="buckets"></param>
        /// <returns></returns>
        public static IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>> GetAggregatedBuckets(
            IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>> buckets)
        {
            var ret = new Dictionary<int, List<IReadOnlyDictionary<long, object[]>>>();

            foreach (KeyValuePair<BigInteger, List<IReadOnlyDictionary<long, object[]>>> row in buckets)
            {
                var aggregatedBucket = 0;

                BigInteger remains = row.Key;

                while (remains > 0)
                {
                    if (remains % 2 == 1)
                    {
                        aggregatedBucket++;
                    }
                    remains /= 2;
                }

                if (!ret.ContainsKey(aggregatedBucket))
                {
                    ret.Add(aggregatedBucket, new List<IReadOnlyDictionary<long, object[]>>());
                }

                List<IReadOnlyDictionary<long, object[]>> list = ret[aggregatedBucket];
                list.AddRange(row.Value);
            }

            return ret;
        }

        /// <summary>
        ///     TODO: from http://www.remondo.net/calculate-mean-median-mode-averages-csharp/
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        public static double Median(IEnumerable<double> list)
        {
            int midIndex;
            return Median(list, out midIndex);
        }

        /// <summary>
        ///     TODO: based on http://www.remondo.net/calculate-mean-median-mode-averages-csharp/
        /// </summary>
        /// <param name="list"></param>
        /// <param name="midIndex"></param>
        /// <returns></returns>
        public static double Median(IEnumerable<double> list, out int midIndex)
        {
            List<double> orderedList = list.OrderBy(numbers => numbers).ToList();

            int listSize = orderedList.Count;
            double result;

            midIndex = listSize / 2;

            if (listSize % 2 == 0) // even
            {
                result = ((orderedList.ElementAt(midIndex - 1) + orderedList.ElementAt(midIndex)) / 2);
            }
            else // odd
            {
                result = orderedList.ElementAt(midIndex);
            }

            return result;
        }
    }
}