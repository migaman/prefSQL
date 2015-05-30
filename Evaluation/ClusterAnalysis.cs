namespace prefSQL.Evaluation
{
    using System.Collections.Generic;
    using System.Numerics;

    public sealed class ClusterAnalysis
    {
        /// <summary>
        /// TODO W.-T. Balke, J. X. Zheng, and U. Güntzer (2005).
        /// </summary>
        /// <remarks>
        /// Publication:
        ///     W.-T. Balke, J. X. Zheng, and U. Güntzer, “Approaching the Efficient Frontier: Cooperative Database Retrieval Using
        ///     High-Dimensional Skylines,” in Lecture Notes in Computer Science, Database Systems for Advanced Applications, D.
        ///     Hutchison, T. Kanade, J. Kittler, J. M. Kleinberg, F. Mattern, J. C. Mitchell, M. Naor, O. Nierstrasz, C. Pandu
        ///     Rangan, B. Steffen, M. Sudan, D. Terzopoulos, D. Tygar, M. Y. Vardi, G. Weikum, L. Zhou, B. C. Ooi, and X. Meng,
        ///     Eds, Berlin, Heidelberg: Springer Berlin Heidelberg, 2005, pp. 410–421.
        /// </remarks>
        /// <param name="normalizedBaseData"></param>
        /// <param name="useColumns"></param>
        /// <returns></returns>
        public static IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>> GetBuckets(
            IReadOnlyDictionary<long, object[]> normalizedBaseData, int[] useColumns)
        {
            var ret = new Dictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>();

            foreach (KeyValuePair<long, object[]> row in normalizedBaseData)
            {
                BigInteger bucket = BigInteger.Zero;

                for (var i = 0; i < useColumns.Length; i++)
                {
                    int column = useColumns[i];
                    if ((double) row.Value[column] >= .5)
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
        /// TODO W.-T. Balke, J. X. Zheng, and U. Güntzer (2005).
        /// </summary>
        /// <remarks>
        /// Publication:
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

        /// <summary>
        /// TODO W.-T. Balke, J. X. Zheng, and U. Güntzer (2005).
        /// </summary>
        /// <remarks>
        /// Publication:
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
    }
}