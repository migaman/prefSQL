namespace prefSQL.SQLParserTest
{
    using System.Collections.Generic;
    using System.Numerics;

    internal sealed class ClusterAnalysis
    {
        public static IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>> GetBuckets(
            IReadOnlyDictionary<long, object[]> normalizedBaseData, int[] useColumns)
        {
            var bucketCount = 2;
            var ret = new Dictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>();

            foreach (KeyValuePair<long, object[]> row in normalizedBaseData)
            {
                BigInteger bucket = BigInteger.Zero;

                for (var i = 0; i < useColumns.Length; i++)
                {
                    int column = useColumns[i];
                    if ((double) row.Value[column] >= .5)
                    {
                        bucket += BigInteger.Pow(bucketCount, i);
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

        public static IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>> GetAggregatedBuckets(
            IReadOnlyDictionary<long, object[]> normalizedBaseData, int[] useColumns)
        {
            return GetAggregatedBuckets(GetBuckets(normalizedBaseData, useColumns));
        }

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