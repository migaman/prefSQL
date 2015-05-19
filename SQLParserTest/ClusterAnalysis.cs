namespace prefSQL.SQLParserTest
{
    using System.Collections.Generic;
    using System.Numerics;

    internal sealed class ClusterAnalysis
    {
        public static Dictionary<BigInteger, List<Dictionary<long, object[]>>> GetBuckets(
            Dictionary<long, object[]> normalizedBaseData, int[] useColumns)
        {
            var bucketCount = 2;
            var ret = new Dictionary<BigInteger, List<Dictionary<long, object[]>>>();

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
                    ret.Add(bucket, new List<Dictionary<long, object[]>>());
                }

                List<Dictionary<long, object[]>> list = ret[bucket];
                list.Add(new Dictionary<long, object[]> {{row.Key, row.Value}});
            }

            return ret;
        }

        public static Dictionary<int, List<Dictionary<long, object[]>>> GetAggregatedBuckets(
            Dictionary<long, object[]> normalizedBaseData, int[] useColumns)
        {
            return GetAggregatedBuckets(GetBuckets(normalizedBaseData, useColumns));
        }

        public static Dictionary<int, List<Dictionary<long, object[]>>> GetAggregatedBuckets(
            Dictionary<BigInteger, List<Dictionary<long, object[]>>> buckets)
        {
            var ret = new Dictionary<int, List<Dictionary<long, object[]>>>();

            foreach (KeyValuePair<BigInteger, List<Dictionary<long, object[]>>> row in buckets)
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
                    ret.Add(aggregatedBucket, new List<Dictionary<long, object[]>>());
                }

                List<Dictionary<long, object[]>> list = ret[aggregatedBucket];
                list.AddRange(row.Value);
            }

            return ret;
        }
    }
}