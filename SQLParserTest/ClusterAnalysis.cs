namespace prefSQL.SQLParserTest
{
    using System.Collections.Generic;
    using System.Numerics;

    internal sealed class ClusterAnalysis
    {
        public static Dictionary<BigInteger, List<Dictionary<int, object[]>>> GetBuckets(
            Dictionary<int, object[]> normalizedBaseData, int[] useColumns)
        {
            var bucketCount = 2;
            var ret = new Dictionary<BigInteger, List<Dictionary<int, object[]>>>();

            foreach (KeyValuePair<int, object[]> row in normalizedBaseData)
            {
                BigInteger bucket = BigInteger.Zero;

                for (int i = 0; i < useColumns.Length; i++)
                {
                    int column = useColumns[i];
                    if ((double) row.Value[column] >= .5)
                    {
                        bucket += BigInteger.Pow(bucketCount,i);
                    }
                }
                
                if (!ret.ContainsKey(bucket))
                {
                    ret.Add(bucket,new List<Dictionary<int, object[]>>());
                }

                List<Dictionary<int, object[]>> list = ret[bucket];
                list.Add(new Dictionary<int, object[]> { { row.Key, row.Value } });
            }

            return ret;
        }

        public static Dictionary<BigInteger, List<Dictionary<int, object[]>>> GetAggregatedBuckets(
            Dictionary<int, object[]> normalizedBaseData, int[] useColumns)
        {
            return GetAggregatedBuckets(GetBuckets(normalizedBaseData, useColumns));
        }

        public static Dictionary<BigInteger, List<Dictionary<int, object[]>>> GetAggregatedBuckets(
            Dictionary<BigInteger, List<Dictionary<int, object[]>>> buckets)
        {
            var ret = new Dictionary<BigInteger, List<Dictionary<int, object[]>>>();

            foreach (KeyValuePair<BigInteger, List<Dictionary<int, object[]>>> row in buckets)
            {
                BigInteger aggregatedBucket = BigInteger.Zero;

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
                    ret.Add(aggregatedBucket, new List<Dictionary<int, object[]>>());
                }

                List<Dictionary<int, object[]>> list = ret[aggregatedBucket];
                list.AddRange(row.Value);
            }

            return ret;
        }
    }
}