using System;
using System.Collections.Generic;

namespace prefSQL.SQLParserTest
{
    internal sealed class SetCoverage
    {
        public static double GetCoverage(Dictionary<long, object[]> normalizedBaseData,
            Dictionary<long, object[]> normalizedSampleData, int[] useColumns)
        {
            var assignments = new HashSet<long>();

            foreach (var o in normalizedSampleData)
            {
                var objectWithMinimumEuclideanDistanceKey = GetObjectWithMinimumEuclideanDistanceKey(o.Value,
                    normalizedBaseData, useColumns);
                if (!assignments.Contains(objectWithMinimumEuclideanDistanceKey))
                {
                    assignments.Add(objectWithMinimumEuclideanDistanceKey);
                }
            }

            return (double)assignments.Count / normalizedBaseData.Count;
        }

        internal static long GetObjectWithMinimumEuclideanDistanceKey(object[] o, Dictionary<long, object[]> normalizedBaseData, int[] useColumns)
        {
            long minimumDistanceObjectKey = -1;
            double minimumDistance = double.MaxValue;

            foreach (KeyValuePair<long, object[]> oo in normalizedBaseData)
            {
                var euclideanDistance = CalculateEuclideanDistance(o, oo.Value, useColumns);
                if (euclideanDistance < minimumDistance)
                {
                    minimumDistance = euclideanDistance;
                    minimumDistanceObjectKey = oo.Key;
                }
            }

            return minimumDistanceObjectKey;
        }

        internal static double CalculateEuclideanDistance(object[] o, object[] oo, int[] useColumns)
        {
            double sum = 0;

            foreach (var index in useColumns)
            {
                var dist = (double)o[index] - (double)oo[index];
                sum += dist * dist;
            }
         
            return Math.Sqrt(sum);
        }
    }
}