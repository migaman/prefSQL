namespace prefSQL.SQLParserTest
{
    using System;
    using System.Collections.Generic;

    internal sealed class SetCoverage
    {
        public static double GetCoverage(Dictionary<int, object[]> normalizedBaseData,
            Dictionary<int, object[]> normalizedSampleData, int[] useColumns)
        {
            var assignments = new HashSet<int>();

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

        internal static int GetObjectWithMinimumEuclideanDistanceKey(object[] o, Dictionary<int, object[]> normalizedBaseData, int[] useColumns)
        {
            var minimumDistanceObjectKey = -1;
            var minimumDistance = Double.MaxValue;

            foreach (var oo in normalizedBaseData)
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