namespace prefSQL.Evaluation
{
    using System;
    using System.Collections.Generic;

    internal sealed class SetCoverage
    {
        public static double GetCoverage(IReadOnlyDictionary<long, object[]> normalizedBaseData,
            IReadOnlyDictionary<long, object[]> normalizedSampleData, int[] useColumns)
        {
            var assignments = new HashSet<long>();

            foreach (KeyValuePair<long, object[]> o in normalizedSampleData)
            {
                long objectWithMinimumEuclideanDistanceKey = GetObjectWithMinimumEuclideanDistanceKey(o.Value,
                    normalizedBaseData, useColumns);
                if (!assignments.Contains(objectWithMinimumEuclideanDistanceKey))
                {
                    assignments.Add(objectWithMinimumEuclideanDistanceKey);
                }
            }

            return (double) assignments.Count / normalizedBaseData.Count;
        }

        internal static long GetObjectWithMinimumEuclideanDistanceKey(object[] o,
            IReadOnlyDictionary<long, object[]> normalizedBaseData, int[] useColumns)
        {
            long minimumDistanceObjectKey = -1;
            double minimumDistance = double.MaxValue;

            foreach (KeyValuePair<long, object[]> oo in normalizedBaseData)
            {
                double euclideanDistance = CalculateEuclideanDistance(o, oo.Value, useColumns);
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

            foreach (int index in useColumns)
            {
                double dist = (double) o[index] - (double) oo[index];
                sum += dist * dist;
            }

            return Math.Sqrt(sum);
        }
    }
}