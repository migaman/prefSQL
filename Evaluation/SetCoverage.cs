namespace prefSQL.Evaluation
{
    using System;
    using System.Collections.Generic;

    public sealed class SetCoverage
    {
        /// <summary>
        ///     Determines the percentage of objects in normalizedDataToBeCovered covered by objects in
        ///     normalizedDataCoveringDataToBeCovered according to W.-T. Balke, J. X. Zheng, and U. Güntzer (2005).
        /// </summary>
        /// <remarks>
        ///     Publication:
        ///     W.-T. Balke, J. X. Zheng, and U. Güntzer, “Approaching the Efficient Frontier: Cooperative Database Retrieval Using
        ///     High-Dimensional Skylines,” in Lecture Notes in Computer Science, Database Systems for Advanced Applications, D.
        ///     Hutchison, T. Kanade, J. Kittler, J. M. Kleinberg, F. Mattern, J. C. Mitchell, M. Naor, O. Nierstrasz, C. Pandu
        ///     Rangan, B. Steffen, M. Sudan, D. Terzopoulos, D. Tygar, M. Y. Vardi, G. Weikum, L. Zhou, B. C. Ooi, and X. Meng,
        ///     Eds, Berlin, Heidelberg: Springer Berlin Heidelberg, 2005, pp. 410–421.
        /// </remarks>
        /// <param name="normalizedDataToBeCovered">
        ///     The set of objects which should be covered by normalizedDataCoveringDataToBeCovered.
        /// </param>
        /// <param name="normalizedDataCoveringDataToBeCovered">The set of objects which should cover normalizedDataToBeCovered.</param>
        /// <param name="useColumns">
        ///     Array indices which should be used for the distance calculation. Useful if the object consists
        ///     of more dimensions than those over which the distance should be calculated, e.g. if the objects are database
        ///     objects with additional columns.
        /// </param>
        /// <returns>
        ///     The percentage of objects in normalizedDataToBeCovered covered by objects in
        ///     normalizedDataCoveringDataToBeCovered, i.e., the percentage of objects from normalizedDataCoveringDataToBeCovered
        ///     assigned to objects in normalizedDataToBeCovered. 0 = no objects covered, 1 = all objects covered.
        /// </returns>
        public static double GetCoverage(IReadOnlyDictionary<long, object[]> normalizedDataToBeCovered,
            IReadOnlyDictionary<long, object[]> normalizedDataCoveringDataToBeCovered, int[] useColumns)
        {
            var keysOfCoveredObjects = new HashSet<long>();

            Dictionary<long, double[]> normalizedDataToBeCoveredDouble;
            Dictionary<long, double[]> normalizedDataCoveringDataToBeCoveredDouble;
            ConvertDatabaseObjectsToDouble(out normalizedDataToBeCoveredDouble, normalizedDataToBeCovered,
                normalizedDataCoveringDataToBeCovered, useColumns, out normalizedDataCoveringDataToBeCoveredDouble);

            // for each object in normalizedDataCoveringDataToBeCovered, find its corresponding, nearest (i.e., covered) object
            foreach (KeyValuePair<long, double[]> coveringObject in normalizedDataCoveringDataToBeCoveredDouble)
            {
                long coveredObjectKey = GetCoveredObject(coveringObject.Value, normalizedDataToBeCoveredDouble).Item1;
                keysOfCoveredObjects.Add(coveredObjectKey);
            }

            return (double) keysOfCoveredObjects.Count / normalizedDataToBeCovered.Count;
        }

        private static void ConvertDatabaseObjectsToDouble(
            out Dictionary<long, double[]> normalizedDataToBeCoveredDouble,
            IReadOnlyDictionary<long, object[]> normalizedDataToBeCovered,
            IReadOnlyDictionary<long, object[]> normalizedDataCoveringDataToBeCovered,
            int[] useColumns, out Dictionary<long, double[]> normalizedDataCoveringDataToBeCoveredDouble)
        {
            normalizedDataToBeCoveredDouble = new Dictionary<long, double[]>();

            foreach (KeyValuePair<long, object[]> objectToBeCovered in normalizedDataToBeCovered)
            {
                var rowDouble = new double[useColumns.Length];

                for (var i = 0; i < useColumns.Length; i++)
                {
                    int index = useColumns[i];
                    rowDouble[i] = (double) objectToBeCovered.Value[index];
                }
                normalizedDataToBeCoveredDouble.Add(objectToBeCovered.Key, rowDouble);
            }

            normalizedDataCoveringDataToBeCoveredDouble = new Dictionary<long, double[]>();

            foreach (KeyValuePair<long, object[]> objectCoveringDataToBeCovered in normalizedDataCoveringDataToBeCovered
                )
            {
                var rowDouble = new double[useColumns.Length];

                for (var i = 0; i < useColumns.Length; i++)
                {
                    int index = useColumns[i];
                    rowDouble[i] = (double) objectCoveringDataToBeCovered.Value[index];
                }
                normalizedDataCoveringDataToBeCoveredDouble.Add(objectCoveringDataToBeCovered.Key, rowDouble);
            }
        }

        /// <summary>
        ///     TODO: rephrase since all distances returned
        ///     Calculates the representation error of normalizedDataCoveringDataToBeCovered according to Y. Tao, L. Ding, X. Lin,
        ///     and J. Pei (2009).
        /// </summary>
        /// <remarks>
        ///     The representation error is defined as the maximum of the minimum distances between each object in
        ///     normalizedDataToBeCovered and their covering object in normalizedDataCoveringDataToBeCovered.
        ///     Publication:
        ///     Y. Tao, L. Ding, X. Lin, and J. Pei, “Distance-Based Representative Skyline,” in 2009 IEEE 25th International
        ///     Conference on Data Engineering (ICDE), 2009, pp. 892–903.
        /// </remarks>
        /// <param name="normalizedDataToBeCovered">
        ///     The set of objects which should be covered by normalizedDataCoveringDataToBeCovered.
        /// </param>
        /// <param name="normalizedDataCoveringDataToBeCovered">The set of objects which should cover normalizedDataToBeCovered.</param>
        /// <param name="useColumns">
        ///     Array indices which should be used for the distance calculation. Useful if the object consists
        ///     of more dimensions than those over which the distance should be calculated, e.g. if the objects are database
        ///     objects with additional columns.
        /// </param>
        /// <returns>
        ///     The representation error of normalizedDataCoveringDataToBeCovered, i.e., the maximum of the minimum distances
        ///     between each object in normalizedDataToBeCovered and their covering object in
        ///     normalizedDataCoveringDataToBeCovered.
        /// </returns>
        public static Dictionary<long, double>.ValueCollection GetRepresentationError(
            IReadOnlyDictionary<long, object[]> normalizedDataToBeCovered,
            IReadOnlyDictionary<long, object[]> normalizedDataCoveringDataToBeCovered, int[] useColumns)
        {
            var distances = new Dictionary<long, double>();

            Dictionary<long, double[]> normalizedDataToBeCoveredDouble;
            Dictionary<long, double[]> normalizedDataCoveringDataToBeCoveredDouble;
            ConvertDatabaseObjectsToDouble(out normalizedDataToBeCoveredDouble, normalizedDataToBeCovered,
                normalizedDataCoveringDataToBeCovered, useColumns, out normalizedDataCoveringDataToBeCoveredDouble);

            // for each object in normalizedDataToBeCovered, find its corresponding, nearest (i.e., covering) object
            foreach (KeyValuePair<long, double[]> objectToBeCovered in normalizedDataToBeCoveredDouble)
            {
                double distance =
                    GetCoveredObject(objectToBeCovered.Value, normalizedDataCoveringDataToBeCoveredDouble).Item2;

                distances.Add(objectToBeCovered.Key, distance);
            }

            return distances.Values;
        }

        /// <summary>
        ///     Determines the object closest to the coveringObject, i.e., the object in normalizedDataToBeCovered which is
        ///     "covered" by the coveringObject, making coveringObject its nearest representative.
        /// </summary>
        /// <param name="coveringObject">
        ///     An object covering another object in normalizedDataToBeCovered. The latter object is
        ///     covered by the former if its distance to coveringObject is minimal.
        /// </param>
        /// <param name="normalizedDataToBeCovered">
        ///     The set examined for coverage. The coveringObject covers the object in
        ///     normalizedDataToBeCovered when its distance to coveringObject is minimal.
        /// </param>
        /// <returns>A Tuple with the key of the covered object in Item1 and its distance to coveringObject in Item2.</returns>
        internal static Tuple<long, double> GetCoveredObject(double[] coveringObject,
            Dictionary<long, double[]> normalizedDataToBeCovered)
        {
            long minimumDistanceObjectKey = -1;
            double minimumDistance = double.MaxValue;
            double breakOnLimit = double.MaxValue;

            foreach (KeyValuePair<long, double[]> potentiallyCoveredObject in normalizedDataToBeCovered)
            {
                double euclideanDistance = CalculateEuclideanDistance(coveringObject, potentiallyCoveredObject.Value,
                    breakOnLimit);

                if (euclideanDistance < minimumDistance)
                {
                    minimumDistance = euclideanDistance;
                    minimumDistanceObjectKey = potentiallyCoveredObject.Key;
                    breakOnLimit = minimumDistance * minimumDistance;
                }
            }

            return new Tuple<long, double>(minimumDistanceObjectKey, minimumDistance);
        }

        /// <summary>
        ///     Calculates the Euclidean Distance between two objects.
        /// </summary>
        /// <param name="item1">First object used for distance calculation.</param>
        /// <param name="item2">Second object used for distance calculation.</param>
        /// <returns>The Euclidean distance between item1 and item2.</returns>
        internal static double CalculateEuclideanDistance(double[] item1, double[] item2)
        {
            return CalculateEuclideanDistance(item1, item2, double.MaxValue);
        }

        /// <summary>
        ///     Calculates the Euclidean Distance between two objects. Returns double.MaxValue if the distance is greater than the
        ///     specified breakOnLimit.
        /// </summary>
        /// <param name="item1">First object used for distance calculation.</param>
        /// <param name="item2">Second object used for distance calculation.</param>
        /// <param name="breakOnLimit">
        ///     If the distance is greater than this parameter, double.MaxValue is returned => faster
        ///     performance when searching for a minimum distance between a set of objects.
        /// </param>
        /// <returns>The Euclidean distance between item1 and item2, or double.MaxValue.</returns>
        internal static double CalculateEuclideanDistance(double[] item1, double[] item2, double breakOnLimit)
        {
            double sum = 0;
            int length = item1.Length;

            for (var i = 0; i < length; i++)
            {
                double distance = item1[i] - item2[i];
                sum += distance * distance;
                if (sum > breakOnLimit)
                {
                    return double.MaxValue;
                }
            }

            return Math.Sqrt(sum);
        }
    }
}