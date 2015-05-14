namespace prefSQL.SQLSkyline.SamplingSkyline
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using Microsoft.SqlServer.Server;

    /// <summary>
    ///     Implementation of sampling skyline algorithm according to the algorithm pseudocode in Balke, W.-T., Zheng, J. X., &
    ///     Güntzer, U. (2005).
    /// </summary>
    /// <remarks>
    ///     Balke, W.-T., Zheng, J. X., & Güntzer, U. (2005). Approaching the Efficient Frontier: Cooperative Database
    ///     Retrieval Using High-Dimensional Skylines. In D. Hutchison, T. Kanade, J. Kittler, J. M. Kleinberg, F. Mattern, J.
    ///     C. Mitchell, . . . X. Meng (Eds.), Lecture Notes in Computer Science. Database Systems for Advanced Applications
    ///     (pp. 410–421). Berlin, Heidelberg: Springer Berlin Heidelberg.
    ///     This implementation re-uses the already implemented algorithms of the prefSQL framework to calculate the necessary
    ///     subspace skylines.
    /// </remarks>
    public sealed class SamplingSkyline
    {
        private readonly SamplingSkylineUtility _utility;

        /// <summary>
        ///     TODO: comment
        /// </summary>
        public long TimeMilliseconds;

        /// <summary>
        ///     TODO: comment
        /// </summary>
        public SamplingSkyline() : this(new SamplingSkylineUtility())
        {
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        /// <param name="utility"></param>
        internal SamplingSkyline(SamplingSkylineUtility utility)
        {
            _utility = utility;
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        public string Provider { get; set; }

        private SamplingSkylineUtility Utility
        {
            get { return _utility; }
        }

        public long NumberOfOperations { get; set; }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        /// <param name="dbConnection"></param>
        /// <param name="query"></param>
        /// <param name="operators"></param>
        /// <param name="numberOfRecords"></param>
        /// <param name="hasIncomparable"></param>
        /// <param name="additionalParameters"></param>
        /// <param name="skylineStrategy"></param>
        /// <param name="subspacesCount"></param>
        /// <param name="subspaceDimension"></param>
        /// <param name="uniqueIdColumnIndex"></param>
        /// <returns></returns>
        public DataTable GetSkylineTable(string dbConnection, string query, string operators, int numberOfRecords,
            bool hasIncomparable, string[] additionalParameters, SkylineStrategy skylineStrategy, int subspacesCount,
            int subspaceDimension, int uniqueIdColumnIndex)
        {
            var skylineAlgorithmParameters = new SkylineAlgorithmParameters(operators, numberOfRecords, hasIncomparable,
                additionalParameters);

            ConfigureUtility(subspacesCount, subspaceDimension, skylineAlgorithmParameters.OperatorsCollection.Count);

            DataTable fullDataTable = Helper.GetDataTableFromSQL(query, dbConnection, Provider);
            IReadOnlyDictionary<int, object[]> database = Helper.GetDictionaryFromDataTable(fullDataTable,
                Utility.AllPreferencesCount + uniqueIdColumnIndex);
            var dataTableTemplate = new DataTable();
            SqlDataRecord dataRecordTemplate = Helper.BuildDataRecord(fullDataTable,
                skylineAlgorithmParameters.OperatorsCollection.ToArray(), dataTableTemplate);

            return GetSkyline(database, dataTableTemplate, dataRecordTemplate, skylineStrategy,
                skylineAlgorithmParameters);
        }

        private void ConfigureUtility(int subspacesCount, int subspaceDimension, int allPreferencesCount)
        {
            Utility.SubspacesCount = subspacesCount;
            Utility.SubspaceDimension = subspaceDimension;
            Utility.AllPreferencesCount = allPreferencesCount;
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        /// <param name="database"></param>
        /// <param name="dataTableTemplate"></param>
        /// <param name="dataRecordTemplate"></param>
        /// <param name="skylineStrategy"></param>
        /// <param name="skylineAlgorithmParameters"></param>
        /// <returns></returns>
        internal DataTable GetSkyline(IReadOnlyDictionary<int, object[]> database, DataTable dataTableTemplate,
            SqlDataRecord dataRecordTemplate, SkylineStrategy skylineStrategy,
            SkylineAlgorithmParameters skylineAlgorithmParameters)
        {
            DataTable skylineSampleReturn = dataTableTemplate.Clone();
            var skylineSampleFinalDatabase = new Dictionary<int, object[]>();

            TimeMilliseconds = 0;
            var sw = new Stopwatch();
            sw.Start();

            foreach (HashSet<int> subspace in Utility.Subspaces)
            {
                string subpaceOperators = string.Join(";",
                    GetOperatorsWithIgnoredEntries(skylineAlgorithmParameters.OperatorsCollection.ToArray(), subspace));
                var skylineAlgorithmSubspaceParameters = new SkylineAlgorithmParameters(subpaceOperators,
                    skylineAlgorithmParameters);

                sw.Stop();
                TimeMilliseconds += sw.ElapsedMilliseconds;
                DataTable subspaceDataTable = GetSkylineTable(database, dataTableTemplate, dataRecordTemplate,
                    skylineStrategy, skylineAlgorithmSubspaceParameters);
                TimeMilliseconds += skylineStrategy.TimeMilliseconds;
                sw.Restart();

                IDictionary<int, object[]> subspaceDatabase = GetDatabaseFromDataTable(database, subspaceDataTable);

                List<HashSet<int>> rowsWithEqualValuesWithRespectToSubspaceColumns =
                    CompareEachRowWithRespectToSubspaceColumnsPairwise(subspaceDatabase, subspace);

                foreach (HashSet<int> rowsWithEqualValues in rowsWithEqualValuesWithRespectToSubspaceColumns)
                {
                    subpaceOperators = string.Join(";",
                        GetOperatorsWithIgnoredEntries(skylineAlgorithmParameters.OperatorsCollection.ToArray(),
                            GetSubspaceComplement(subspace)));
                    skylineAlgorithmSubspaceParameters = new SkylineAlgorithmParameters(subpaceOperators,
                        skylineAlgorithmParameters);

                    IReadOnlyDictionary<int, object[]> rowsWithEqualValuesDatabase = GetSubsetOfDatabase(database,
                        rowsWithEqualValues);

                    sw.Stop();
                    TimeMilliseconds += sw.ElapsedMilliseconds;
                    DataTable subspaceComplementDataTable = GetSkylineTable(rowsWithEqualValuesDatabase,
                        dataTableTemplate,
                        dataRecordTemplate, skylineStrategy, skylineAlgorithmSubspaceParameters);
                    TimeMilliseconds += skylineStrategy.TimeMilliseconds;
                    sw.Restart();

                    IDictionary<int, object[]> subspaceComplementDatabase = GetDatabaseFromDataTable(database,
                        subspaceComplementDataTable);

                    RemoveDominatedObjects(rowsWithEqualValues,
                        subspaceComplementDatabase, subspaceDatabase);
                }

                MergeSubspaceSkylineIntoFinalSkylineSample(subspaceDatabase, skylineSampleFinalDatabase);
            }

            foreach (KeyValuePair<int, object[]> item in skylineSampleFinalDatabase)
            {
                DataRow row = skylineSampleReturn.NewRow();
                for (int i = Utility.AllPreferencesCount; i < item.Value.Length; i++)
                {
                    row[i - Utility.AllPreferencesCount] = item.Value[i];
                }
                skylineSampleReturn.Rows.Add(row);
            }

            sw.Stop();
            TimeMilliseconds += sw.ElapsedMilliseconds;

            return skylineSampleReturn;
        }

        private IReadOnlyDictionary<int, object[]> GetSubsetOfDatabase(IReadOnlyDictionary<int, object[]> database,
            IEnumerable<int> erwrtsc)
        {
            return erwrtsc.ToDictionary(erw => erw, erw => database[erw]);
        }

        private static DataTable GetSkylineTable(IReadOnlyDictionary<int, object[]> subspaceDatabase,
            DataTable dataTableTemplate,
            SqlDataRecord dataRecordTemplate, SkylineStrategy skylineStrategy,
            SkylineAlgorithmParameters skylineAlgorithmParameters)
        {
            DataTable skylineDataTable =
                skylineStrategy.GetSkylineTableBackdoorSample(subspaceDatabase.Values.ToList(),
                    dataTableTemplate, dataRecordTemplate, skylineAlgorithmParameters.Operators,
                    skylineAlgorithmParameters.NumberOfRecords,
                    skylineAlgorithmParameters.HasIncomparable,
                    skylineAlgorithmParameters.AdditionalParameters.ToArray());
            return skylineDataTable;
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        /// <param name="sourceDatabase"></param>
        /// <param name="destinationDatabase"></param>
        private static void MergeSubspaceSkylineIntoFinalSkylineSample(
            IDictionary<int, object[]> sourceDatabase,
            IDictionary<int, object[]> destinationDatabase)
        {
            foreach (KeyValuePair<int, object[]> subspaceDataTableOrigObject in sourceDatabase)
            {
                if (!destinationDatabase.ContainsKey(subspaceDataTableOrigObject.Key))
                {
                    destinationDatabase.Add(subspaceDataTableOrigObject.Key, subspaceDataTableOrigObject.Value);
                }
            }
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        /// <param name="database"></param>
        /// <param name="reducedObjectArrayFromDataTable"></param>
        /// <returns></returns>
        private static IDictionary<int, object[]> GetDatabaseFromDataTable(
            IReadOnlyDictionary<int, object[]> database, DataTable reducedObjectArrayFromDataTable)
        {
            return reducedObjectArrayFromDataTable.Rows.Cast<DataRow>()
                .ToDictionary(row => (int) row[0], row => database[(int) row[0]]);
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        /// <param name="operators"></param>
        /// <param name="subspace"></param>
        /// <returns></returns>
        private static string[] GetOperatorsWithIgnoredEntries(string[] operators, ICollection<int> subspace)
        {
            var reducedOperators = new string[operators.Length];
            Array.Copy(operators, reducedOperators, operators.Length);
            for (var i = 0; i < operators.Length; i++)
            {
                reducedOperators[i] = operators[i];
                if (!subspace.Contains(i))
                {
                    reducedOperators[i] = "IGNORE";
                }
            }

            return reducedOperators;
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        /// <param name="subspaceDatabase"></param>
        /// <param name="columnsUsedInSubspace"></param>
        /// <returns></returns>
        private List<HashSet<int>> CompareEachRowWithRespectToSubspaceColumnsPairwise(
            IDictionary<int, object[]> subspaceDatabase, IEnumerable<int> columnsUsedInSubspace)
        {
            var equalRowsWithRespectToSubspaceColumns = new Dictionary<long[], HashSet<int>>(new ArrayEqualityComparer());

            List<int> columnsUsedInSubspaceList = columnsUsedInSubspace.ToList();
            int columnsInSubspaceCount = columnsUsedInSubspaceList.Count;

            List<object[]> myList = subspaceDatabase.Values.ToList();
            myList.Sort(
                (object1, object2) =>
                    ((long) object1[columnsUsedInSubspaceList[0]]).CompareTo(
                        (long) object2[columnsUsedInSubspaceList[0]]));

            int sortedColumn = columnsUsedInSubspaceList[0];

            int allPrefs = Utility.AllPreferencesCount;

            for (var i = 0; i < myList.Count; i++)
            {
                object[] iValue = myList[i];
                var iKey = (int) iValue[allPrefs];

                for (int j = i + 1; j < myList.Count; j++)
                {
                    object[] jValue = myList[j];

                    if ((long) jValue[sortedColumn] > (long) iValue[sortedColumn])
                    {
                        j = myList.Count;
                        continue;
                    }

                    var jKey = (int) jValue[allPrefs];

                    if (iKey == jKey)
                    {
                        continue;
                    }

                    var isEqual = true;

                    for (var k = 0; k < columnsInSubspaceCount; k++)
                    {
                        int column = columnsUsedInSubspaceList[k];
                        var iColumnValue = (long) iValue[column];
                        var jColumnValue = (long) jValue[column];

                        if (iColumnValue != jColumnValue)
                        {
                            isEqual = false;
                            break;
                        }
                    }

                    if (!isEqual)
                    {
                        continue;
                    }

                    var equalCombination = new long[columnsInSubspaceCount];

                    for (var k = 0; k < columnsInSubspaceCount; k++)
                    {
                        int column = columnsUsedInSubspaceList[k];
                        var iColumnValue = (long) iValue[column];
                        equalCombination[k] = iColumnValue;
                    }

                    if (!equalRowsWithRespectToSubspaceColumns.ContainsKey(equalCombination))
                    {
                        equalRowsWithRespectToSubspaceColumns.Add(equalCombination, new HashSet<int>());
                    }

                    HashSet<int> equalRowsWithRespectToSubspaceColumn =
                        equalRowsWithRespectToSubspaceColumns[equalCombination];

                    equalRowsWithRespectToSubspaceColumn.Add(iKey);
                    equalRowsWithRespectToSubspaceColumn.Add(jKey);
                }
            }

            return equalRowsWithRespectToSubspaceColumns.Values.ToList();
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        /// <param name="subspace"></param>
        /// <returns></returns>
        public HashSet<int> GetSubspaceComplement(HashSet<int> subspace)
        {
            var subspaceComplement = new HashSet<int>();
            for (var i = 0; i < Utility.AllPreferencesCount; i++)
            {
                if (!subspace.Contains(i))
                {
                    subspaceComplement.Add(i);
                }
            }
            return subspaceComplement;
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        /// <param name="potentiallyDominatedObjects"></param>
        /// <param name="dominatingObjectsDatabase"></param>
        /// <param name="destinationDatabase"></param>
        private static void RemoveDominatedObjects(
            IEnumerable<int> potentiallyDominatedObjects,
            IDictionary<int, object[]> dominatingObjectsDatabase, IDictionary<int, object[]> destinationDatabase)
        {
            foreach (
                int equalRow in
                    potentiallyDominatedObjects.Where(
                        equalRow => !dominatingObjectsDatabase.ContainsKey(equalRow)))
            {
                destinationDatabase.Remove(equalRow);
            }
        }

        /// <summary>
        ///     Compares by comparing each array dimension.
        /// </summary>
        private sealed class ArrayEqualityComparer : IEqualityComparer<long[]>
        {
            public bool Equals(long[] x, long[] y)
            {
                for (var i = 0; i < x.Length; i++)
                {
                    if (x[i] != y[i])
                    {
                        return false;
                    }
                }

                return true;
            }

            /// <summary>
            ///     Calculate hash code.
            /// </summary>
            /// <remarks>
            ///     Idea based on:
            ///     http://stackoverflow.com/questions/263400/what-is-the-best-algorithm-for-an-overridden-system-object-gethashcode/263416#263416
            /// </remarks>
            /// <param name="obj">Array to get hash code for.</param>
            /// <returns>Hash code.</returns>
            public int GetHashCode(long[] obj)
            {
                unchecked // Overflow is not a problem, just wrap
                {
                    var result = (int) 2166136261;
                    foreach (long l in obj)
                    {
                        result = result * 16777619 ^ l.GetHashCode();
                    }
                    return result;
                }
            }
        }
    }
}