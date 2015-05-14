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
        /// <returns></returns>
        public DataTable GetSkylineTable(string dbConnection, string query, string operators, int numberOfRecords,
            bool hasIncomparable, string[] additionalParameters, SkylineStrategy skylineStrategy, int subspacesCount,
            int subspaceDimension)
        {
            var skylineAlgorithmParameters = new SkylineAlgorithmParameters(operators, numberOfRecords, hasIncomparable,
                additionalParameters);

            DataTable fullDataTable = Helper.GetDataTableFromSQL(query, dbConnection, Provider);
            fullDataTable.Columns.Add("_internalArtificialUniqueRowIdentifier", typeof (long));
            int rowIdentifierColumnIndex = fullDataTable.Columns.Count - 1;

            ConfigureUtility(subspacesCount, subspaceDimension, skylineAlgorithmParameters.OperatorsCollection.Count,
                rowIdentifierColumnIndex);

            //AddInternalArtificialUniqueRowIdentifier(fullDataTable, rowIdentifierColumnIndex);

            //IReadOnlyDictionary<long, object[]> database = Helper.GetDictionaryFromDataTable(fullDataTable,
            //rowIdentifierColumnIndex);

            IDictionary<long, object[]> database = new Dictionary<long, object[]>();
            var count = 0;
            foreach (DataRow row in fullDataTable.Rows)
            {
                row[rowIdentifierColumnIndex] = count;
                database.Add(count, row.ItemArray);
                count++;
            }
            //List<DataRow> dataTableRowList = fullDataTable.Rows.Cast<DataRow>().ToList();
            //Write all attributes to a Object-Array
            //Profiling: This is much faster (factor 2) than working with the SQLReader

            //List<object[]> objectArrayFromDataTableOrig = dataTableRowList.Select(dataRow => dataRow.ItemArray).ToList();
            //IReadOnlyDictionary<long, object[]> database = objectArrayFromDataTableOrig.ToDictionary(dataRow => (long)dataRow[rowIdentifierColumnIndex]);

            var dataTableTemplate = new DataTable();
            SqlDataRecord dataRecordTemplate = Helper.BuildDataRecord(fullDataTable,
                skylineAlgorithmParameters.OperatorsCollection.ToArray(), dataTableTemplate);

            DataTable skylineDataTableReturn = GetSkyline(database, dataTableTemplate, dataRecordTemplate,
                skylineStrategy,
                skylineAlgorithmParameters);

            RemoveInternalArtificialUniqueRowIdentifier(skylineDataTableReturn);

            return skylineDataTableReturn;
        }

        private static void RemoveInternalArtificialUniqueRowIdentifier(DataTable dataTable)
        {
            dataTable.Columns.RemoveAt(dataTable.Columns.Count - 1);
        }

        private static void AddInternalArtificialUniqueRowIdentifier(DataTable dataTable, int rowIdentifierColumnIndex)
        {
            var count = 0;
            foreach (DataRow row in dataTable.Rows)
            {
                row[rowIdentifierColumnIndex] = count++;
            }
        }

        private void ConfigureUtility(int subspacesCount, int subspaceDimension, int allPreferencesCount,
            int rowIdentifierColumnIndex)
        {
            Utility.SubspacesCount = subspacesCount;
            Utility.SubspaceDimension = subspaceDimension;
            Utility.AllPreferencesCount = allPreferencesCount;
            Utility.RowIdentifierColumnIndex = rowIdentifierColumnIndex;
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
        internal DataTable GetSkyline(IDictionary<long, object[]> database, DataTable dataTableTemplate,
            SqlDataRecord dataRecordTemplate, SkylineStrategy skylineStrategy,
            SkylineAlgorithmParameters skylineAlgorithmParameters)
        {
            DataTable skylineSampleReturn = dataTableTemplate.Clone();
            var skylineSampleFinalDatabase = new Dictionary<long, object[]>();

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

                IDictionary<long, object[]> subspaceDatabase = GetDatabaseFromDataTable(database, subspaceDataTable);

                IEnumerable<HashSet<long>> rowsWithEqualValuesWithRespectToSubspaceColumns =
                    CompareEachRowWithRespectToSubspaceColumnsPairwise(subspaceDatabase, subspace);

                foreach (HashSet<long> rowsWithEqualValues in rowsWithEqualValuesWithRespectToSubspaceColumns)
                {
                    subpaceOperators = string.Join(";",
                        GetOperatorsWithIgnoredEntries(skylineAlgorithmParameters.OperatorsCollection.ToArray(),
                            GetSubspaceComplement(subspace)));
                    skylineAlgorithmSubspaceParameters = new SkylineAlgorithmParameters(subpaceOperators,
                        skylineAlgorithmParameters);

                    IDictionary<long, object[]> rowsWithEqualValuesDatabase = GetSubsetOfDatabase(database,
                        rowsWithEqualValues);

                    sw.Stop();
                    TimeMilliseconds += sw.ElapsedMilliseconds;
                    DataTable subspaceComplementDataTable = GetSkylineTable(rowsWithEqualValuesDatabase,
                        dataTableTemplate,
                        dataRecordTemplate, skylineStrategy, skylineAlgorithmSubspaceParameters);
                    TimeMilliseconds += skylineStrategy.TimeMilliseconds;
                    sw.Restart();

                    IDictionary<long, object[]> subspaceComplementDatabase = GetDatabaseFromDataTable(database,
                        subspaceComplementDataTable);

                    RemoveDominatedObjects(rowsWithEqualValues,
                        subspaceComplementDatabase, subspaceDatabase);
                }

                MergeSubspaceSkylineIntoFinalSkylineSample(subspaceDatabase, skylineSampleFinalDatabase);
            }

            foreach (KeyValuePair<long, object[]> item in skylineSampleFinalDatabase)
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

        private static IDictionary<long, object[]> GetSubsetOfDatabase(
            IDictionary<long, object[]> database,
            IEnumerable<long> databaseSubsetKeys)
        {
            return databaseSubsetKeys.ToDictionary(row => row, row => database[row]);
        }

        private static DataTable GetSkylineTable(IDictionary<long, object[]> subspaceDatabase,
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
            IDictionary<long, object[]> sourceDatabase,
            IDictionary<long, object[]> destinationDatabase)
        {
            foreach (KeyValuePair<long, object[]> subspaceDataTableOrigObject in sourceDatabase)
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
        /// <param name="dataTable"></param>
        /// <returns></returns>
        private IDictionary<long, object[]> GetDatabaseFromDataTable(
            IDictionary<long, object[]> database, DataTable dataTable)
        {
            int rowIdentifierColumnIndex = Utility.RowIdentifierColumnIndex - Utility.AllPreferencesCount;
            return dataTable.Rows.Cast<DataRow>()
                .ToDictionary(row => (long) row[rowIdentifierColumnIndex],
                    row => database[(long) row[rowIdentifierColumnIndex]]);
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
        /// <remarks>TODO: remarks about performance (this is the most time consuming method besides the actual skyline algorithm called</remarks>
        /// <param name="subspaceDatabase"></param>
        /// <param name="columnsUsedInSubspace"></param>
        /// <returns></returns>
        private IEnumerable<HashSet<long>> CompareEachRowWithRespectToSubspaceColumnsPairwise(
            IDictionary<long, object[]> subspaceDatabase, IEnumerable<int> columnsUsedInSubspace)
        {
            var equalRowsWithRespectToSubspaceColumns =
                new Dictionary<long[], HashSet<int>>(new ArrayEqualityComparer());

            List<int> columnsUsedInSubspaceList = columnsUsedInSubspace.ToList();
            int columnsInSubspaceCount = columnsUsedInSubspaceList.Count;

            List<object[]> sortedDatabaseList = subspaceDatabase.Values.ToList();
            sortedDatabaseList.Sort(
                (row1, row2) =>
                    ((long) row1[columnsUsedInSubspaceList[0]]).CompareTo(
                        (long) row2[columnsUsedInSubspaceList[0]]));

            var usedKeys = new HashSet<int>();

            int sortedColumn = columnsUsedInSubspaceList[0];

            int rowIdentifierIndex = Utility.RowIdentifierColumnIndex;

            for (var i = 0; i < sortedDatabaseList.Count; i++)
            {
                object[] iValue = sortedDatabaseList[i];

                for (int j = i + 1; j < sortedDatabaseList.Count; j++)
                {
                    if (usedKeys.Contains(j))
                    {
                        continue;
                    }

                    object[] jValue = sortedDatabaseList[j];

                    if ((long) jValue[sortedColumn] > (long) iValue[sortedColumn])
                    {
                        j = sortedDatabaseList.Count;
                        continue;
                    }

                    var isEqual = true;

                    var equalCombination = new long[columnsInSubspaceCount];

                    for (var k = 0; k < columnsInSubspaceCount; k++)
                    {
                        int column = columnsUsedInSubspaceList[k];
                        var iColumnValue = (long) iValue[column];
                        var jColumnValue = (long) jValue[column];
                        equalCombination[k] = iColumnValue;

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

                    if (!equalRowsWithRespectToSubspaceColumns.ContainsKey(equalCombination))
                    {
                        equalRowsWithRespectToSubspaceColumns.Add(equalCombination, new HashSet<int>());
                    }

                    HashSet<int> equalRowsWithRespectToSubspaceColumn =
                        equalRowsWithRespectToSubspaceColumns[equalCombination];

                    equalRowsWithRespectToSubspaceColumn.Add(i);
                    equalRowsWithRespectToSubspaceColumn.Add(j);

                    usedKeys.Add(j);
                }
            }

            var ret = new List<HashSet<long>>();

            foreach (KeyValuePair<long[], HashSet<int>> i in equalRowsWithRespectToSubspaceColumns)
            {
                var j = new HashSet<long>();
                foreach (int ii in i.Value)
                {
                    j.Add((long) sortedDatabaseList[ii][rowIdentifierIndex]);
                }
                ret.Add(j);
            }

            return ret;
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
            IEnumerable<long> potentiallyDominatedObjects,
            IDictionary<long, object[]> dominatingObjectsDatabase, IDictionary<long, object[]> destinationDatabase)
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