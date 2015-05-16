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
            var column = new DataColumn
            {
                ColumnName = "_internalEqualValuesBucket",
                DataType = typeof (long),
                DefaultValue = 0
            };
            fullDataTable.Columns.Add(column);
            int equalValuesBucketColumnIndex = fullDataTable.Columns.Count - 1;

            ConfigureUtility(subspacesCount, subspaceDimension, skylineAlgorithmParameters.OperatorsCollection.Count,
                rowIdentifierColumnIndex, equalValuesBucketColumnIndex);

            AddInternalArtificialUniqueRowIdentifier(fullDataTable, rowIdentifierColumnIndex);

            IDictionary<long, object[]> database = Helper.GetDictionaryFromDataTable(fullDataTable,
                rowIdentifierColumnIndex);

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
            dataTable.Columns.RemoveAt(dataTable.Columns.Count - 1); // _internalEqualValuesBucket
            dataTable.Columns.RemoveAt(dataTable.Columns.Count - 1); // _internalArtificialUniqueRowIdentifier
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
            int rowIdentifierColumnIndex, int equalValuesBucketColumnIndex)
        {
            Utility.SubspacesCount = subspacesCount;
            Utility.SubspaceDimension = subspaceDimension;
            Utility.AllPreferencesCount = allPreferencesCount;
            Utility.RowIdentifierColumnIndex = rowIdentifierColumnIndex;
            Utility.EqualValuesBucketColumnIndex = equalValuesBucketColumnIndex;
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

                IEnumerable<long[]> subspaceForPairwiseComparison = GetSubspaceForPairwiseComparison(subspaceDatabase,
                    subspace);

                IEnumerable<HashSet<long>> rowsWithEqualValuesWithRespectToSubspaceColumns =
                    CompareEachRowWithRespectToSubspaceColumnsPairwise(subspaceForPairwiseComparison);

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

        private IEnumerable<long[]> GetSubspaceForPairwiseComparison(IDictionary<long, object[]> subspaceDatabase,
            ICollection<int> subspace)
        {
            var subspaceForPairwiseComparison = new List<long[]>();
            foreach (object[] row in subspaceDatabase.Values)
            {
                var rowForPairwiseComparison = new long[subspace.Count + 2];

                var count = 0;
                foreach (int sub in subspace)
                {
                    rowForPairwiseComparison[count] = (long) row[sub];
                    count++;
                }

                rowForPairwiseComparison[count] = (long) row[Utility.RowIdentifierColumnIndex];
                rowForPairwiseComparison[count + 1] = (long) row[Utility.EqualValuesBucketColumnIndex];

                subspaceForPairwiseComparison.Add(rowForPairwiseComparison);
            }
            return subspaceForPairwiseComparison;
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
        /// <remarks>
        ///     TODO: remarks about performance (this is the most time consuming method besides the actual skyline algorithm
        ///     called
        /// </remarks>
        /// <param name="subspaceDatabase"></param>
        /// <param name="columnsUsedInSubspace"></param>
        /// <returns></returns>
        private IEnumerable<HashSet<long>> CompareEachRowWithRespectToSubspaceColumnsPairwise(
            IEnumerable<long[]> subspaceDatabase)
        {
            var equalRowsWithRespectToSubspaceColumns = new Dictionary<long, HashSet<long>>();

            List<long[]> sortedDatabaseList = subspaceDatabase.ToList();
            int columnsInSubspaceCount = sortedDatabaseList[0].Length - 2;
            int equalValuesBucketColumnIndex = sortedDatabaseList[0].Length - 1;
            int rowIdentifierColumnIndex = sortedDatabaseList[0].Length - 2;

            sortedDatabaseList.Sort((row1, row2) => (row1[0]).CompareTo(row2[0]));

            List<long[]> sortedDatabaseList2 = sortedDatabaseList.GetRange(1, sortedDatabaseList.Count - 1);

            var jStartIndex = 0;

            var equalCombination = new long[columnsInSubspaceCount];

            int sortedDatabaseListCount = sortedDatabaseList.Count;

            for (var i = 0; i < sortedDatabaseListCount; i++)
            {
                long[] iValue = sortedDatabaseList[i];

                var sortedDatabaseList2Next = new List<long[]>();

                int sortedDatabaseList2Count = sortedDatabaseList2.Count;

                for (int j = jStartIndex; j < sortedDatabaseList2Count; j++)
                {
                    long[] jValue = sortedDatabaseList2[j];

                    if (jValue[0] > iValue[0])
                    {
                        jStartIndex++;
                        sortedDatabaseList2Next = sortedDatabaseList2;
                        j = sortedDatabaseList2.Count;
                        continue;
                    }

                    jStartIndex = 0;

                    var isEqual = true;

                    for (var k = 0; k < columnsInSubspaceCount; k++)
                    {
                        long iColumnValue = iValue[k];
                        long jColumnValue = jValue[k];

                        if (iColumnValue != jColumnValue)
                        {
                            isEqual = false;
                            break;
                        }

                        equalCombination[k] = iColumnValue;
                    }

                    if (!isEqual)
                    {
                        if (j > 0)
                        {
                            sortedDatabaseList2Next.Add(jValue);
                        }

                        continue;
                    }

                    long equalCombinationHash = GetArrayHashCode(equalCombination);

                    iValue[equalValuesBucketColumnIndex] = equalCombinationHash;
                    jValue[equalValuesBucketColumnIndex] = equalCombinationHash;
                }

                sortedDatabaseList2 = sortedDatabaseList2Next;

                if (sortedDatabaseList2.Count - jStartIndex <= 0)
                {
                    i = sortedDatabaseList.Count;
                }
            }

            foreach (long[] i in sortedDatabaseList.Where(i => i[equalValuesBucketColumnIndex] != 0))
            {
                long bucket = i[equalValuesBucketColumnIndex];
                if (!equalRowsWithRespectToSubspaceColumns.ContainsKey(bucket))
                {
                    equalRowsWithRespectToSubspaceColumns.Add(bucket, new HashSet<long>());
                }
                equalRowsWithRespectToSubspaceColumns[bucket].Add(i[rowIdentifierColumnIndex]);
            }

            return equalRowsWithRespectToSubspaceColumns.Values;
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
                long equalRow in
                    potentiallyDominatedObjects.Where(
                        equalRow => !dominatingObjectsDatabase.ContainsKey(equalRow)))
            {
                destinationDatabase.Remove(equalRow);
            }
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
        private static long GetArrayHashCode(long[] obj)
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