namespace prefSQL.SQLSkyline.SamplingSkyline
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
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

        private string Operators { get; set; }
        private ReadOnlyCollection<string> OperatorsCollection { get; set; }

        private SamplingSkylineUtility Utility
        {
            get { return _utility; }
        }

        public long NumberOfOperations { get; set; }
        private SkylineStrategy SelectedStrategy { get; set; }
        public int SubspacesCount { get; set; }
        public int SubspaceDimension { get; set; }

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
        public DataTable GetSkylineTable(string query, string operators, SkylineStrategy skylineStrategy)
        {
            SelectedStrategy = skylineStrategy;
            Operators = operators;
            OperatorsCollection = Array.AsReadOnly(Operators.Split(';'));

            DataTable fullDataTable = Helper.GetDataTableFromSQL(query, SelectedStrategy.ConnectionString,
                SelectedStrategy.Provider);

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

            Utility.SubspacesCount = SubspacesCount;
            Utility.SubspaceDimension = SubspaceDimension;
            Utility.AllPreferencesCount = OperatorsCollection.Count;
            Utility.RowIdentifierColumnIndex = rowIdentifierColumnIndex;
            Utility.EqualValuesBucketColumnIndex = equalValuesBucketColumnIndex;

            AddInternalArtificialUniqueRowIdentifier(fullDataTable, Utility.RowIdentifierColumnIndex);

            IDictionary<long, object[]> database = Helper.GetDictionaryFromDataTable(fullDataTable,
                Utility.RowIdentifierColumnIndex);

            var dataTableTemplate = new DataTable();
            SqlDataRecord dataRecordTemplate = Helper.BuildDataRecord(fullDataTable, OperatorsCollection.ToArray(),
                dataTableTemplate);

            DataTable skylineDataTableReturn = GetSkyline(database, dataTableTemplate, dataRecordTemplate);

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
            SqlDataRecord dataRecordTemplate)
        {
            DataTable skylineSampleReturn = dataTableTemplate.Clone();
            var skylineSampleFinalDatabase = new Dictionary<long, object[]>();

            int recordAmountLimit = SelectedStrategy.RecordAmountLimit;
            int sortType = SelectedStrategy.SortType;
            SelectedStrategy.RecordAmountLimit = 0;
            SelectedStrategy.SortType = 0;

            TimeMilliseconds = 0;
            var sw = new Stopwatch();
            sw.Start();

            foreach (HashSet<int> subspace in Utility.Subspaces)
            {
                string subpaceOperators = string.Join(";",
                    GetOperatorsWithIgnoredEntries(OperatorsCollection.ToArray(), subspace));

                sw.Stop();
                TimeMilliseconds += sw.ElapsedMilliseconds;
                DataTable subspaceDataTable = SelectedStrategy.GetSkylineTable(database.Values.ToList(),
                    dataTableTemplate, dataRecordTemplate, subpaceOperators);
                TimeMilliseconds += SelectedStrategy.TimeMilliseconds;
                sw.Restart();

                IDictionary<long, object[]> subspaceDatabase = GetDatabaseFromDataTable(database, subspaceDataTable);

                IEnumerable<long[]> databaseForPairwiseComparison = GetDatabaseForPairwiseComparison(subspaceDatabase.Values,subspace);

                IEnumerable<HashSet<long>> rowsWithEqualValuesWithRespectToSubspaceColumns =
                    CompareEachRowWithRespectToSubspaceColumnsPairwise(databaseForPairwiseComparison);

                foreach (HashSet<long> rowsWithEqualValues in rowsWithEqualValuesWithRespectToSubspaceColumns)
                {
                    subpaceOperators = string.Join(";",
                        GetOperatorsWithIgnoredEntries(OperatorsCollection.ToArray(), GetSubspaceComplement(subspace)));

                    IDictionary<long, object[]> rowsWithEqualValuesDatabase = GetSubsetOfDatabase(database,
                        rowsWithEqualValues);

                    sw.Stop();
                    TimeMilliseconds += sw.ElapsedMilliseconds;
                    DataTable subspaceComplementDataTable =
                        SelectedStrategy.GetSkylineTable(rowsWithEqualValuesDatabase.Values.ToList(),
                            dataTableTemplate, dataRecordTemplate, subpaceOperators);
                    TimeMilliseconds += SelectedStrategy.TimeMilliseconds;
                    sw.Restart();

                    IDictionary<long, object[]> subspaceComplementDatabase = GetDatabaseFromDataTable(database,
                        subspaceComplementDataTable);

                    RemoveDominatedObjects(rowsWithEqualValues,
                        subspaceComplementDatabase, subspaceDatabase);
                }

                MergeSubspaceSkylineIntoFinalSkylineSample(subspaceDatabase, skylineSampleFinalDatabase);
            }

            var skylineValues = new List<long[]>();

            foreach (KeyValuePair<long, object[]> skylineSampleRow in skylineSampleFinalDatabase)
            {
                DataRow row = skylineSampleReturn.NewRow();
                var skylineValueRow = new long[Utility.AllPreferencesCount];

                for (var i = 0; i < skylineSampleRow.Value.Length; i++)
                {
                    if (i < Utility.AllPreferencesCount)
                    {
                        skylineValueRow[i] = (long) skylineSampleRow.Value[i];
                    } else
                    {
                        row[i - Utility.AllPreferencesCount] = skylineSampleRow.Value[i];
                    }
                }

                skylineSampleReturn.Rows.Add(row);
                skylineValues.Add(skylineValueRow);
            }

            RemoveInternalArtificialUniqueRowIdentifier(skylineSampleReturn);

            //Remove certain amount of rows if query contains TOP Keyword
            Helper.GetAmountOfTuples(skylineSampleReturn, SelectedStrategy.RecordAmountLimit);

            //Sort ByRank
            if (SelectedStrategy.SortType == 1)
            {
                skylineSampleReturn = Helper.SortByRank(skylineSampleReturn, skylineValues);
            } else if (SelectedStrategy.SortType == 2)
            {
                skylineSampleReturn = Helper.SortBySum(skylineSampleReturn, skylineValues);
            }

            sw.Stop();
            TimeMilliseconds += sw.ElapsedMilliseconds;

            SelectedStrategy.RecordAmountLimit = recordAmountLimit;
            SelectedStrategy.SortType = sortType;

            return skylineSampleReturn;
        }

        private IEnumerable<long[]> GetDatabaseForPairwiseComparison(IEnumerable<object[]> subspaceDatabase,
            ICollection<int> subspace)
        {
            var subspaceForPairwiseComparison = new List<long[]>();
            foreach (object[] row in subspaceDatabase)
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
        ///     TODO: remarks about performance (this is the most time consuming method besides the actual skyline algorithm)
        /// </remarks>
        /// <param name="subspaceDatabase"></param>
        /// <param name="columnsUsedInSubspace"></param>
        /// <returns></returns>
        private IEnumerable<HashSet<long>> CompareEachRowWithRespectToSubspaceColumnsPairwise(
            IEnumerable<long[]> subspaceDatabase)
        {
            List<long[]> database = subspaceDatabase.ToList();

            int columnsInSubspaceCount = database[0].Length - 2;
            int rowIdentifierColumnIndex = database[0].Length - 2;
            int equalValuesBucketColumnIndex = database[0].Length - 1;

            // sort on first subspace attribute in order to skip some comparisons later
            database.Sort((row1, row2) => (row1[0]).CompareTo(row2[0]));

            // compare pairwise, initially skip first element to prevent comparison of elements with themselves
            var databaseForComparison = new List<long[]>(database);

            // store the actual values which are equal to another database row in order to create separate lists
            // for each combination of equal values
            var equalValues = new long[columnsInSubspaceCount];

            // performance; access counter only once
            int databaseCount = database.Count;

            for (var databaseIndex = 0; databaseIndex < databaseCount; databaseIndex++)
            {
                // possibly reduced list to use for next iteration over databaseForComparison, see iteration over
                // databaseForComparison
                var databaseForComparisonForNextIteration = new List<long[]>();

                // performance; dereference only once
                long[] databaseRowValue = database[databaseIndex];

                // performance; access counter only once
                int databaseForComparisonCount = databaseForComparison.Count;

                for (var databaseForComparisonIndex = 0;
                    databaseForComparisonIndex < databaseForComparisonCount;
                    databaseForComparisonIndex++)
                {
                    // performance; dereference only once
                    long[] databaseForComparisonRowValue = databaseForComparison[databaseForComparisonIndex];

                    if (databaseRowValue[rowIdentifierColumnIndex] ==
                        databaseForComparisonRowValue[rowIdentifierColumnIndex])
                    {
                        continue;
                    }

                    // performance; dereference only once
                    long sortedColumnDatabaseRowValue = databaseRowValue[0];

                    // the value at index 0 is the sorted value; hence, skip the remaining elements of
                    // databaseForComparison if the current element is larger than the element from database,
                    // since there cannot be equal values in databaseForComparison from now on (the list is sorted)
                    if (databaseForComparisonRowValue[0] > sortedColumnDatabaseRowValue)
                    {
                        // don't recalculate the whole databaseForComparison list, instead move the starting
                        // index one step forward for the next iteration
                        ////int startAddRange = databaseForComparisonIndex == 0
                        ////    ? databaseForComparisonIndex + 1
                        ////    : databaseForComparisonIndex;

                        ////int startAddRange = databaseForComparisonIndex;

                        ////if (startAddRange > databaseForComparisonCount)
                        ////{
                            ////databaseForComparisonForNextIteration.AddRange(databaseForComparison.GetRange(
                            ////    startAddRange, databaseForComparisonCount - startAddRange));
                        databaseForComparisonForNextIteration.AddRange(databaseForComparison.GetRange(
                            databaseForComparisonIndex, databaseForComparisonCount - databaseForComparisonIndex));
                        ////}

                        // terminate inner iteration over databaseForComparison
                        break;
                    }

                    // the attribute at index 0 of the current row of database is equal to databaseForComparison;
                    // it cannot be less, because the column at index 0 is sorted; hence, insert the value directly
                    equalValues[0] = sortedColumnDatabaseRowValue;

                    bool isDatabaseRowEqualToDatabaseForComparisonRow = DetermineEqualValues(columnsInSubspaceCount,
                        databaseRowValue, databaseForComparisonRowValue, equalValues);

                    if (!isDatabaseRowEqualToDatabaseForComparisonRow)
                    {
                        ////if (databaseForComparisonIndex > 0)
                        ////{
                            // at least one value of the subspace differs, so the current row of databaseForComparison
                            // will have to be compared in forthcoming iteartions with another row of database;
                            // hence, add this row to databaseForComparisonForNextIteration, which will be the
                            // list being iterated over in the next iteration
                            databaseForComparisonForNextIteration.Add(databaseForComparisonRowValue);
                        ////}

                        // terminate inner iteration over databaseForComparison, since the values were not equal
                        continue;
                    }

                    long equalValuesBucket = GetArrayHashCode(equalValues);

                    // these rows have equal values, mark the rows with the corresponding hash which can be
                    // viewed as a bucket for all rows with the same hash, i.e. for all rows with the same
                    // values in their subspace
                    databaseRowValue[equalValuesBucketColumnIndex] = equalValuesBucket;
                    databaseForComparisonRowValue[equalValuesBucketColumnIndex] = equalValuesBucket;
                }

                databaseForComparison = databaseForComparisonForNextIteration;

                if (databaseForComparison.Count == 0)
                {
                    // terminate outer iteration over database
                    break;
                }
            }

            // return list of row identifiers for each row in each bucket
            return GetEqualRowsWithRespectToSubspaceColumns(database, rowIdentifierColumnIndex,
                equalValuesBucketColumnIndex);
        }

        private static bool DetermineEqualValues(int columnsInSubspaceCount, long[] databaseRowValue,
            long[] databaseForComparisonRowValue, long[] equalValues)
        {
            var isDatabaseRowEqualToDatabaseForComparisonRow = true;

            // start from index 1, since the sorted column 0 is already set in equalValues[0]
            for (var subspaceColumnIndex = 1;
                subspaceColumnIndex < columnsInSubspaceCount;
                subspaceColumnIndex++)
            {
                // performance; dereference only once
                long databaseRowColumnValue = databaseRowValue[subspaceColumnIndex];
                // performance; dereference only once
                long databaseForComparisonRowColumnValue = databaseForComparisonRowValue[subspaceColumnIndex];

                if (databaseRowColumnValue != databaseForComparisonRowColumnValue)
                {
                    // one value differs, break this loop
                    isDatabaseRowEqualToDatabaseForComparisonRow = false;
                    break;
                }

                equalValues[subspaceColumnIndex] = databaseRowColumnValue;
            }

            return isDatabaseRowEqualToDatabaseForComparisonRow;
        }

        private static IEnumerable<HashSet<long>> GetEqualRowsWithRespectToSubspaceColumns(IEnumerable<long[]> database,
            int rowIdentifierColumnIndex, int equalValuesBucketColumnIndex)
        {
            var equalRowsWithRespectToSubspaceColumns = new Dictionary<long, HashSet<long>>();

            foreach (long[] i in database.Where(i => i[equalValuesBucketColumnIndex] != 0))
            {
                long equalValuesBucket = i[equalValuesBucketColumnIndex];
                if (!equalRowsWithRespectToSubspaceColumns.ContainsKey(equalValuesBucket))
                {
                    equalRowsWithRespectToSubspaceColumns.Add(equalValuesBucket, new HashSet<long>());
                }
                equalRowsWithRespectToSubspaceColumns[equalValuesBucket].Add(i[rowIdentifierColumnIndex]);
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
        /// <param name="array">Array to get hash code for.</param>
        /// <returns>Hash code.</returns>
        private static long GetArrayHashCode(long[] array)
        {
            return array.Aggregate("", (current, i) => current + i + "-").GetHashCode();

            //unchecked // Overflow is not a problem, just wrap
            //{
            //    var result = (int) 2166136261;
            //    foreach (long l in array)
            //    {
            //        result = result * 16777619 ^ l.GetHashCode();
            //    }
            //    return result;
            //}
        }
    }
}