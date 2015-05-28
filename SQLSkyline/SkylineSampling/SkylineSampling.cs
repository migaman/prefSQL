namespace prefSQL.SQLSkyline.SkylineSampling
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using Microsoft.SqlServer.Server;

    /// <summary>
    ///     Implementation of the skyline sampling algorithm according to the algorithm pseudocode in W.-T. Balke, J. X. Zheng,
    ///     and U. Güntzer (2005).
    /// </summary>
    /// <remarks>
    ///     This implementation re-uses the already implemented algorithms of the prefSQL framework to calculate the necessary
    ///     subspace skylines. Publication:
    ///     W.-T. Balke, J. X. Zheng, and U. Güntzer, “Approaching the Efficient Frontier: Cooperative Database Retrieval Using
    ///     High-Dimensional Skylines,” in Lecture Notes in Computer Science, Database Systems for Advanced Applications, D.
    ///     Hutchison, T. Kanade, J. Kittler, J. M. Kleinberg, F. Mattern, J. C. Mitchell, M. Naor, O. Nierstrasz, C. Pandu
    ///     Rangan, B. Steffen, M. Sudan, D. Terzopoulos, D. Tygar, M. Y. Vardi, G. Weikum, L. Zhou, B. C. Ooi, and X. Meng,
    ///     Eds, Berlin, Heidelberg: Springer Berlin Heidelberg, 2005, pp. 410–421.
    /// </remarks>
    public sealed class SkylineSampling
    {
        /// <summary>
        ///     Name of additional column which holds a unique row identifier maintained by the skyline sampling algorithm.
        /// </summary>
        private const string InternalArtificialUniqueRowIdentifierColumnName = "_internalArtificialUniqueRowIdentifier";

        /// <summary>
        ///     Name of additional column which holds
        /// </summary>
        private const string InternalEqualValuesBucketColumnName = "_internalEqualValuesBucket";

        /// <summary>
        ///     Declared as backing variable in order to provide "readonly" semantics.
        /// </summary>
        private readonly SkylineSamplingUtility _utility;

        /// <summary>
        ///     The time spent to perform this whole algorithm.
        /// </summary>
        /// <remarks>
        ///     Includes the calculation of the various subpace skylines via the already existing skyline algorithms. Excludes the
        ///     time spent to query SQL server and retrieve database.
        /// </remarks>
        public long TimeMilliseconds { get; set; }

        /// <summary>
        ///     All Operators over the preferences (e.g., "LOW", "INCOMPARABLE").
        /// </summary>
        private string[] Operators { get; set; }

        /// <summary>
        ///     All strings for the Operators (e.g., "LOW", "LOW;INCOMPARABLE").
        /// </summary>
        /// <remarks>
        ///     Since INCOMPARABLE preferences are represented by two columns resp. operators, OperatorStrings is used to represent
        ///     this situation. There are two possibilities: Either an element of OperatorStrings contains "LOW", or it contains
        ///     "LOW;INCOMPARABLE". This is also used for convenience when executing the skyline algorithm over a subspace of all
        ///     preferences - the operators for this subspace can be simply concatenated from the OperatorStrings property.
        /// </remarks>
        private string[] OperatorStrings { get; set; }

        /// <summary>
        ///     The positions of the preferences (i.e., the columns) within the skyline reported from the skyline algorithm.
        /// </summary>
        /// <remarks>
        ///     This is used to skip over INCOMPARABLE columns when they're not used (e.g., when collecting the skylineValues for
        ///     sorting methods).
        /// </remarks>
        private int[] PreferenceColumnIndex { get; set; }

        /// <summary>
        ///     Utility providing functionality to assist the algorithm.
        /// </summary>
        private SkylineSamplingUtility Utility
        {
            get { return _utility; }
        }

        /// <summary>
        ///     The sum of all NumberOfOperations of the skyline algorithm (specified by SelectedStrategy) executed for each
        ///     subspace skyline.
        /// </summary>
        public long NumberOfOperations { get; set; }

        /// <summary>
        ///     The strategs selected, i.e., the base skyline algorithm that should be executed when calculating the various
        ///     subspace skylines.
        /// </summary>
        private SkylineStrategy SelectedStrategy { get; set; }

        /// <summary>
        ///     How many subspace skylines should be calculated in order to retrieve a skyline sample.
        /// </summary>
        /// <remarks>
        ///     Usually specified by an SQL COUNT-clause ("... SKYLINE OF ... SAMPLE BY RANDOM_SUBSETS COUNT ... DIMENSION ...").
        /// </remarks>
        public int SubspacesCount { get; set; }

        /// <summary>
        ///     The number of preferences for each calculated subspace skyline.
        /// </summary>
        /// <remarks>
        ///     Usually specified by an SQL DIMENSION-clause ("...
        ///     SKYLINE OF ... SAMPLE BY RANDOM_SUBSETS COUNT ... DIMENSION ...").
        /// </remarks>
        public int SubspaceDimension { get; set; }

        /// <summary>
        ///     Instantiates an object with a new SkylineSamplingUtility as its Utility.
        /// </summary>
        public SkylineSampling() : this(new SkylineSamplingUtility())
        {
        }

        /// <summary>
        ///     Instantiates an object with the specified SkylineSamplingUtility as its Utility.
        /// </summary>
        /// <param name="utility">Used as Utility for the skyline sampling algorithm.</param>
        internal SkylineSampling(SkylineSamplingUtility utility)
        {
            _utility = utility;
        }

        /// <summary>
        ///     Entry point for calculating a skyline sample via the skyline sampling^algorithm.
        /// </summary>
        /// <param name="query">
        ///     An ANSI SQL statement to query the SQL server in order to fetch the whole database including its SkylineAttribute
        ///     colums (which precede the real database columns as by convention of the prefSQL framework).
        /// </param>
        /// <param name="operators">
        ///     The operators with which the preferences are handled; can be either "LOW" or "INCOMPARABLE",
        ///     specified in the format "LOW;LOW;INCOMPARABLE;LOW;LOW;...", i.e., separated via ";". The position of the keyword
        ///     has to correspond to the position of its preference.
        /// </param>
        /// <param name="skylineStrategy">The skyline algorithm used to calculate the various subspace skylines.</param>
        /// <returns>The skyline sample.</returns>
        public DataTable GetSkylineTable(string query, string operators, SkylineStrategy skylineStrategy)
        {
            SelectedStrategy = skylineStrategy;

            CalculatePropertiesWithRespectToIncomparableOperators(operators);

            DataTable fullDataTable = Helper.GetDataTableFromSQL(query, SelectedStrategy.ConnectionString,
                SelectedStrategy.Provider);
            
            AddGeneratedInternalColumns(fullDataTable);

            FillUtilityProperties(fullDataTable);

            IReadOnlyDictionary<long, object[]> database = Helper.GetDatabaseAccessibleByUniqueId(fullDataTable,
                Utility.ArtificialUniqueRowIdentifierColumnIndex, true);

            var dataTableTemplate = new DataTable();
            SqlDataRecord dataRecordTemplate = Helper.BuildDataRecord(fullDataTable, Operators.ToArray(),
                dataTableTemplate);

            return GetSkyline(database, dataTableTemplate, dataRecordTemplate);
        }

        /// <summary>
        ///     Fill Operators, OperatorStrings and PreferenceColumnIndex properties.
        /// </summary>
        /// <param name="operators">
        ///     The operators with which the preferences are handled; can be either "LOW" or "INCOMPARABLE",
        ///     specified in the format "LOW;LOW;INCOMPARABLE;LOW;LOW;...", i.e., separated via ";".
        /// </param>
        private void CalculatePropertiesWithRespectToIncomparableOperators(string operators)
        {
            Operators = operators.Split(';');
            OperatorStrings = new string[Operators.Count(op => op != "INCOMPARABLE")];
            PreferenceColumnIndex = new int[Operators.Count(op => op != "INCOMPARABLE")];
            var nextOperatorIndex = 0;
            for (var opIndex = 0; opIndex < Operators.Length; opIndex++)
            {
                if (Operators[opIndex] != "INCOMPARABLE")
                {
                    OperatorStrings[nextOperatorIndex] = Operators[opIndex];
                    PreferenceColumnIndex[nextOperatorIndex] = opIndex;
                    nextOperatorIndex++;
                }
                else
                {
                    // keep "LOW;INCOMPARABLE" together
                    OperatorStrings[nextOperatorIndex - 1] += ";" + Operators[opIndex];
                }
            }
        }

        /// <summary>
        ///     Add InternalArtificialUniqueRowIdentifierColumnName and InternalEqualValuesBucketColumnName, respectively. First,
        ///     InternalArtificialUniqueRowIdentifierColumnName is added, then InternalEqualValuesBucketColumnName; i.e.,
        ///     InternalEqualValuesBucketColumnName is the new last column of the DataTable.
        /// </summary>
        /// <param name="dataTable">The DataTable on which to add the two columns.</param>
        private static void AddGeneratedInternalColumns(DataTable dataTable)
        {
            var internalArtificialUniqueRowIdentifierColumn = new DataColumn()
            {
                ColumnName = InternalArtificialUniqueRowIdentifierColumnName,
                DataType = typeof (long),
                DefaultValue = 0
            };
            var internalEqualValuesBucketColumn = new DataColumn()
            {
                ColumnName = InternalEqualValuesBucketColumnName,
                DataType = typeof (long),
                DefaultValue = 0
            };
            dataTable.Columns.Add(internalArtificialUniqueRowIdentifierColumn);
            dataTable.Columns.Add(internalEqualValuesBucketColumn);
        }

        /// <summary>
        ///     Sets necessary properties of Utility.
        /// </summary>
        /// <param name="dataTable">
        ///     A DataTable to which the columns InternalArtificialUniqueRowIdentifierColumnName and
        ///     InternalEqualValuesBucketColumnName have already been added.
        /// </param>
        private void FillUtilityProperties(DataTable dataTable)
        {
            Utility.SubspacesCount = SubspacesCount;
            Utility.SubspaceDimension = SubspaceDimension;
            Utility.AllPreferencesCount = Operators.Count(op => op != "INCOMPARABLE");
            Utility.ArtificialUniqueRowIdentifierColumnIndex = dataTable.Columns.Count - 2;
            Utility.EqualValuesBucketColumnIndex = dataTable.Columns.Count - 1;
        }

        /// <summary>
        ///     Entry point for calculating a skyline sample via the skyline sampling algorithm after preparatory work in
        ///     <see cref="GetSkylineTable" /> has been carried out.
        /// </summary>
        /// <param name="database">
        ///     A Collection by which a row can be accessed via its unique ID. The values represent the database
        ///     rows including the preceding SkylineAttribute columns.
        /// </param>
        /// <param name="dataTableTemplate">
        ///     An empty DataTable with all columns to return to which the columns InternalArtificialUniqueRowIdentifierColumnName
        ///     and InternalEqualValuesBucketColumnName have already been added.
        /// </param>
        /// <param name="dataRecordTemplate">A template used to report rows for the skyline algorithm if working with the CLR.</param>
        /// <returns>The skyline sample.</returns>
        internal DataTable GetSkyline(IReadOnlyDictionary<long, object[]> database, DataTable dataTableTemplate,
            SqlDataRecord dataRecordTemplate)
        {
            var sw = new Stopwatch();
            sw.Start();

            // preserve current skyline algorithm's settings
            int recordAmountLimit = SelectedStrategy.RecordAmountLimit;
            int sortType = SelectedStrategy.SortType;
            bool hasIncomparablePreferences = SelectedStrategy.HasIncomparablePreferences;

            // the calculated subspace skylines need all rows, sorting is unnecessary
            SelectedStrategy.RecordAmountLimit = 0;
            SelectedStrategy.SortType = 0;

            TimeMilliseconds = 0;
            NumberOfOperations = 0;

            DataTable skylineSampleReturn = dataTableTemplate.Clone();
            var skylineValues = new List<long[]>();

            IReadOnlyDictionary<long, object[]> skylineSampleFinalDatabase = CalculateSkylineSampleFinalDatabase(database,
                dataTableTemplate, dataRecordTemplate, sw);
            CalculateSkylineSample(skylineSampleFinalDatabase, skylineSampleReturn, skylineValues);

            RemoveGeneratedInternalColumns(skylineSampleReturn);

            SortDataTable(skylineSampleReturn, skylineValues);

            //Remove certain amount of rows if query contains TOP Keyword
            Helper.GetAmountOfTuples(skylineSampleReturn, recordAmountLimit);

            // restore current skyline algorithm's settings
            SelectedStrategy.RecordAmountLimit = recordAmountLimit;
            SelectedStrategy.SortType = sortType;
            SelectedStrategy.HasIncomparablePreferences = hasIncomparablePreferences;

            sw.Stop();
            TimeMilliseconds += sw.ElapsedMilliseconds;

            return skylineSampleReturn;
        }

        /// <summary>
        ///     Calculate the necessary subspace skylines and merge them into the skyline sample which will finally be reported by
        ///     the skyline sampling algorithm.
        /// </summary>
        /// <remarks>
        ///     Calculate subspaces. For each subspace, calculate a skyline via the selected skyline algorithm. Determine the
        ///     objects for which the calculation of a subspace skyline with respect to the subspace's complement is necessary; if
        ///     so, calculate this subspace complement skyline via the selected skyline algorithm and remove dominated objects from
        ///     the subspace skyline. Finally, merge the subspace skyline into the skyline sample which will be reported by the
        ///     skyline sampling algorithm.
        /// </remarks>
        /// <param name="database">
        ///     A Collection by which a row can be accessed via its unique ID. The values represent the database
        ///     rows including the preceding SkylineAttribute columns.
        /// </param>
        /// <param name="dataTableTemplate">
        ///     An empty DataTable with all columns to return to which the columns
        ///     InternalArtificialUniqueRowIdentifierColumnName and InternalEqualValuesBucketColumnName have already been added.
        /// </param>
        /// <param name="dataRecordTemplate">A template used to report rows for the skyline algorithm if working with the CLR.</param>
        /// <param name="sw">
        ///     To measture the time spent to perform this whole algorithm. Has to be started before calling this
        ///     method, will be running after this method.
        /// </param>
        private IReadOnlyDictionary<long, object[]> CalculateSkylineSampleFinalDatabase(IReadOnlyDictionary<long, object[]> database,
            DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, Stopwatch sw)
        {
            var skylineSampleFinalDatabase = new Dictionary<long, object[]>();

            foreach (HashSet<int> subspace in Utility.Subspaces)
            {
                string subpaceOperators = GetOperatorsWithIgnoredEntriesString(subspace);

                SelectedStrategy.HasIncomparablePreferences = subpaceOperators.Contains("INCOMPARABLE");

                sw.Stop();
                TimeMilliseconds += sw.ElapsedMilliseconds;
                DataTable subspaceDataTable = SelectedStrategy.GetSkylineTable(database.Values,
                    dataTableTemplate.Clone(), dataRecordTemplate, subpaceOperators);
                TimeMilliseconds += SelectedStrategy.TimeMilliseconds;
                NumberOfOperations += SelectedStrategy.NumberOfOperations;
                sw.Restart();

                IDictionary<long, object[]> subspaceDatabase = GetDatabaseFromDataTable(database, subspaceDataTable);

                IEnumerable<Tuple<long[], string[]>> databaseForPairwiseComparison =
                    GetDatabaseForPairwiseComparison(subspaceDatabase.Values, subspace.ToList());
                // TODO: comment: needs guaranteed stable order
                
                IEnumerable<HashSet<long>> rowsWithEqualValuesWithRespectToSubspaceColumns =
                    CompareEachRowWithRespectToSubspaceColumnsPairwise(databaseForPairwiseComparison);

                string subpaceComplementOperators =
                    GetOperatorsWithIgnoredEntriesString(Utility.GetSubspaceComplement(subspace));
                SelectedStrategy.HasIncomparablePreferences = subpaceComplementOperators.Contains("INCOMPARABLE");

                foreach (HashSet<long> rowsWithEqualValues in rowsWithEqualValuesWithRespectToSubspaceColumns)
                {
                    IReadOnlyDictionary<long, object[]> rowsWithEqualValuesDatabase = GetSubsetOfDatabase(database,
                        rowsWithEqualValues);

                    sw.Stop();
                    TimeMilliseconds += sw.ElapsedMilliseconds;
                    DataTable subspaceComplementDataTable =
                        SelectedStrategy.GetSkylineTable(rowsWithEqualValuesDatabase.Values,
                            dataTableTemplate.Clone(), dataRecordTemplate, subpaceComplementOperators);
                    TimeMilliseconds += SelectedStrategy.TimeMilliseconds;
                    NumberOfOperations += SelectedStrategy.NumberOfOperations;
                    sw.Restart();

                    IReadOnlyDictionary<long, object[]> subspaceComplementDatabase = new ReadOnlyDictionary<long, object[]>(GetDatabaseFromDataTable(database,subspaceComplementDataTable));

                    RemoveDominatedObjects(rowsWithEqualValues,subspaceComplementDatabase, subspaceDatabase);
                }

                MergeSubspaceSkylineIntoFinalSkylineSample(new ReadOnlyDictionary<long, object[]>(subspaceDatabase), skylineSampleFinalDatabase);
            }

            return skylineSampleFinalDatabase;
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        /// <param name="subspace"></param>
        /// <returns></returns>
        private string GetOperatorsWithIgnoredEntriesString(ICollection<int> subspace)
        {
            var operatorsReturn = "";

            for (var i = 0; i < OperatorStrings.Length; i++)
            {
                if (!subspace.Contains(i))
                {
                    if (OperatorStrings[i].Contains(';'))
                    {
                        operatorsReturn += "IGNORE;IGNORE;";
                    }
                    else
                    {
                        operatorsReturn += "IGNORE;";
                    }
                }
                else
                {
                    operatorsReturn += OperatorStrings[i] + ";";
                }
            }

            return operatorsReturn.TrimEnd(';');
        }

        /// <summary>
        ///     Get the database collection of the given dataTable including the preceding SkylineAttribute columns.
        /// </summary>
        /// <param name="database">
        ///     A Collection by which a row can be accessed via its unique ID. The values represent the database
        ///     rows including the preceding SkylineAttribute columns.
        /// </param>
        /// <param name="dataTable">A dataTable containing the InternalArtificialUniqueRowIdentifierColumnName column.</param>
        /// <returns>
        ///     The objects that correspond to the given dataTable, i.e., the rows including the preceding SkylineAttribute
        ///     columns.
        /// </returns>
        private IDictionary<long, object[]> GetDatabaseFromDataTable(IReadOnlyDictionary<long, object[]> database,
            DataTable dataTable)
        {
            int rowIdentifierColumnIndex = Utility.ArtificialUniqueRowIdentifierColumnIndex -
                                           Utility.AllPreferencesCount - Operators.Count(op => op == "INCOMPARABLE");

            return dataTable.Rows.Cast<DataRow>()
                .ToDictionary(row => (long) row[rowIdentifierColumnIndex],
                    row => database[(long) row[rowIdentifierColumnIndex]]);
        }

        private IEnumerable<Tuple<long[], string[]>> GetDatabaseForPairwiseComparison(
            IEnumerable<object[]> subspaceDatabase,
            IReadOnlyCollection<int> subspace)
        {
            var subspaceForPairwiseComparison = new List<Tuple<long[], string[]>>();

            foreach (object[] row in subspaceDatabase)
            {
                var rowForPairwiseComparison = new long[subspace.Count + 2];
                var rowForPairwiseComparisonIncomparable = new string[subspace.Count];

                var count = 0;
                foreach (int sub in subspace)
                {
                    int rowIndex = PreferenceColumnIndex[sub];
                    rowForPairwiseComparison[count] = (long) row[rowIndex];

                    if (OperatorStrings[sub].Contains(';')) // i.e., INCOMPARABLE specified in preference
                    {
                        rowForPairwiseComparisonIncomparable[count] = (string) row[rowIndex + 1];
                    }

                    count++;
                }

                rowForPairwiseComparison[count] = (long) row[Utility.ArtificialUniqueRowIdentifierColumnIndex];
                rowForPairwiseComparison[count + 1] = 0;

                subspaceForPairwiseComparison.Add(new Tuple<long[], string[]>(rowForPairwiseComparison,
                    rowForPairwiseComparisonIncomparable));
            }

            return subspaceForPairwiseComparison;
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        /// <remarks>
        ///     TODO: remarks about performance (this is the most time consuming method besides the actual skyline algorithm)
        /// </remarks>
        /// <param name="subspaceDatabase"></param>
        /// <returns></returns>
        private IEnumerable<HashSet<long>> CompareEachRowWithRespectToSubspaceColumnsPairwise(
            IEnumerable<Tuple<long[], string[]>> subspaceDatabase)
        {
            List<Tuple<long[], string[]>> database = subspaceDatabase.ToList();
            var usedBuckets = new Dictionary<string, long> {{"", 0}};

            if (database.Count == 0)
            {
                return new List<HashSet<long>>();
            }

            int columnsInSubspaceCount = database[0].Item1.Length - 2;
            int rowIdentifierColumnIndex = database[0].Item1.Length - 2;
            int equalValuesBucketColumnIndex = database[0].Item1.Length - 1;

            // sort on first subspace attribute in order to skip some comparisons later
            database.Sort((row1, row2) => (row1.Item1[0]).CompareTo(row2.Item1[0]));

            // compare pairwise, initially skip first element to prevent comparison of elements with themselves
            var databaseForComparison = new List<Tuple<long[], string[]>>(database);

            // store the actual values which are equal to another database row in order to create separate lists
            // for each combination of equal values
            var equalValues = new string[columnsInSubspaceCount];

            // performance; access counter only once
            int databaseCount = database.Count;

            for (var databaseIndex = 0; databaseIndex < databaseCount; databaseIndex++)
            {
                // possibly reduced list to use for next iteration over databaseForComparison, see iteration over
                // databaseForComparison
                var databaseForComparisonForNextIteration = new List<Tuple<long[], string[]>>();

                // performance; dereference only once
                Tuple<long[], string[]> databaseRowValueFull = database[databaseIndex];
                long[] databaseRowValue = databaseRowValueFull.Item1;

                // performance; access counter only once
                int databaseForComparisonCount = databaseForComparison.Count;

                for (var databaseForComparisonIndex = 0;
                    databaseForComparisonIndex < databaseForComparisonCount;
                    databaseForComparisonIndex++)
                {
                    // performance; dereference only once
                    Tuple<long[], string[]> databaseForComparisonRowValueFull =
                        databaseForComparison[databaseForComparisonIndex];
                    long[] databaseForComparisonRowValue = databaseForComparisonRowValueFull.Item1;

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
                        databaseForComparisonForNextIteration.AddRange(databaseForComparison.GetRange(
                            databaseForComparisonIndex, databaseForComparisonCount - databaseForComparisonIndex));

                        // terminate inner iteration over databaseForComparison
                        break;
                    }

                    // the attribute at index 0 of the current row of database is equal to databaseForComparison;
                    // it cannot be less, because the column at index 0 is sorted; hence, insert the value directly
                    equalValues[0] = sortedColumnDatabaseRowValue.ToString();

                    bool isDatabaseRowEqualToDatabaseForComparisonRow = DetermineEqualValues(columnsInSubspaceCount,
                        databaseRowValueFull, databaseForComparisonRowValueFull, equalValues);

                    if (!isDatabaseRowEqualToDatabaseForComparisonRow)
                    {
                        // at least one value of the subspace differs, so the current row of databaseForComparison
                        // will have to be compared in forthcoming iteartions with another row of database;
                        // hence, add this row to databaseForComparisonForNextIteration, which will be the
                        // list being iterated over in the next iteration
                        databaseForComparisonForNextIteration.Add(databaseForComparisonRowValueFull);

                        // terminate inner iteration over databaseForComparison, since the values were not equal
                        continue;
                    }

                    string bucketKey = equalValues.Aggregate("", (accu, current) => accu + current + "-");

                    if (!usedBuckets.ContainsKey(bucketKey))
                    {
                        usedBuckets.Add(bucketKey, usedBuckets.Values.Max() + 1);
                    }

                    long equalValuesBucket = usedBuckets[bucketKey];

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
            return GetEqualRowsWithRespectToSubspaceColumns(database.Select(item => item.Item1),
                rowIdentifierColumnIndex,
                equalValuesBucketColumnIndex);
        }

        private bool DetermineEqualValues(int columnsInSubspaceCount, Tuple<long[], string[]> databaseRowValue,
            Tuple<long[], string[]> databaseForComparisonRowValue, string[] equalValues)
        {
            var isDatabaseRowEqualToDatabaseForComparisonRow = true;

            // start from index 1, since the sorted column 0 is already set in equalValues[0]
            for (var subspaceColumnIndex = 1;
                subspaceColumnIndex < columnsInSubspaceCount;
                subspaceColumnIndex++)
            {
                // performance; dereference only once
                long databaseRowColumnValue = databaseRowValue.Item1[subspaceColumnIndex];
                // performance; dereference only once
                long databaseForComparisonRowColumnValue = databaseForComparisonRowValue.Item1[subspaceColumnIndex];

                if (databaseRowColumnValue != databaseForComparisonRowColumnValue)
                {
                    // one value differs, break this loop
                    isDatabaseRowEqualToDatabaseForComparisonRow = false;
                    break;
                }

                string eqValue = databaseRowColumnValue.ToString();

                if (OperatorStrings[subspaceColumnIndex].Contains(';'))
                {
                    // performance; dereference only once
                    string databaseRowColumnValueIncomparable = databaseRowValue.Item2[subspaceColumnIndex];
                    // performance; dereference only once
                    string databaseForComparisonRowColumnValueIncomparable =
                        databaseForComparisonRowValue.Item2[subspaceColumnIndex];

                    if (databaseRowColumnValueIncomparable != databaseForComparisonRowColumnValueIncomparable)
                    {
                        // one value differs, break this loop
                        isDatabaseRowEqualToDatabaseForComparisonRow = false;
                        break;
                    }

                    eqValue += "-" + databaseRowColumnValueIncomparable;
                }

                equalValues[subspaceColumnIndex] = eqValue;
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

        private static IReadOnlyDictionary<long, object[]> GetSubsetOfDatabase(
            IReadOnlyDictionary<long, object[]> database,
            IEnumerable<long> databaseSubsetKeys)
        {
            return new ReadOnlyDictionary<long, object[]>(databaseSubsetKeys.ToDictionary(row => row, row => database[row]));
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        /// <param name="potentiallyDominatedObjects"></param>
        /// <param name="dominatingObjectsDatabase"></param>
        /// <param name="destinationDatabase"></param>
        private static void RemoveDominatedObjects(
            IEnumerable<long> potentiallyDominatedObjects,
            IReadOnlyDictionary<long, object[]> dominatingObjectsDatabase, IDictionary<long, object[]> destinationDatabase)
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
        ///     TODO: comment
        /// </summary>
        /// <param name="sourceDatabase"></param>
        /// <param name="destinationDatabase"></param>
        private static void MergeSubspaceSkylineIntoFinalSkylineSample(
            IReadOnlyDictionary<long, object[]> sourceDatabase,
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

        private void CalculateSkylineSample(IReadOnlyDictionary<long, object[]> skylineSampleFinalDatabase,
            DataTable skylineSampleReturn, ICollection<long[]> skylineValues)
        {
            foreach (KeyValuePair<long, object[]> skylineSampleRow in skylineSampleFinalDatabase)
            {
                DataRow row = skylineSampleReturn.NewRow();
                var skylineValueRow = new long[Utility.AllPreferencesCount];

                var count = 0;
                for (var i = 0; i < skylineSampleRow.Value.Length; i++)
                {
                    if (i < Operators.Length)
                    {
                        if (Operators[i] != "INCOMPARABLE")
                        {
                            int rowIndex = PreferenceColumnIndex[count];
                            skylineValueRow[count] = (long) skylineSampleRow.Value[rowIndex];
                            count++;
                        }
                    }
                    else
                    {
                        row[i - Operators.Length] = skylineSampleRow.Value[i];
                    }
                }

                skylineSampleReturn.Rows.Add(row);
                skylineValues.Add(skylineValueRow);
            }
        }

        private static void RemoveGeneratedInternalColumns(DataTable dataTable)
        {
            dataTable.Columns.RemoveAt(dataTable.Columns.Count - 1); // InternalEqualValuesBucketColumnName
            dataTable.Columns.RemoveAt(dataTable.Columns.Count - 1); // InternalArtificialUniqueRowIdentifierColumnName
        }

        private void SortDataTable(DataTable dataTable, List<long[]> skylineValues)
        {
            switch (SelectedStrategy.SortType)
            {
                case 1:
                    Helper.SortByRank(dataTable, skylineValues);
                    break;
                case 2:
                    Helper.SortBySum(dataTable, skylineValues);
                    break;
                default:
                    break;
            }
        }
    }
}