namespace prefSQL.SQLSkyline.SkylineSampling
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using System.Text.RegularExpressions;
    using Microsoft.SqlServer.Server;

    /// <summary>
    ///     Implementation of the skyline sampling algorithm according to the algorithm pseudocode in W.-T. Balke, J. X. Zheng,
    ///     and U. Güntzer (2005).
    /// </summary>
    /// <remarks>
    ///     This implementation re-uses the already implemented algorithms of the prefSQL framework to calculate the necessary
    ///     subset skylines.
    ///     Publication:
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
        ///     Declared as backing variable in order to provide "readonly" semantics.
        /// </summary>
        private readonly SkylineSamplingUtility _utility;

        /// <summary>
        ///     The time spent to perform this whole algorithm.
        /// </summary>
        /// <remarks>
        ///     Includes the calculation of the various subpace skylines via the already existing skyline algorithms. Excludes the
        ///     time spent to query SQL server and initially retrieve database.
        /// </remarks>
        public long TimeMilliseconds { get; set; }

        /// <summary>
        ///     Utility providing functionality to assist the algorithm.
        /// </summary>
        private SkylineSamplingUtility Utility
        {
            get { return _utility; }
        }

        /// <summary>
        ///     The sum of all NumberOfOperations of the skyline algorithm (specified by SelectedStrategy) executed for each
        ///     subset skyline.
        /// </summary>
        public long NumberOfOperations { get; set; }

        /// <summary>
        ///     The strategy selected, i.e., the base skyline algorithm that should be executed when calculating the various
        ///     subset skylines.
        /// </summary>
        public SkylineStrategy SelectedStrategy { get; set; }

        /// <summary>
        ///     How many subset skylines should be calculated in order to retrieve a skyline sample.
        /// </summary>
        /// <remarks>
        ///     Usually specified by an SQL COUNT-clause ("... SKYLINE OF ... SAMPLE BY RANDOM_SUBSETS COUNT ... DIMENSION ...").
        /// </remarks>
        public int SubsetCount { get; set; }

        /// <summary>
        ///     The number of preferences for each calculated subset skyline.
        /// </summary>
        /// <remarks>
        ///     Usually specified by an SQL DIMENSION-clause ("...
        ///     SKYLINE OF ... SAMPLE BY RANDOM_SUBSETS COUNT ... DIMENSION ...").
        /// </remarks>
        public int SubsetDimension { get; set; }

        /// <summary>
        ///     A template used to report rows for the skyline algorithm.
        /// </summary>
        private SqlDataRecord DataRecordTemplate { get; set; }

        /// <summary>
        ///     A template used to report rows for the skyline algorithm if working with the CLR.
        /// </summary>
        public SqlDataRecord DataRecordTemplateForStoredProcedure { get; set; }

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
        ///     Determines the complete call to the stored procedure if executing the algorithm via CLR.
        /// </summary>
        /// <param name="strWhere">passed to the SelectedStrategy's parameter.</param>
        /// <param name="strOrderBy">passed to the SelectedStrategy's parameter.</param>
        /// <param name="strFirstSQL">passed to the SelectedStrategy's parameter.</param>
        /// <param name="strOperators">passed to the SelectedStrategy's parameter.</param>
        /// <param name="strOrderByAttributes">passed to the SelectedStrategy's parameter.</param>
        /// <returns>A string to be used to execute the stored procedure.</returns>
        public string GetStoredProcedureCommand(string strWhere, string strOrderBy, string strFirstSQL,
            string strOperators, string strOrderByAttributes)
        {
            string storedProcedureCommand = SelectedStrategy.GetStoredProcedureCommand(strWhere, strOrderBy, strFirstSQL,
                strOperators, strOrderByAttributes);

            storedProcedureCommand = Regex.Replace(storedProcedureCommand, @"dbo\.prefSQL_[^ ]* ", "dbo.prefSQL_SkylineSampling ");

            if (SelectedStrategy.GetType() != typeof (SkylineSQL))
            {
                storedProcedureCommand += ", " + SubsetCount + ", " + SubsetDimension + ", " +
                                          SelectedStrategy.GetType().Name + ", " +
                                          (SelectedStrategy.HasIncomparablePreferences ? "1" : "0");
            }

            return storedProcedureCommand;
        }

        /// <summary>
        ///     Entry point for calculating a skyline sample via the skyline sampling algorithm.
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
        /// <returns>The skyline sample.</returns>
        public DataTable GetSkylineTable(string query, string operators)
        {
            Utility.CalculatePropertiesWithRespectToIncomparableOperators(operators);

            // get complete database just once, not for every subset skyline => performance, network traffic
            DataTable fullDataTable = Helper.GetDataTableFromSQL(query, SelectedStrategy.ConnectionString,
                SelectedStrategy.Provider);

            var dataTableTemplate = new DataTable();
            DataRecordTemplateForStoredProcedure = Helper.BuildDataRecord(fullDataTable, Utility.Operators.ToArray(),
                dataTableTemplate);

            AddGeneratedInternalColumn(fullDataTable);

            FillUtilityProperties(fullDataTable);

            IReadOnlyDictionary<long, object[]> database = Helper.GetDatabaseAccessibleByUniqueId(fullDataTable,
                Utility.ArtificialUniqueRowIdentifierColumnIndex, true);

            dataTableTemplate = new DataTable();
            DataRecordTemplate = Helper.BuildDataRecord(fullDataTable, Utility.Operators.ToArray(),
                dataTableTemplate);

            return GetSkyline(database, dataTableTemplate);
        }

        /// <summary>
        ///     Add InternalArtificialUniqueRowIdentifierColumnName.
        /// </summary>
        /// <param name="dataTable">The DataTable on which to add the column.</param>
        private static void AddGeneratedInternalColumn(DataTable dataTable)
        {
            var internalArtificialUniqueRowIdentifierColumn = new DataColumn()
            {
                ColumnName = InternalArtificialUniqueRowIdentifierColumnName,
                DataType = typeof (long),
                DefaultValue = 0
            };          
            dataTable.Columns.Add(internalArtificialUniqueRowIdentifierColumn);
        }

        /// <summary>
        ///     Sets necessary properties of Utility.
        /// </summary>
        /// <param name="dataTable">
        ///     A DataTable to which the column InternalArtificialUniqueRowIdentifierColumnName has already been added.
        /// </param>
        private void FillUtilityProperties(DataTable dataTable)
        {
            Utility.SubsetCount = SubsetCount;
            Utility.SubsetDimension = SubsetDimension;
            Utility.AllPreferencesCount = Utility.Operators.Count(op => op != "INCOMPARABLE");
            Utility.ArtificialUniqueRowIdentifierColumnIndex = dataTable.Columns.Count - 1;
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
        ///     An empty DataTable with all columns to return to which the column InternalArtificialUniqueRowIdentifierColumnName
        ///     has already been added.
        /// </param>
        /// <returns>The skyline sample.</returns>
        internal DataTable GetSkyline(IReadOnlyDictionary<long, object[]> database, DataTable dataTableTemplate)
        {
            var sw = new Stopwatch();
            sw.Start();

            // preserve current skyline algorithm's settings in order to restore them before returning
            int recordAmountLimit = SelectedStrategy.RecordAmountLimit;
            int sortType = SelectedStrategy.SortType;
            bool hasIncomparablePreferences = SelectedStrategy.HasIncomparablePreferences;

            // the calculated subset skylines need all rows, sorting is unnecessary
            SelectedStrategy.RecordAmountLimit = 0;
            SelectedStrategy.SortType = 0;

            TimeMilliseconds = 0;
            NumberOfOperations = 0;

            // clone since 
            DataTable skylineSampleReturn = dataTableTemplate.Clone();
            var skylineValues = new List<long[]>();

            IReadOnlyDictionary<long, object[]> skylineSampleFinalDatabase =
                CalculateSkylineSampleFinalDatabase(database,
                    dataTableTemplate, sw);
            CalculateSkylineSample(skylineSampleFinalDatabase, skylineSampleReturn, skylineValues);

            RemoveGeneratedInternalColumn(skylineSampleReturn);

            // restore current skyline algorithm's settings
            SelectedStrategy.RecordAmountLimit = recordAmountLimit;
            SelectedStrategy.SortType = sortType;
            SelectedStrategy.HasIncomparablePreferences = hasIncomparablePreferences;

            SortDataTable(skylineSampleReturn, skylineValues);

            //Remove certain amount of rows if query contains TOP Keyword
            Helper.GetAmountOfTuples(skylineSampleReturn, recordAmountLimit);

            sw.Stop();
            TimeMilliseconds += sw.ElapsedMilliseconds;

            return skylineSampleReturn;
        }

        /// <summary>
        ///     Calculate the necessary subset skylines and merge them into the skyline sample which will finally be reported by
        ///     the skyline sampling algorithm.
        /// </summary>
        /// <remarks>
        ///     Calculate subsets of preferences. For each subset, calculate a skyline via the selected skyline algorithm. Determine the
        ///     objects for which the calculation of a subset skyline with respect to the subset's complement is necessary; if
        ///     so, calculate this subset complement skyline via the selected skyline algorithm and remove dominated objects from
        ///     the subset skyline. Finally, merge the subset skyline into the skyline sample which will be reported by the
        ///     skyline sampling algorithm.
        /// </remarks>
        /// <param name="database">
        ///     A Collection by which a row can be accessed via its unique ID. The values represent the database
        ///     rows including the preceding SkylineAttribute columns.
        /// </param>
        /// <param name="dataTableTemplate">
        ///     An empty DataTable with all columns to return to which the column
        ///     InternalArtificialUniqueRowIdentifierColumnName has already been added.
        /// </param>
        /// <param name="sw">
        ///     To measture the time spent to perform this whole algorithm. Has to be started before calling this
        ///     method, will be running after this method.
        /// </param>
        private IReadOnlyDictionary<long, object[]> CalculateSkylineSampleFinalDatabase(
            IReadOnlyDictionary<long, object[]> database, DataTable dataTableTemplate, Stopwatch sw)
        {
            var skylineSampleFinalDatabase = new Dictionary<long, object[]>();

            foreach (CLRSafeHashSet<int> subset in Utility.Subsets)
            {
                IEnumerable<object[]> useDatabase = database.Values;

                string subpaceOperators = GetOperatorsWithIgnoredEntriesString(subset);

                SelectedStrategy.HasIncomparablePreferences = subpaceOperators.Contains("INCOMPARABLE");

                SelectedStrategy.PrepareDatabaseForAlgorithm(ref useDatabase, subset.ToList(),
                    Utility.PreferenceColumnIndex, Utility.IsPreferenceIncomparable);

                sw.Stop();
                TimeMilliseconds += sw.ElapsedMilliseconds;
                DataTable subsetDataTable = SelectedStrategy.GetSkylineTable(useDatabase,
                    dataTableTemplate.Clone(), DataRecordTemplate, subpaceOperators);
                TimeMilliseconds += SelectedStrategy.TimeMilliseconds;
                NumberOfOperations += SelectedStrategy.NumberOfComparisons;
                sw.Restart();

                IDictionary<long, object[]> subsetDatabase = GetDatabaseFromDataTable(database, subsetDataTable);

                IEnumerable<Tuple<long[], string[]>> databaseForPairwiseComparison =
                    GetDatabaseForPairwiseComparison(subsetDatabase.Values, subset.ToList());

                IEnumerable<CLRSafeHashSet<long>> rowsWithEqualValuesWithRespectToSubsetColumns =
                    CompareEachRowWithRespectToSubsetColumnsPairwise(databaseForPairwiseComparison);

                CLRSafeHashSet<int> subsetComplement = Utility.GetSubsetComplement(subset);
                string subsetComplementOperators =
                    GetOperatorsWithIgnoredEntriesString(Utility.GetSubsetComplement(subset));

                SelectedStrategy.HasIncomparablePreferences = subsetComplementOperators.Contains("INCOMPARABLE");
                List<int> subsetComplementList = subsetComplement.ToList();

                foreach (CLRSafeHashSet<long> rowsWithEqualValues in rowsWithEqualValuesWithRespectToSubsetColumns)
                {
                    IReadOnlyDictionary<long, object[]> rowsWithEqualValuesDatabase = GetSubsetOfDatabase(database,
                        rowsWithEqualValues);

                    useDatabase = rowsWithEqualValuesDatabase.Values;
                    SelectedStrategy.PrepareDatabaseForAlgorithm(ref useDatabase, subsetComplementList,
                        Utility.PreferenceColumnIndex, Utility.IsPreferenceIncomparable);

                    sw.Stop();
                    TimeMilliseconds += sw.ElapsedMilliseconds;
                    DataTable subsetComplementDataTable =
                        SelectedStrategy.GetSkylineTable(useDatabase,
                            dataTableTemplate.Clone(), DataRecordTemplate, subsetComplementOperators);
                    TimeMilliseconds += SelectedStrategy.TimeMilliseconds;
                    NumberOfOperations += SelectedStrategy.NumberOfComparisons;
                    sw.Restart();

                    IReadOnlyDictionary<long, object[]> subsetComplementDatabase =
                        new ReadOnlyDictionary<long, object[]>(GetDatabaseFromDataTable(database,
                            subsetComplementDataTable));

                    RemoveDominatedObjects(rowsWithEqualValues, subsetComplementDatabase, subsetDatabase);
                }

                MergeSubsetSkylineIntoFinalSkylineSample(new ReadOnlyDictionary<long, object[]>(subsetDatabase),
                    skylineSampleFinalDatabase);
            }

            return skylineSampleFinalDatabase;
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        /// <param name="subset"></param>
        /// <returns></returns>
        private string GetOperatorsWithIgnoredEntriesString(ICollection<int> subset)
        {
            var operatorsReturn = "";

            for (var i = 0; i < Utility.OperatorStrings.Length; i++)
            {
                if (!subset.Contains(i))
                {
                    if (Utility.IsPreferenceIncomparable[i])
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
                    operatorsReturn += Utility.OperatorStrings[i] + ";";
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
                                           Utility.AllPreferencesCount -
                                           Utility.Operators.Count(op => op == "INCOMPARABLE");

            return dataTable.Rows.Cast<DataRow>()
                .ToDictionary(row => (long) row[rowIdentifierColumnIndex],
                    row => database[(long) row[rowIdentifierColumnIndex]]);
        }

        /// <summary>
        ///     Get attribute's values (and strings if INCOMPARABLE) of the database.
        /// </summary>
        /// <param name="database">Database from which to extract attributes.</param>
        /// <param name="subset">Subset used for the subset skyline. Needs guaranteed stable order.</param>
        /// <returns>IEnumerable of the attribute's values together with string values if attribute is INCOMPARABLE.</returns>
        private IEnumerable<Tuple<long[], string[]>> GetDatabaseForPairwiseComparison(
            IEnumerable<object[]> database,
            IList<int> subset)
        {
            var subsetForPairwiseComparison = new List<Tuple<long[], string[]>>();

            foreach (object[] row in database)
            {
                var rowForPairwiseComparison = new long[subset.Count + 1];
                var rowForPairwiseComparisonIncomparable = new string[subset.Count];

                var count = 0;
                foreach (int sub in subset)
                {
                    int rowIndex = Utility.PreferenceColumnIndex[sub];
                    rowForPairwiseComparison[count] = (long) row[rowIndex];

                    if (Utility.IsPreferenceIncomparable[sub])
                    {
                        rowForPairwiseComparisonIncomparable[count] = (string) row[rowIndex + 1];
                    }

                    count++;
                }

                rowForPairwiseComparison[count] = (long) row[Utility.ArtificialUniqueRowIdentifierColumnIndex];

                subsetForPairwiseComparison.Add(new Tuple<long[], string[]>(rowForPairwiseComparison,
                    rowForPairwiseComparisonIncomparable));
            }

            return subsetForPairwiseComparison;
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        /// <remarks>
        ///     TODO: remarks about performance (this is the most time consuming method besides the actual skyline algorithm)
        /// </remarks>
        /// <param name="subsetDatabase"></param>
        /// <returns></returns>
        private IEnumerable<CLRSafeHashSet<long>> CompareEachRowWithRespectToSubsetColumnsPairwise(
            IEnumerable<Tuple<long[], string[]>> subsetDatabase)
        {
            List<Tuple<long[], string[]>> database = subsetDatabase.ToList();
            var usedBuckets = new Dictionary<string, long> {{"", 0}};

            if (database.Count == 0)
            {
                return new List<CLRSafeHashSet<long>>();
            }

            int columnsInSubsetCount = database[0].Item1.Length-1;
            int rowIdentifierColumnIndex = database[0].Item1.Length-1;

            // sort on first subset attribute in order to skip some comparisons later
            database.Sort((row1, row2) => (row1.Item1[0]).CompareTo(row2.Item1[0]));

            // compare pairwise, initially skip first element to prevent comparison of elements with themselves
            var databaseForComparison = new List<Tuple<long[], string[]>>(database);

            // store the actual values which are equal to another database row in order to create separate lists
            // for each combination of equal values
            var equalValues = new string[columnsInSubsetCount];

            // performance; access counter only once
            int databaseCount = database.Count;

            var equalRowsWithRespectToSubsetColumns = new Dictionary<long, CLRSafeHashSet<long>>();

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

                    bool isDatabaseRowEqualToDatabaseForComparisonRow = DetermineEqualValues(columnsInSubsetCount,
                        databaseRowValueFull, databaseForComparisonRowValueFull, equalValues);

                    if (!isDatabaseRowEqualToDatabaseForComparisonRow)
                    {
                        // at least one value of the subset differs, so the current row of databaseForComparison
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
                    // values in their subset                  
                    if (!equalRowsWithRespectToSubsetColumns.ContainsKey(equalValuesBucket))
                    {
                        equalRowsWithRespectToSubsetColumns.Add(equalValuesBucket, new CLRSafeHashSet<long>());
                    }

                    equalRowsWithRespectToSubsetColumns[equalValuesBucket].Add(databaseRowValue[rowIdentifierColumnIndex]);
                    equalRowsWithRespectToSubsetColumns[equalValuesBucket].Add(databaseForComparisonRowValue[rowIdentifierColumnIndex]);
                }

                databaseForComparison = databaseForComparisonForNextIteration;

                if (databaseForComparison.Count == 0)
                {
                    // terminate outer iteration over database
                    break;
                }
            }

            // return list of row identifiers for each row in each bucket          
            return equalRowsWithRespectToSubsetColumns.Values;
        }

        private bool DetermineEqualValues(int columnsInSubsetCount, Tuple<long[], string[]> databaseRowValue,
            Tuple<long[], string[]> databaseForComparisonRowValue, string[] equalValues)
        {
            var isDatabaseRowEqualToDatabaseForComparisonRow = true;

            // start from index 1, since the sorted column 0 is already set in equalValues[0]
            for (var subsetColumnIndex = 1;
                subsetColumnIndex < columnsInSubsetCount;
                subsetColumnIndex++)
            {
                // performance; dereference only once
                long databaseRowColumnValue = databaseRowValue.Item1[subsetColumnIndex];
                // performance; dereference only once
                long databaseForComparisonRowColumnValue = databaseForComparisonRowValue.Item1[subsetColumnIndex];

                if (databaseRowColumnValue != databaseForComparisonRowColumnValue)
                {
                    // one value differs, break this loop
                    isDatabaseRowEqualToDatabaseForComparisonRow = false;
                    break;
                }

                string eqValue = databaseRowColumnValue.ToString();

                if (Utility.IsPreferenceIncomparable[subsetColumnIndex])
                {
                    // performance; dereference only once
                    string databaseRowColumnValueIncomparable = databaseRowValue.Item2[subsetColumnIndex];
                    // performance; dereference only once
                    string databaseForComparisonRowColumnValueIncomparable =
                        databaseForComparisonRowValue.Item2[subsetColumnIndex];

                    if (databaseRowColumnValueIncomparable != databaseForComparisonRowColumnValueIncomparable)
                    {
                        // one value differs, break this loop
                        isDatabaseRowEqualToDatabaseForComparisonRow = false;
                        break;
                    }

                    eqValue += "-" + databaseRowColumnValueIncomparable;
                }

                equalValues[subsetColumnIndex] = eqValue;
            }

            return isDatabaseRowEqualToDatabaseForComparisonRow;
        }  

        private static IReadOnlyDictionary<long, object[]> GetSubsetOfDatabase(
            IReadOnlyDictionary<long, object[]> database,
            IEnumerable<long> databaseSubsetKeys)
        {
            return
                new ReadOnlyDictionary<long, object[]>(databaseSubsetKeys.ToDictionary(row => row, row => database[row]));
        }

        /// <summary>
        ///     Remove objects from destinationDatabase that are contained in potentiallyDominatedObjects but not in
        ///     dominatingObjectsDatabase.
        /// </summary>
        /// <param name="potentiallyDominatedObjects">Objects with equal values with respect to a subset of preferences that might be dominated with respect to all preferences.</param>
        /// <param name="dominatingObjectsDatabase">Objects in potentiallyDominatedObjects that are not dominated with respect to all preferences.</param>
        /// <param name="destinationDatabase">Database from which to remove objects dominated with respect to all preferences.</param>
        private static void RemoveDominatedObjects(
            IEnumerable<long> potentiallyDominatedObjects,
            IReadOnlyDictionary<long, object[]> dominatingObjectsDatabase,
            IDictionary<long, object[]> destinationDatabase)
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
        private static void MergeSubsetSkylineIntoFinalSkylineSample(
            IReadOnlyDictionary<long, object[]> sourceDatabase,
            IDictionary<long, object[]> destinationDatabase)
        {
            foreach (KeyValuePair<long, object[]> subsetDataTableOrigObject in sourceDatabase)
            {
                if (!destinationDatabase.ContainsKey(subsetDataTableOrigObject.Key))
                {
                    destinationDatabase.Add(subsetDataTableOrigObject.Key, subsetDataTableOrigObject.Value);
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
                    if (i < Utility.Operators.Length)
                    {
                        if (Utility.Operators[i] != "INCOMPARABLE")
                        {
                            int rowIndex = Utility.PreferenceColumnIndex[count];
                            skylineValueRow[count] = (long) skylineSampleRow.Value[rowIndex];
                            count++;
                        }
                    }
                    else
                    {
                        row[i - Utility.Operators.Length] = skylineSampleRow.Value[i];
                    }
                }

                skylineSampleReturn.Rows.Add(row);
                skylineValues.Add(skylineValueRow);
            }
        }

        private static void RemoveGeneratedInternalColumn(DataTable dataTable)
        {
            dataTable.Columns.RemoveAt(dataTable.Columns.Count - 1); // InternalArtificialUniqueRowIdentifierColumnName
        }

        private void SortDataTable(DataTable dataTable, List<long[]> skylineValues)
        {
            switch (SelectedStrategy.SortType)
            {
                case 1:
                    Helper.SortBySum(dataTable, skylineValues);
                    break;
                case 2:
                    Helper.SortByRank(dataTable, skylineValues);
                    break;
                default:
                    break;
            }
        }
    }
}