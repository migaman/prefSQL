namespace prefSQL.SQLSkyline.SamplingSkyline
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
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
        public string DbProvider { get; set; }

        private SamplingSkylineUtility Utility
        {
            get { return _utility; }
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        public long timeMilliseconds;

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
            string[] operatorsArray = operators.ToString(CultureInfo.InvariantCulture).Split(';');

            ConfigureUtility(subspacesCount, subspaceDimension, operatorsArray.Length);

            DataTable fullDataTable = Helper.GetSkylineDataTable(query, true, dbConnection, DbProvider);
            Dictionary<int, object[]> database = Helper.GetDictionaryFromDataTable(fullDataTable,
                Utility.AllPreferencesCount + uniqueIdColumnIndex);
            var dataTableTemplate = new DataTable();
            SqlDataRecord dataRecordTemplate = Helper.buildDataRecord(fullDataTable, operatorsArray, dataTableTemplate);

            return GetSkyline(database, dataTableTemplate, dataRecordTemplate, skylineStrategy, operatorsArray,
                numberOfRecords, hasIncomparable, additionalParameters);
        }

        private void ConfigureUtility(int subspacesCount, int subspaceDimension, int operatorsArray)
        {
            Utility.AllPreferencesCount = operatorsArray;
            Utility.SubspacesCount = subspacesCount;
            Utility.SubspaceDimension = subspaceDimension;
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        /// <param name="database"></param>
        /// <param name="dataTableTemplate"></param>
        /// <param name="dataRecordTemplate"></param>
        /// <param name="skylineStrategy"></param>
        /// <param name="operators"></param>
        /// <param name="numberOfRecords"></param>
        /// <param name="hasIncomparable"></param>
        /// <param name="additionalParameters"></param>
        /// <returns></returns>
        internal DataTable GetSkyline(Dictionary<int, object[]> database, DataTable dataTableTemplate,
            SqlDataRecord dataRecordTemplate, SkylineStrategy skylineStrategy, string[] operators, int numberOfRecords,
            bool hasIncomparable, string[] additionalParameters)
        {
            DataTable skylineSampleReturn = dataTableTemplate.Clone();
            var skylineSampleFinalObjects = new Dictionary<int, object[]>();

            timeMilliseconds = 0;
            var sw = new Stopwatch();
            sw.Start();

            foreach (var subspace in Utility.Subspaces)
            {
                string[] reducedOperators = ReducedOperators(operators, subspace);

                sw.Stop();
                timeMilliseconds += sw.ElapsedMilliseconds;
                DataTable subspaceDataTable = skylineStrategy.getSkylineTable(database.Values.ToList(),
                    dataRecordTemplate,
                    string.Join(";", reducedOperators),
                    numberOfRecords, hasIncomparable, additionalParameters, dataTableTemplate);
                timeMilliseconds += skylineStrategy.timeMilliseconds;
                sw.Restart();

                Dictionary<int, object[]> subspaceDataTableOrigObjects = GetOriginalObjects(database, subspaceDataTable);

                Dictionary<int, object[]> equalRowsWithRespectToSubspaceColumnsDataTable =
                    CompareEachRowWithRespectToSubspaceColumnsPairwise(subspaceDataTableOrigObjects, subspace);

                if (equalRowsWithRespectToSubspaceColumnsDataTable.Count > 0)
                {
                    HashSet<int> subspaceComplement = GetSubspaceComplement(subspace);

                    reducedOperators = ReducedOperators(operators, subspaceComplement);

                    sw.Stop();
                    timeMilliseconds += sw.ElapsedMilliseconds;
                    DataTable subspaceComplementDataTable =
                        skylineStrategy.getSkylineTable(subspaceDataTableOrigObjects.Values.ToList(), dataRecordTemplate,
                            string.Join(";", reducedOperators), numberOfRecords, hasIncomparable, additionalParameters,
                            dataTableTemplate);
                    timeMilliseconds += skylineStrategy.timeMilliseconds;
                    sw.Restart();

                    Dictionary<int, object[]> subspaceComplementDataTableOrigObjects = GetOriginalObjects(database,
                        subspaceComplementDataTable);

                    RemoveDominatedObjects(equalRowsWithRespectToSubspaceColumnsDataTable,
                        subspaceComplementDataTableOrigObjects,
                        subspaceDataTableOrigObjects);
                }

                MergeSubspaceSkylineIntoFinalSkylineSample(subspaceDataTableOrigObjects, skylineSampleFinalObjects);
            }

            foreach (KeyValuePair<int, object[]> item in skylineSampleFinalObjects)
            {
                DataRow row = skylineSampleReturn.NewRow();
                for (int i = Utility.AllPreferencesCount; i < item.Value.Length; i++)
                {
                    row[i - Utility.AllPreferencesCount] = item.Value[i];
                }
                skylineSampleReturn.Rows.Add(row);
            }

            sw.Stop();
            timeMilliseconds += sw.ElapsedMilliseconds;

            return skylineSampleReturn;
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        /// <param name="skylineSampleOrigObjects"></param>
        /// <param name="subspaceDataTableOrigObjects"></param>
        private static void MergeSubspaceSkylineIntoFinalSkylineSample(
            Dictionary<int, object[]> skylineSampleOrigObjects,
            IDictionary<int, object[]> subspaceDataTableOrigObjects)
        {
            foreach (var subspaceDataTableOrigObject in skylineSampleOrigObjects)
            {
                if (!subspaceDataTableOrigObjects.ContainsKey(subspaceDataTableOrigObject.Key))
                {
                    subspaceDataTableOrigObjects.Add(subspaceDataTableOrigObject.Key, subspaceDataTableOrigObject.Value);
                }
            }
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        /// <param name="databaseAsObjectArray"></param>
        /// <param name="reducedObjectArrayFromDataTable"></param>
        /// <returns></returns>
        private static Dictionary<int, object[]> GetOriginalObjects(
            IReadOnlyDictionary<int, object[]> databaseAsObjectArray, DataTable reducedObjectArrayFromDataTable)
        {
            return reducedObjectArrayFromDataTable.Rows.Cast<DataRow>()
                .ToDictionary(dataRow => (int) dataRow[0], dataRow => databaseAsObjectArray[(int) dataRow[0]]);
        }

        /// <summary>
        ///     TODO: comment
        /// </summary>
        /// <param name="operators"></param>
        /// <param name="subspace"></param>
        /// <returns></returns>
        private static string[] ReducedOperators(string[] operators, ICollection<int> subspace)
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
        /// <param name="subspaceDataTable"></param>
        /// <param name="columnsUsedInSubspace"></param>
        /// <returns></returns>
        private static Dictionary<int, object[]> CompareEachRowWithRespectToSubspaceColumnsPairwise(
            Dictionary<int, object[]> subspaceDataTable,
            IEnumerable<int> columnsUsedInSubspace)
        {
            var equalRowsWithRespectToSubspaceColumnsDataTable = new Dictionary<int, object[]>();

            var columnsUsedInSubspaceList = columnsUsedInSubspace.ToList();
            var equalRowsWithRespectToSubspaceColumns = new HashSet<int>();
            var columnsInSubspaceCount = columnsUsedInSubspaceList.Count;

            foreach (var i in subspaceDataTable)
            {
                var iValue = i.Value;
                foreach (var j in subspaceDataTable)
                {
                    var jValue = j.Value;

                    if (i.Key == j.Key)
                    {
                        continue;
                    }

                    var isEqual = true;

                    for (var k = 0; k < columnsInSubspaceCount; k++)
                    {
                        var column = columnsUsedInSubspaceList[k];
                        var iColumnValue = (long) iValue[column];
                        var jColumnValue = (long) jValue[column];

                        if (iColumnValue != jColumnValue)
                        {
                            isEqual = false;
                            break;
                        }
                    }

                    if (isEqual)
                    {
                        if (!equalRowsWithRespectToSubspaceColumns.Contains(i.Key))
                        {
                            equalRowsWithRespectToSubspaceColumnsDataTable.Add(i.Key, i.Value);
                            equalRowsWithRespectToSubspaceColumns.Add(i.Key);
                        }
                        if (!equalRowsWithRespectToSubspaceColumns.Contains(j.Key))
                        {
                            equalRowsWithRespectToSubspaceColumnsDataTable.Add(j.Key, j.Value);
                            equalRowsWithRespectToSubspaceColumns.Add(j.Key);
                        }
                    }
                }
            }

            return equalRowsWithRespectToSubspaceColumnsDataTable;
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
        /// <param name="equalRowsWithRespectToSubspaceColumnsDataTable"></param>
        /// <param name="subspaceComplementDataTable"></param>
        /// <param name="subspaceDataTable"></param>
        private static void RemoveDominatedObjects(
            Dictionary<int, object[]> equalRowsWithRespectToSubspaceColumnsDataTable,
            IReadOnlyDictionary<int, object[]> subspaceComplementDataTable, IDictionary<int, object[]> subspaceDataTable)
        {
            foreach (
                var equalRow in
                    equalRowsWithRespectToSubspaceColumnsDataTable.Keys.Where(
                        equalRow => !subspaceComplementDataTable.ContainsKey(equalRow)))
            {
                subspaceDataTable.Remove(equalRow);
            }
        }
    }
}