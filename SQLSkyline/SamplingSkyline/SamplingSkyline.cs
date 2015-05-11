using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using Microsoft.SqlServer.Server;

namespace prefSQL.SQLSkyline.SamplingSkyline
{
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
            var skylineAlgorithmParameters = new SkylineAlgorithmParameters(operators, numberOfRecords, hasIncomparable,
                additionalParameters);

            ConfigureUtility(subspacesCount, subspaceDimension, skylineAlgorithmParameters.OperatorsCollection.Count);

            DataTable fullDataTable = Helper.GetSkylineDataTable(query, true, dbConnection, DbProvider);
            Dictionary<int, object[]> database = Helper.GetDictionaryFromDataTable(fullDataTable,
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
        internal DataTable GetSkyline(Dictionary<int, object[]> database, DataTable dataTableTemplate,
            SqlDataRecord dataRecordTemplate, SkylineStrategy skylineStrategy,
            SkylineAlgorithmParameters skylineAlgorithmParameters)
        {
            DataTable skylineSampleReturn = dataTableTemplate.Clone();
            var skylineSampleFinalDatabase = new Dictionary<int, object[]>();

            timeMilliseconds = 0;
            var sw = new Stopwatch();
            sw.Start();

            foreach (HashSet<int> subspace in Utility.Subspaces)
            {
                string subpaceOperators = string.Join(";",
                    GetOperatorsWithIgnoredEntries(skylineAlgorithmParameters.OperatorsCollection.ToArray(), subspace));
                var skylineAlgorithmSubspaceParameters = new SkylineAlgorithmParameters(subpaceOperators,
                    skylineAlgorithmParameters);

                sw.Stop();
                timeMilliseconds += sw.ElapsedMilliseconds;
                DataTable subspaceDataTable = GetSkylineTable(database, dataTableTemplate, dataRecordTemplate,
                    skylineStrategy, skylineAlgorithmSubspaceParameters);
                timeMilliseconds += skylineStrategy.TimeMilliseconds;
                sw.Restart();

                Dictionary<int, object[]> subspaceDatabase = GetDatabaseFromDataTable(database, subspaceDataTable);

                HashSet<int> equalRowsWithRespectToSubspaceColumns =
                    CompareEachRowWithRespectToSubspaceColumnsPairwise(subspaceDatabase, subspace);

                if (equalRowsWithRespectToSubspaceColumns.Count > 0)
                {
                    subpaceOperators = string.Join(";",
                        GetOperatorsWithIgnoredEntries(skylineAlgorithmParameters.OperatorsCollection.ToArray(),
                            GetSubspaceComplement(subspace)));
                    skylineAlgorithmSubspaceParameters = new SkylineAlgorithmParameters(subpaceOperators,
                        skylineAlgorithmParameters);

                    sw.Stop();
                    timeMilliseconds += sw.ElapsedMilliseconds;
                    DataTable subspaceComplementDataTable = GetSkylineTable(subspaceDatabase, dataTableTemplate,
                        dataRecordTemplate, skylineStrategy, skylineAlgorithmSubspaceParameters);
                    timeMilliseconds += skylineStrategy.TimeMilliseconds;
                    sw.Restart();

                    Dictionary<int, object[]> subspaceComplementDatabase = GetDatabaseFromDataTable(database,
                        subspaceComplementDataTable);

                    RemoveDominatedObjects(equalRowsWithRespectToSubspaceColumns,
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
            timeMilliseconds += sw.ElapsedMilliseconds;

            return skylineSampleReturn;
        }

        private static DataTable GetSkylineTable(Dictionary<int, object[]> subspaceDatabase, DataTable dataTableTemplate,
            SqlDataRecord dataRecordTemplate, SkylineStrategy skylineStrategy,
            SkylineAlgorithmParameters skylineAlgorithmParameters)
        {
            DataTable skylineDataTable =
                skylineStrategy.GetSkylineTable(subspaceDatabase.Values.ToList(),
                    dataTableTemplate, dataRecordTemplate, skylineAlgorithmParameters.Operators,
                    skylineAlgorithmParameters.NumberOfRecords,
                    skylineAlgorithmParameters.HasIncomparable,
                    skylineAlgorithmParameters.AdditionalParameters.ToArray());
            return skylineDataTable;
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
        private static Dictionary<int, object[]> GetDatabaseFromDataTable(
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
        private static HashSet<int> CompareEachRowWithRespectToSubspaceColumnsPairwise(
            Dictionary<int, object[]> subspaceDatabase, IEnumerable<int> columnsUsedInSubspace)
        {
            var equalRowsWithRespectToSubspaceColumns = new HashSet<int>();

            List<int> columnsUsedInSubspaceList = columnsUsedInSubspace.ToList();
            int columnsInSubspaceCount = columnsUsedInSubspaceList.Count;

            foreach (KeyValuePair<int, object[]> i in subspaceDatabase)
            {
                object[] iValue = i.Value;

                foreach (KeyValuePair<int, object[]> j in subspaceDatabase)
                {
                    if (i.Key == j.Key)
                    {
                        continue;
                    }

                    object[] jValue = j.Value;

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

                    if (isEqual)
                    {
                        equalRowsWithRespectToSubspaceColumns.Add(i.Key);
                        equalRowsWithRespectToSubspaceColumns.Add(j.Key);
                    }
                }
            }

            return equalRowsWithRespectToSubspaceColumns;
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
            IEnumerable<int> equalRowsWithRespectToSubspaceColumnsDataTable,
            IReadOnlyDictionary<int, object[]> subspaceComplementDataTable, IDictionary<int, object[]> subspaceDataTable)
        {
            foreach (
                var equalRow in
                    equalRowsWithRespectToSubspaceColumnsDataTable.Where(
                        equalRow => !subspaceComplementDataTable.ContainsKey(equalRow)))
            {
                subspaceDataTable.Remove(equalRow);
            }
        }
    }
}