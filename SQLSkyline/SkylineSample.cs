namespace prefSQL.SQLSkyline
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using Microsoft.SqlServer.Server;

    public sealed class SkylineSample
    {
        public long timeMilliseconds;

        private SkylineSampleUtility Utility { get; set; }

        public DataTable getSkylineTable(string strConnection, string strQuery, string strOperators, int numberOfRecords,
            bool hasIncomparable, string[] additionalParameters, SkylineStrategy algorithm, int count, int dimension)
        {
            var dataTableResult = new DataTable();

            var operators = strOperators.ToString(CultureInfo.InvariantCulture).Split(';');

            Utility = new SkylineSampleUtility(operators.Length, count, dimension);

            var fullDataTable = Helper.GetSkylineDataTable(strQuery, true, strConnection);
            var objectArrayFromDataTableOrig = Helper.GetObjectArrayFromDataTable(fullDataTable);
            var objectArrayFromDataTable =
                objectArrayFromDataTableOrig.ToDictionary(dataRow => (int) dataRow[Utility.AllPreferencesCount]);
            var sqlDataRecord = Helper.buildDataRecord(fullDataTable, operators, dataTableResult);

            return GetSkyline(objectArrayFromDataTable, algorithm, sqlDataRecord, operators, dataTableResult,
                numberOfRecords, hasIncomparable, additionalParameters);
        }

        internal DataTable GetSkyline(Dictionary<int, object[]> databaseAsObjectArray, SkylineStrategy algorithm,
            SqlDataRecord record, string[] operators, DataTable baseDataTableResult, int numberOfRecords,
            bool hasIncomparable, string[] additionalParameters)
        {
            var skylineSampleReturn = baseDataTableResult.Clone();
            var skylineSampleFinalObjects = new Dictionary<int, object[]>();

            timeMilliseconds = 0;
            var sw = new Stopwatch();
            sw.Start();

            foreach (var subspace in Utility.Subspaces)
            {
                var reducedOperators = ReducedOperators(operators, subspace);

                sw.Stop();
                timeMilliseconds += sw.ElapsedMilliseconds;
                var subspaceDataTable = algorithm.getSkylineTable(databaseAsObjectArray.Values.ToList(), record,
                    string.Join(";", reducedOperators),
                    numberOfRecords, hasIncomparable, additionalParameters, baseDataTableResult.Clone());
                timeMilliseconds += algorithm.timeMilliseconds;
                sw.Restart();

                var subspaceDataTableOrigObjects = GetOriginalObjects(databaseAsObjectArray, subspaceDataTable);

                var equalRowsWithRespectToSubspaceColumnsDataTable =
                    CompareEachRowWithRespectToSubspaceColumnsPairwise(subspaceDataTableOrigObjects, subspace);

                if (equalRowsWithRespectToSubspaceColumnsDataTable.Count > 0)
                {
                    var subspaceComplement = GetSubspaceComplement(subspace);

                    reducedOperators = ReducedOperators(operators, subspaceComplement);

                    sw.Stop();
                    timeMilliseconds += sw.ElapsedMilliseconds;
                    var subspaceComplementDataTable =
                        algorithm.getSkylineTable(subspaceDataTableOrigObjects.Values.ToList(),
                            record, string.Join(";", reducedOperators),
                            numberOfRecords, hasIncomparable, additionalParameters, baseDataTableResult.Clone());
                    timeMilliseconds += algorithm.timeMilliseconds;
                    sw.Restart();

                    var subspaceComplementDataTableOrigObjects = GetOriginalObjects(databaseAsObjectArray,
                        subspaceComplementDataTable);

                    RemoveDominatedObjects(equalRowsWithRespectToSubspaceColumnsDataTable,
                        subspaceComplementDataTableOrigObjects,
                        subspaceDataTableOrigObjects);
                }

                Merge(subspaceDataTableOrigObjects, skylineSampleFinalObjects);
            }

            foreach (var item in skylineSampleFinalObjects)
            {
                var row = skylineSampleReturn.NewRow();
                for (var i = Utility.AllPreferencesCount; i < item.Value.Length; i++)
                {
                    row[i - Utility.AllPreferencesCount] = item.Value[i];
                }
                skylineSampleReturn.Rows.Add(row);
            }

            sw.Stop();
            timeMilliseconds += sw.ElapsedMilliseconds;

            return skylineSampleReturn;
        }

        private static void Merge(Dictionary<int, object[]> skylineSampleOrigObjects,
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

        private static Dictionary<int, object[]> GetOriginalObjects(
            IReadOnlyDictionary<int, object[]> databaseAsObjectArray, DataTable reducedObjectArrayFromDataTable)
        {
            return reducedObjectArrayFromDataTable.Rows.Cast<DataRow>()
                .ToDictionary(dataRow => (int) dataRow[0], dataRow => databaseAsObjectArray[(int) dataRow[0]]);
        }

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

        private static Dictionary<int, object[]> CompareEachRowWithRespectToSubspaceColumnsPairwise(
            Dictionary<int, object[]> subspaceDataTable,
            HashSet<int> columnsUsedInSubspace)
        {
            var equalRowsWithRespectToSubspaceColumnsDataTable = new Dictionary<int, object[]>();

            var equalRowsWithRespectToSubspaceColumns = new HashSet<object[]>();

            foreach (var i in subspaceDataTable)
            {
                foreach (var j in subspaceDataTable)
                {
                    if (i.Key != j.Key && columnsUsedInSubspace.All(
                        item => i.Value[item].Equals(j.Value[item])))
                    {
                        if (!equalRowsWithRespectToSubspaceColumns.Contains(i.Value))
                        {
                            equalRowsWithRespectToSubspaceColumnsDataTable.Add(i.Key, i.Value);
                            equalRowsWithRespectToSubspaceColumns.Add(i.Value);
                        }
                        if (!equalRowsWithRespectToSubspaceColumns.Contains(j.Value))
                        {
                            equalRowsWithRespectToSubspaceColumnsDataTable.Add(j.Key, j.Value);
                            equalRowsWithRespectToSubspaceColumns.Add(j.Value);
                        }
                    }
                }
            }

            return equalRowsWithRespectToSubspaceColumnsDataTable;
        }

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