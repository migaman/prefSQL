namespace prefSQL.SQLSkyline
{
    using System;
    using System.Collections.Generic;
    using System.Data;
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
            var objectArrayFromDataTable = FillbyRowId(objectArrayFromDataTableOrig);
            var sqlDataRecord = Helper.buildDataRecord(fullDataTable, operators, dataTableResult);

            return GetSkyline(objectArrayFromDataTable, algorithm, sqlDataRecord, operators, dataTableResult,
                numberOfRecords, hasIncomparable, additionalParameters);
        }

        private Dictionary<int, object[]> FillbyRowId(List<object[]> objectArrayFromDataTableOrig)
        {
            var result = new Dictionary<int, object[]>();

            foreach (var dataRow in objectArrayFromDataTableOrig)
            {
                result.Add((int) dataRow[Utility.AllPreferencesCount], dataRow);
            }

            return result;
        }

        internal DataTable GetSkyline(Dictionary<int, object[]> databaseAsObjectArray, SkylineStrategy algorithm,
            SqlDataRecord record, string[] operators, DataTable baseDataTableResult, int numberOfRecords,
            bool hasIncomparable, string[] additionalParameters)
        {
            var skylineSample = baseDataTableResult.Clone();
            var skylineSampleOrigObjects = new Dictionary<int, object[]>();
            var subspaceDataTableOrigObjects = new Dictionary<int, object[]>();
            var skylineSampleFinalObjects = new Dictionary<int, object[]>();

            foreach (var subspace in Utility.Subspaces)
            {
                var reducedObjectArrayFromDataTable = ReducedObjectArrayFromDataTable(databaseAsObjectArray, subspace);

                //var reducedRecord = ReduceRecord(baseDataTableResult.Clone(), subspace);
                var reducedOperators = ReducedOperators(operators, subspace);

                //var reducedDtResult = ReduceDtResult(baseDataTableResult, subspace);

                var subspaceDataTable = algorithm.getSkylineTable(reducedObjectArrayFromDataTable, record,
                    string.Join(";", reducedOperators),
                    numberOfRecords, hasIncomparable, additionalParameters, baseDataTableResult.Clone());

                subspaceDataTableOrigObjects = GetOriginalObjects(databaseAsObjectArray, subspaceDataTable, subspace);

                //var columnsUsedInSubspace = ColumnsUsedInSubspace(baseDataTableResult.Clone(), subspace);
                var equalRowsWithRespectToSubspaceColumnsDataTable =
                    CompareEachRowWithRespectToSubspaceColumnsPairwise(subspaceDataTableOrigObjects, subspace);

                if (equalRowsWithRespectToSubspaceColumnsDataTable.Count > 0)
                {
                    var subspaceComplement = GetSubspaceComplement(subspace);

                    reducedObjectArrayFromDataTable = ReducedObjectArrayFromDataTable(equalRowsWithRespectToSubspaceColumnsDataTable,
                        subspaceComplement);
                    reducedOperators = ReducedOperators(operators, subspaceComplement);
                    //reducedRecord = ReduceRecord(baseDataTableResult.Clone(), subspace);
                    //reducedDtResult = ReduceDtResult(baseDataTableResult, subspace);

                    var subspaceComplementDataTable = algorithm.getSkylineTable(reducedObjectArrayFromDataTable,
                        record, string.Join(";", reducedOperators),
                        numberOfRecords, hasIncomparable, additionalParameters, baseDataTableResult.Clone());

                    var subspaceComplementDataTableOrigObjects = GetOriginalObjects(databaseAsObjectArray,
                        subspaceComplementDataTable, subspace);

                    RemoveDominatedObjects(equalRowsWithRespectToSubspaceColumnsDataTable,
                        subspaceComplementDataTableOrigObjects,
                        subspaceDataTableOrigObjects);
                }

                Merge(subspaceDataTableOrigObjects, skylineSampleFinalObjects);
            }

            foreach (var item in skylineSampleFinalObjects)
            {
                var row = skylineSample.NewRow();
                for (var i = Utility.AllPreferencesCount; i < item.Value.Length; i++)
                {
                    row[i - Utility.AllPreferencesCount] = item.Value[i];
                }
                skylineSample.Rows.Add(row);
            }

            return skylineSample;
        }

        private void Merge(Dictionary<int, object[]> skylineSampleOrigObjects,
            Dictionary<int, object[]> subspaceDataTableOrigObjects)
        {
            foreach (var subspaceDataTableOrigObject in skylineSampleOrigObjects)
            {
                if (!subspaceDataTableOrigObjects.ContainsKey(subspaceDataTableOrigObject.Key))
                {
                    subspaceDataTableOrigObjects.Add(subspaceDataTableOrigObject.Key, subspaceDataTableOrigObject.Value);                    
                }
            }
        }

        private Dictionary<int, object[]> GetOriginalObjects(Dictionary<int, object[]> databaseAsObjectArray,
            DataTable reducedObjectArrayFromDataTable, HashSet<int> subspace)
        {
            var result = new Dictionary<int, object[]>();
            foreach (DataRow dataRow in reducedObjectArrayFromDataTable.Rows)
            {
                result.Add((int) dataRow[0], databaseAsObjectArray[(int) dataRow[0]]);
            }

            return result;
        }

        private static string[] ReducedOperators(string[] operators, HashSet<int> subspace)
        {
            var reducedOperators = new string[operators.Length];
            Array.Copy(operators, reducedOperators, operators.Length);
            foreach (var subspaceItem in subspace)
            {
                reducedOperators[subspaceItem] = "IGNORE";
            }

            reducedOperators = new string[subspace.Count];
            var count2 = 0;
            for (var i = 0; i < operators.Length; i++)
            {
                if (subspace.Contains(i))
                {
                    reducedOperators[count2] = operators[i];
                    count2++;
                }
            }
            return reducedOperators;
        }

        private List<object[]> ReducedObjectArrayFromDataTable(Dictionary<int, object[]> databaseAsObjectArray,
            HashSet<int> subspace)
        {
            var reducedObjectArrayFromDataTable = new List<object[]>();
            var objectFullDimension = databaseAsObjectArray.First().Value.Length;

            var objectDim = databaseAsObjectArray.First().Value.Length - Utility.AllPreferencesCount + subspace.Count;
            foreach (var item in databaseAsObjectArray)
            {
                var count = 0;
                var newObject = new object[objectDim];
                for (var i = 0; i < Utility.AllPreferencesCount; i++)
                {
                    if (subspace.Contains(i))
                    {
                        newObject[count] = item.Value[i];
                        count++;
                    }
                }

                for (var i = Utility.AllPreferencesCount; i < objectFullDimension; i++)
                {
                    newObject[i + count - Utility.AllPreferencesCount] = item.Value[i];
                }
                reducedObjectArrayFromDataTable.Add(newObject);
            }
            return reducedObjectArrayFromDataTable;
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
                            equalRowsWithRespectToSubspaceColumnsDataTable.Add(i.Key,i.Value);
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

        private static void RemoveDominatedObjects(Dictionary<int, object[]> equalRowsWithRespectToSubspaceColumnsDataTable,
            Dictionary<int, object[]> subspaceComplementDataTable, Dictionary<int, object[]> subspaceDataTable)
        {
            foreach (var equalRow in equalRowsWithRespectToSubspaceColumnsDataTable.Keys)
            {
                if (IsEqualRowStillContainedWithinSubspaceComplementSkyline(subspaceComplementDataTable, equalRow))
                {
                    continue;
                }

                subspaceDataTable.Remove(equalRow);
            }
        }

        private static bool IsEqualRowStillContainedWithinSubspaceComplementSkyline(
            Dictionary<int, object[]> subspaceComplementDataTable, int equalRow)
        {
            return subspaceComplementDataTable.ContainsKey(equalRow);

            //foreach (DataRow row in subspaceComplementDataTable.Rows)
            //{
            //    if (row["Id"].Equals(equalRow["Id"]))
            //    {
            //        return true;
            //    }
            //}

            //return false;
        }
    }
}