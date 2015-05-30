//------------------------------------------------------------------------------
// <copyright file="CSSqlClassFile.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.SqlServer.Server;

namespace prefSQL.SQLSkyline
{
    using System.Linq;

    public class SkylineBNLSort : SkylineStrategy
    {
        

        public override bool IsNative()
        {
            return false;
        }

        public override bool SupportImplicitPreference()
        {
            return true;
        }

        public override bool SupportIncomparable()
        {
            return true;
        }

        public override void PrepareDatabaseForAlgorithm(ref IEnumerable<object[]> useDatabase, List<int> subspace, int[] preferenceColumnIndex, bool[] isPreferenceIncomparable)
        {
            List<object[]> useTempDatabase = useDatabase.ToList();
            List<int> databaseIndices = subspace.Select(subspaceColumnIndex => preferenceColumnIndex[subspaceColumnIndex]).ToList();
            useTempDatabase.Sort((item1, item2) => CompareTwoDatabaseObjects(item1, item2, databaseIndices, isPreferenceIncomparable));
            useDatabase = useTempDatabase;
        }
      
        public override string GetStoredProcedureCommand(string strWhere, string strOrderBy, string strFirstSQL, string strOperators, string strOrderByAttributes)
        {
            strFirstSQL += strOrderByAttributes;
            //Quote quotes because it is a parameter of the stored procedure
            strFirstSQL = strFirstSQL.Replace("'", "''");
            string strSQLReturn;
            if (HasIncomparablePreferences)
            {
                strSQLReturn = "EXEC dbo.SP_SkylineBNLSort '" + strFirstSQL + "', '" + strOperators + "', " + RecordAmountLimit + ", " + SortType;
            }
            else
            {
                strSQLReturn = "EXEC dbo.SP_SkylineBNLSortLevel '" + strFirstSQL + "', '" + strOperators + "', " + RecordAmountLimit + ", " + SortType;
            }
            return strSQLReturn;
            
        }

        public override DataTable GetSkylineTable(String querySQL, String preferenceOperators)
        {
            Strategy = getSP_Skyline();
            DataTable dt = Strategy.GetSkylineTable(querySQL, preferenceOperators, RecordAmountLimit, true, ConnectionString, Provider, AdditionParameters, SortType);
            TimeMilliseconds = Strategy.TimeInMs;
            NumberOfOperations = Strategy.NumberOfOperations;
            return dt;         
        }

        internal override DataTable GetSkylineTable(IEnumerable<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string preferenceOperators)
        {
            Strategy = getSP_Skyline();
            DataTable dt = Strategy.GetSkylineTable(database, dataTableTemplate, dataRecordTemplate, preferenceOperators, RecordAmountLimit, true, SortType, AdditionParameters);
            TimeMilliseconds = Strategy.TimeInMs;
            NumberOfOperations = Strategy.NumberOfOperations;
            return dt;            
        }

        private TemplateStrategy getSP_Skyline()
        {
            if (HasIncomparablePreferences)
            {
                return new SPSkylineBNLSort();
            }

            return new SPSkylineBNLSortLevel();
        }

        private static int CompareTwoDatabaseObjects(object[] item1, object[] item2, IList<int> databaseIndices, bool[] isPreferenceIncomparable)
        {
            int databaseIndicesCount = databaseIndices.Count;

            for(var i=0;i<databaseIndicesCount;i++)
            {
                int databaseIndex = databaseIndices[i];

                var item1ForComparison = (long) item1[databaseIndex];
                var item2ForComparison = (long) item2[databaseIndex];

                if (item1ForComparison < item2ForComparison)
                {
                    return -1;
                }

                if (item1ForComparison > item2ForComparison)
                {
                    return 1;
                }

                if (!isPreferenceIncomparable[i])
                {
                    continue;
                }

                switch (
                    string.Compare(((string) item1[databaseIndex + 1]), (string) item2[databaseIndex + 1],
                        StringComparison.InvariantCulture))
                {
                    case -1:
                        return -1;
                    case 1:
                        return 1;
                }
            }

            return 0;
        }
    }
}
