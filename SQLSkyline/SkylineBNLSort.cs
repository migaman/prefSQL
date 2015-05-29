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

        public override void PrepareDatabaseForAlgorithm(ref IEnumerable<object[]> useDatabase, List<int> subspaceList, int[] preferenceColumnIndex, string[] operatorStrings)
        {
            List<object[]> useTempDatabase = useDatabase.ToList();
            useTempDatabase.Sort((item1, item2) => comp(item1, item2, subspaceList, preferenceColumnIndex, operatorStrings));
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

        private int comp(object[] x, object[] y, IEnumerable<int> subspace, int[] preferenceColumnIndex, string[] operatorStrings)
        {
            foreach (int subspaceColumnIndex in subspace)
            {
                int databaseIndex = preferenceColumnIndex[subspaceColumnIndex];
                if ((long)x[databaseIndex] < (long)y[databaseIndex])
                {
                    return -1;
                }
                if ((long)x[databaseIndex] > (long)y[databaseIndex])
                {
                    return 1;
                }

                if (operatorStrings[subspaceColumnIndex].Contains(';'))
                {
                    switch (
                        string.Compare(((string)x[databaseIndex + 1]), (string)y[databaseIndex + 1],
                            StringComparison.InvariantCulture))
                    {
                        case -1:
                            return -1;
                        case 1:
                            return 1;
                    }
                }
            }
            return 0;
        }
    }
}
