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
    public class SkylineBNL : SkylineStrategy
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
            // deliberately empty since no preparations necessary
        }

        public override string GetStoredProcedureCommand(string strWhere, string strOrderBy, string strFirstSQL, string strOperators, string strOrderByAttributes)
        {
            //usual sort clause
            strFirstSQL += strOrderBy;
            //Quote quotes because it is a parameter of the stored procedure
            strFirstSQL = strFirstSQL.Replace("'", "''");
            string strSQLReturn;
            if (HasIncomparablePreferences)
            {
                strSQLReturn = "EXEC dbo.SP_SkylineBNL '" + strFirstSQL + "', '" + strOperators + "', " + RecordAmountLimit + ", " + SortType;
            }
            else
            {
                strSQLReturn = "EXEC dbo.SP_SkylineBNLLevel '" + strFirstSQL + "', '" + strOperators + "', " + RecordAmountLimit + ", " + SortType;
            }
            return strSQLReturn;
        }

        public override DataTable GetSkylineTable(String querySQL, String preferenceOperators)
        {
            Strategy = getSP_Skyline();
            DataTable dt = Strategy.GetSkylineTable(querySQL, preferenceOperators, RecordAmountLimit, true, ConnectionString, Provider, AdditionParameters, SortType);
            TimeMilliseconds = Strategy.TimeInMs;
            NumberOfComparisons = Strategy.NumberOfOperations;
            return dt;         
        }

        internal override DataTable GetSkylineTable(IEnumerable<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string preferenceOperators)
        {
            Strategy = getSP_Skyline();
            DataTable dt = Strategy.GetSkylineTable(database, dataTableTemplate, dataRecordTemplate, preferenceOperators, RecordAmountLimit, true, SortType, AdditionParameters);
            TimeMilliseconds = Strategy.TimeInMs;
            NumberOfComparisons = Strategy.NumberOfOperations;
            return dt;
        }

        private TemplateStrategy getSP_Skyline()
        {
            if (HasIncomparablePreferences)
            {
                return new SPSkylineBNL();
            }

            return new SPSkylineBNLLevel();
        }
    }
}
