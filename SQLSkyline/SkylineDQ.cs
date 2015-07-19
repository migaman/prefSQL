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
    public class SkylineDQ : SkylineStrategy
    {
        public override bool IsNative()
        {
            return false;
        }

        public override bool SupportImplicitPreference()
        {
            return false;
        }

        public override bool SupportIncomparable()
        {
            return false;
        }

        public override void PrepareDatabaseForAlgorithm(ref IEnumerable<object[]> useDatabase, List<int> subspace, int[] preferenceColumnIndex, bool[] isPreferenceIncomparable)
        {
            throw new NotImplementedException();
        }

        public override string GetStoredProcedureCommand(string strWhere, string strOrderBy, string strFirstSQL, string strOperators, string strOrderByAttributes)
        {
            //usual sort clause
            strFirstSQL += strOrderBy;
            //Quote quotes because it is a parameter of the stored procedure
            strFirstSQL = strFirstSQL.Replace("'", "''");

            string strSQLReturn = "EXEC dbo.SP_SkylineDQ '" + strFirstSQL + "', '" + strOperators + "', " + RecordAmountLimit + ", " + SortType;
            return strSQLReturn;
        }

        public override DataTable GetSkylineTable(String querySQL, String preferenceOperators)
        {
            SPSkylineDQ skyline = new SPSkylineDQ();
            DataTable dt = skyline.GetSkylineTable(querySQL, preferenceOperators, RecordAmountLimit, true, ConnectionString, Provider, AdditionParameters, SortType);
            TimeMilliseconds = skyline.TimeInMs;
            NumberOfComparisons = skyline.NumberOfOperations;
            NumberOfMoves = skyline.NumberOfMoves;
            return dt;
        }

        internal override DataTable GetSkylineTable(IEnumerable<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string preferenceOperators)
        {
            throw new NotImplementedException();
        }

    }
}
