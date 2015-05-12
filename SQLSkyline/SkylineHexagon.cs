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
    public class SkylineHexagon : SkylineStrategy
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
            return true;
        }


        public override string GetStoredProcedureCommand(string strWhere, string strOrderBy, int numberOfRecords, string strFirstSQL, string strOperators, int skylineUpToLevel, bool hasIncomparable, string strOrderByAttributes, string[] additionalParameters)
        {

            strFirstSQL = additionalParameters[0];
            strOperators = additionalParameters[1];
            string strSelectDistinctIncomparable = additionalParameters[2];
            int weightHexagonIncomparable = int.Parse(additionalParameters[3]);
            string strSQLReturn;
            if (hasIncomparable)
            {
                strSQLReturn = "EXEC dbo.SP_SkylineHexagon '" + strFirstSQL + "', '" + strOperators + "', " + numberOfRecords + ", '" + strSelectDistinctIncomparable + "'," + weightHexagonIncomparable;
            }
            else
            {
                strSQLReturn = "EXEC dbo.SP_SkylineHexagonLevel '" + strFirstSQL + "', '" + strOperators + "', " + numberOfRecords;
            }
            return strSQLReturn;
        }
        public override DataTable GetSkylineTable(String querySQL, String preferenceOperators)
        {
            DataTable dt;
            if (HasIncomparablePreferences)
            {
                //Hexagon incomparable needs additional parameters
                SPSkylineHexagon skyline = new SPSkylineHexagon();
                dt = skyline.GetSkylineTable(querySQL, preferenceOperators, RecordAmountLimit, true, ConnectionString, Provider, AdditionParameters, SortType);
                TimeMilliseconds = skyline.TimeInMs;
                NumberOfOperations = skyline.NumberOfOperations;
            }
            else
            {
                SPSkylineHexagonLevel skyline = new SPSkylineHexagonLevel();
                dt = skyline.GetSkylineTable(querySQL, preferenceOperators, RecordAmountLimit, true, ConnectionString, Provider, AdditionParameters, SortType);
                TimeMilliseconds = skyline.TimeInMs;
                NumberOfOperations = skyline.NumberOfOperations;
            }
            return dt;
        }

        internal override DataTable GetSkylineTableBackdoorSample(List<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            throw new NotImplementedException();
        }

    }
}
