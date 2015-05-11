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

        internal override DataTable GetSkylineTable(List<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            throw new NotImplementedException();
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
        public override DataTable GetSkylineTable(String strConnection, String strQuery, String strOperators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            DataTable dt;
            if (hasIncomparable)
            {
                //Hexagon incomparable needs additional parameters
                String strHexagonSelectIncomparable = additionalParameters[3].Trim().Replace("''", "'").Trim('\'');
                int weightHexagonIncomparable = int.Parse(additionalParameters[4].Trim());
                SP_SkylineHexagon skyline = new SP_SkylineHexagon();
                dt = skyline.GetSkylineTable(strQuery, strOperators, numberOfRecords, strConnection, Provider, strHexagonSelectIncomparable, weightHexagonIncomparable);
                TimeMilliseconds = skyline.TimeInMs;
            }
            else
            {
                SP_SkylineHexagonLevel skyline = new SP_SkylineHexagonLevel();
                dt = skyline.GetSkylineTable(strQuery, strOperators, numberOfRecords, strConnection, Provider, "", 0);
                TimeMilliseconds = skyline.TimeInMs;
            }
            return dt;
        }

    }
}
