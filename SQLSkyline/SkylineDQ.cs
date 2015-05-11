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

        internal override DataTable GetSkylineTable(List<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            throw new NotImplementedException();
        }

        public override string GetStoredProcedureCommand(string strSQLReturn, string strWhere, string strOrderBy, int numberOfRecords, string strFirstSQL, string strOperators, int skylineUpToLevel, bool hasIncomparable, string strOrderByAttributes, string[] additionalParameters)
        {
            //usual sort clause
            strFirstSQL += strOrderBy;
            //Quote quotes because it is a parameter of the stored procedure
            strFirstSQL = strFirstSQL.Replace("'", "''");

            strSQLReturn = "EXEC dbo.SP_SkylineDQ '" + strFirstSQL + "', '" + strOperators + "'," + numberOfRecords;
            return strSQLReturn;
        }

        public override DataTable GetSkylineTable(String strConnection, String strQuery, String strOperators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            SP_SkylineDQ skyline = new SP_SkylineDQ();
            var dt = skyline.GetSkylineTable(strQuery, strOperators, numberOfRecords, strConnection, Provider);
            TimeMilliseconds = skyline.TimeInMs;
            return dt;
        }

    }
}
