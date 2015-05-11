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
    public class MultipleSkylineBNL : SkylineStrategy
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

        internal override DataTable GetSkylineTable(List<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            throw new NotImplementedException();
        }

        public override string GetStoredProcedureCommand(string strWhere, string strOrderBy, int numberOfRecords, string strFirstSQL, string strOperators, int skylineUpToLevel, bool hasIncomparable, string strOrderByAttributes, string[] additionalParameters)
        {
            strFirstSQL += strOrderByAttributes;
            //Quote quotes because it is a parameter of the stored procedure
            strFirstSQL = strFirstSQL.Replace("'", "''");

            string strSQLReturn = "EXEC dbo.SP_MultipleSkylineBNL '" + strFirstSQL + "', '" + strOperators + "', " + numberOfRecords + ", " + skylineUpToLevel;
            return strSQLReturn;
        }

        public override DataTable GetSkylineTable(String strConnection, String strQuery, String strOperators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            //Additional parameter
            int upToLevel = int.Parse(additionalParameters[3]);

            if (hasIncomparable)
            {
                SP_MultipleSkylineBNL skyline = new SP_MultipleSkylineBNL();
                return skyline.GetSkylineTable(strQuery, strOperators, strConnection, Provider, numberOfRecords, upToLevel);
            }
            else
            {
                SP_MultipleSkylineBNLLevel skyline = new SP_MultipleSkylineBNLLevel();
                return skyline.GetSkylineTable(strQuery, strOperators, strConnection, Provider, numberOfRecords, upToLevel);
            }
            
        }
    }
}
