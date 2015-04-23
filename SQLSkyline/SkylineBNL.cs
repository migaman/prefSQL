//------------------------------------------------------------------------------
// <copyright file="CSSqlClassFile.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;


namespace prefSQL.SQLSkyline
{
    using Microsoft.SqlServer.Server;

    public class SkylineBNL : SkylineStrategy
    {
        public override bool isNative()
        {
            return false;
        }

        public override bool supportImplicitPreference()
        {
            return true;
        }

        public override bool supportIncomparable()
        {
            return true;
        }

        public override string getStoredProcedureCommand(string strSQLReturn, string strWHERE, string strOrderBy, int numberOfRecords, string strFirstSQL, string strOperators, int SkylineUpToLevel, bool hasIncomparable, string strOrderByAttributes, string[] additionalParameters)
        {
            //usual sort clause
            strFirstSQL += strOrderBy;
            //Quote quotes because it is a parameter of the stored procedure
            strFirstSQL = strFirstSQL.Replace("'", "''");

            if (hasIncomparable == true)
            {
                strSQLReturn = "EXEC dbo.SP_SkylineBNL '" + strFirstSQL + "', '" + strOperators + "', " + numberOfRecords;
            }
            else
            {
                strSQLReturn = "EXEC dbo.SP_SkylineBNLLevel '" + strFirstSQL + "', '" + strOperators + "', " + numberOfRecords;
            }
            return strSQLReturn;
        }

        public override DataTable getSkylineTable(String strConnection, String strQuery, String strOperators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            var skyline = getSP_Skyline(hasIncomparable);
            DataTable dt = skyline.getSkylineTable(strQuery, strOperators, numberOfRecords, strConnection);
            timeMilliseconds = skyline.timeInMs;
            return dt;         
        }

        internal override DataTable getSkylineTable(List<object[]> dataTable, SqlDataRecord record, string strOperators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters, DataTable dtResult)
        {
            var skyline = getSP_Skyline(hasIncomparable);
            var dt = skyline.getSkylineTable(dataTable, record, strOperators, numberOfRecords, dtResult);
            timeMilliseconds = skyline.timeInMs;
            return dt;
        }     

        private TemplateBNL getSP_Skyline(bool hasIncomparable)
        {
            if (hasIncomparable)
            {
                return new SP_SkylineBNL();
            }

            return new SP_SkylineBNLLevel();
        }
    }
}
