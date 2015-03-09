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
    public class SkylineBNL : SkylineStrategy
    {
        public override bool isNative()
        {
            return false;
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
            if (hasIncomparable)
            {
                SP_SkylineBNL skyline = new SP_SkylineBNL();
                return skyline.getSkylineTable(strQuery, strOperators, numberOfRecords, strConnection);
            }
            else
            {
                SP_SkylineBNLLevel skyline = new SP_SkylineBNLLevel();
                return skyline.getSkylineTable(strQuery, strOperators, numberOfRecords, strConnection);
            }

        }
    }
}