//------------------------------------------------------------------------------
// <copyright file="CSSqlClassFile.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;

namespace prefSQL.SQLSkyline
{
    public class SkylineDQ : SkylineStrategy
    {
        public override bool isNative()
        {
            return false;
        }

        public override bool supportImplicitPreference()
        {
            return false;
        }

        public override bool supportIncomparable()
        {
            return false;
        }

        public override string getStoredProcedureCommand(string strSQLReturn, string strWHERE, string strOrderBy, int numberOfRecords, string strFirstSQL, string strOperators, int SkylineUpToLevel, bool hasIncomparable, string strOrderByAttributes, string[] additionalParameters)
        {
            //usual sort clause
            strFirstSQL += strOrderBy;
            //Quote quotes because it is a parameter of the stored procedure
            strFirstSQL = strFirstSQL.Replace("'", "''");

            strSQLReturn = "EXEC dbo.SP_SkylineDQ '" + strFirstSQL + "', '" + strOperators + "'," + numberOfRecords;
            return strSQLReturn;
        }

        public override DataTable getSkylineTable(String strConnection, String strQuery, String strOperators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {

            DataTable dt = null;
            prefSQL.SQLSkyline.SP_SkylineDQ skyline = new SQLSkyline.SP_SkylineDQ();   
            dt = skyline.getSkylineTable(strQuery, strOperators, numberOfRecords, strConnection);
            return dt;

        }
    }
}
