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
    public class MultipleSkylineBNL : SkylineStrategy
    {
        public override bool isNative()
        {
            return false;
        }

        public override string getStoredProcedureCommand(string strSQLReturn, string strWHERE, string strOrderBy, int numberOfRecords, string strFirstSQL, string strOperators, int SkylineUpToLevel, bool hasIncomparable, string strOrderByAttributes, string[] additionalParameters)
        {
            strFirstSQL += strOrderByAttributes;
            //Quote quotes because it is a parameter of the stored procedure
            strFirstSQL = strFirstSQL.Replace("'", "''");

            strSQLReturn = "EXEC dbo.SP_MultipleSkylineBNL '" + strFirstSQL + "', '" + strOperators + "', " + numberOfRecords + ", " + SkylineUpToLevel;
            return strSQLReturn;
        }

        public override DataTable getSkylineTable(String strConnection, String strQuery, String strOperators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            //Additional parameter
            int upToLevel = int.Parse(additionalParameters[3]);

            if (hasIncomparable)
            {
                SP_MultipleSkylineBNL skyline = new SP_MultipleSkylineBNL();
                return skyline.getSkylineTable(strQuery, strOperators, strConnection, numberOfRecords, upToLevel);
            }
            else
            {
                SP_MultipleSkylineBNLLevel skyline = new SP_MultipleSkylineBNLLevel();
                return skyline.getSkylineTable(strQuery, strOperators, strConnection, numberOfRecords, upToLevel);
            }
            
        }
    }
}
