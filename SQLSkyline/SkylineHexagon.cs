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
using System.Threading.Tasks;

namespace prefSQL.SQLSkyline
{
    using Microsoft.SqlServer.Server;

    public class SkylineHexagon : SkylineStrategy
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
            return true;
        }
      
        internal override DataTable getSkylineTable(List<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            throw new NotImplementedException();
        }

        public override string getStoredProcedureCommand(string strSQLReturn, string strWHERE, string strOrderBy, int numberOfRecords, string strFirstSQL, string strOperators, int SkylineUpToLevel, bool hasIncomparable, string strOrderByAttributes, string[] additionalParameters)
        {

            strFirstSQL = additionalParameters[0];
            strOperators = additionalParameters[1];
            string strSelectDistinctIncomparable = additionalParameters[2];
            int weightHexagonIncomparable = int.Parse(additionalParameters[3]);

            if (hasIncomparable == true)
            {
                strSQLReturn = "EXEC dbo.SP_SkylineHexagon '" + strFirstSQL + "', '" + strOperators + "', " + numberOfRecords + ", '" + strSelectDistinctIncomparable + "'," + weightHexagonIncomparable;
            }
            else
            {
                strSQLReturn = "EXEC dbo.SP_SkylineHexagonLevel '" + strFirstSQL + "', '" + strOperators + "', " + numberOfRecords;
            }
            return strSQLReturn;
        }
        public override DataTable getSkylineTable(String strConnection, String strQuery, String strOperators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            DataTable dt = null;
            if (hasIncomparable == true)
            {
                //Hexagon incomparable needs additional parameters
                String strHexagonSelectIncomparable = additionalParameters[3].Trim().Replace("''", "'").Trim('\'');
                int weightHexagonIncomparable = int.Parse(additionalParameters[4].Trim());
                prefSQL.SQLSkyline.SP_SkylineHexagon skyline = new SQLSkyline.SP_SkylineHexagon();
                dt = skyline.getSkylineTable(strQuery, strOperators, numberOfRecords, strConnection, Provider, strHexagonSelectIncomparable, weightHexagonIncomparable);
                timeMilliseconds = skyline.timeInMs;
            }
            else
            {
                prefSQL.SQLSkyline.SP_SkylineHexagonLevel skyline = new SQLSkyline.SP_SkylineHexagonLevel();
                dt = skyline.getSkylineTable(strQuery, strOperators, numberOfRecords, strConnection, Provider, "", 0);
                timeMilliseconds = skyline.timeInMs;
            }
            return dt;
        }

    }
}
