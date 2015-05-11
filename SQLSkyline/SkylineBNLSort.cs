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
    public class SkylineBNLSort : SkylineStrategy
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


        public override string GetStoredProcedureCommand(string strWhere, string strOrderBy, int numberOfRecords, string strFirstSQL, string strOperators, int skylineUpToLevel, bool hasIncomparable, string strOrderByAttributes, string[] additionalParameters)
        {
            strFirstSQL += strOrderByAttributes;
            //Quote quotes because it is a parameter of the stored procedure
            strFirstSQL = strFirstSQL.Replace("'", "''");
            string strSQLReturn;
            if (hasIncomparable)
            {
                strSQLReturn = "EXEC dbo.SP_SkylineBNLSort '" + strFirstSQL + "', '" + strOperators + "', " + numberOfRecords;
            }
            else
            {
                strSQLReturn = "EXEC dbo.SP_SkylineBNLSortLevel '" + strFirstSQL + "', '" + strOperators + "', " + numberOfRecords;
            }
            return strSQLReturn;
            
        }

        public override DataTable GetSkylineTable(string strConnection, string strQuery, string strOperators, int numberOfRecords,
            bool hasIncomparable, string[] additionalParameters)
        {
            TemplateBNL skyline = getSP_Skyline(hasIncomparable);
            DataTable dt = skyline.GetSkylineTable(strQuery, strOperators, numberOfRecords, true, strConnection, Provider, additionalParameters);
            TimeMilliseconds = skyline.TimeInMs;
            return dt;         
        }

        internal override DataTable GetSkylineTableBackdoorSample(List<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            throw new NotImplementedException();
        }

        private TemplateBNL getSP_Skyline(bool hasIncomparable)
        {
            if (hasIncomparable)
            {
                return new SPSkylineBNLSort();
            }

            return new SPSkylineBNLSortLevel();
        }
    }
}
