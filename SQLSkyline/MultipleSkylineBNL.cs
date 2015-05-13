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

        internal override DataTable GetSkylineTableBackdoorSample(List<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            throw new NotImplementedException();
        }


        public override string GetStoredProcedureCommand(string strWhere, string strOrderBy, string strFirstSQL, string strOperators, string strOrderByAttributes)
        {
            strFirstSQL += strOrderByAttributes;
            //Quote quotes because it is a parameter of the stored procedure
            strFirstSQL = strFirstSQL.Replace("'", "''");

            string strSQLReturn = "EXEC dbo.SP_MultipleSkylineBNL '" + strFirstSQL + "', '" + strOperators + "', " + RecordAmountLimit + ", " + MultipleSkylineUpToLevel;
            return strSQLReturn;
        }

        public override DataTable GetSkylineTable(String querySQL, String preferenceOperators)
        {
            //Additional parameter
            int upToLevel = int.Parse(AdditionParameters[3]);

            if (HasIncomparablePreferences)
            {
                SPMultipleSkylineBNL skyline = new SPMultipleSkylineBNL();
                return skyline.GetSkylineTable(querySQL, preferenceOperators, ConnectionString, Provider, RecordAmountLimit, upToLevel);
            }
            else
            {
                SPMultipleSkylineBNLLevel skyline = new SPMultipleSkylineBNLLevel();
                return skyline.GetSkylineTable(querySQL, preferenceOperators, ConnectionString, Provider, RecordAmountLimit, upToLevel);
            }
            
        }
    }
}
