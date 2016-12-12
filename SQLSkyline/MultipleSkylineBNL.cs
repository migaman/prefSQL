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

        public override void PrepareDatabaseForAlgorithm(ref IEnumerable<object[]> useDatabase, List<int> subset, int[] preferenceColumnIndex, bool[] isPreferenceIncomparable)
        {
            throw new NotImplementedException();
        }

        internal override DataTable GetSkylineTable(IEnumerable<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string preferenceOperators)
        {
            throw new NotImplementedException();
        }


        public override string GetStoredProcedureCommand(string strWhere, string strOrderBy, string strFirstSQL, string strOperators, string strOrderByAttributes)
        {
            strFirstSQL += strOrderByAttributes;
            //Quote quotes because it is a parameter of the stored procedure
            strFirstSQL = strFirstSQL.Replace("'", "''");

            string strSQLReturn;
            if (HasIncomparablePreferences)
            {
                strSQLReturn = "EXEC dbo.prefSQL_MultipleSkylineBNL '" + strFirstSQL + " ', '" + strOperators + "', " + RecordAmountLimit + ", " + SortType + ", " + MultipleSkylineUpToLevel;
            } else
            {

                strSQLReturn = "EXEC dbo.prefSQL_MultipleSkylineBNLLevel '" + strFirstSQL + " ', '" + strOperators + "', " + RecordAmountLimit + ", " + SortType + ", " + MultipleSkylineUpToLevel;
            }
            return strSQLReturn;
        }



        public override DataTable GetSkylineTable(String querySQL, String preferenceOperators)
        {
            //Additional parameter
            DataTable dt = new DataTable();
            int upToLevel = int.Parse(AdditionParameters[4]);

            if (HasIncomparablePreferences)
            {
                SPMultipleSkylineBNL skyline = new SPMultipleSkylineBNL();
                dt = skyline.GetSkylineTable(querySQL, preferenceOperators, ConnectionString, Provider, RecordAmountLimit, SortType, upToLevel);
            }
            else
            {
                SPMultipleSkylineBNLLevel skyline = new SPMultipleSkylineBNLLevel();
                dt = skyline.GetSkylineTable(querySQL, preferenceOperators, RecordAmountLimit, true, ConnectionString, Provider, AdditionParameters, SortType);

                

                TimeMilliseconds = skyline.TimeInMs;
                NumberOfComparisons = skyline.NumberOfOperations;
                NumberOfMoves = skyline.NumberOfMoves;
            }

            return dt;
            
        }
    }
}
