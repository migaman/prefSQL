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
    public class SkylineHexagon : SkylineStrategy
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
            return true;
        }

        public override void PrepareDatabaseForAlgorithm(ref IEnumerable<object[]> useDatabase, List<int> subset, int[] preferenceColumnIndex, bool[] isPreferenceIncomparable)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="strWhere"></param>
        /// <param name="strOrderBy"></param>
        /// <param name="strFirstSQL"></param>
        /// <param name="strOperators"></param>
        /// <param name="strOrderByAttributes"></param>
        /// <returns></returns>
        public override string GetStoredProcedureCommand(string strWhere, string strOrderBy, string strFirstSQL, string strOperators, string strOrderByAttributes)
        {
            strFirstSQL = AdditionParameters[0];
            strOperators = AdditionParameters[1];
            string strSelectDistinctIncomparable = AdditionParameters[2];
            string weightHexagonIncomparable = AdditionParameters[3];
            string strSQLReturn;
            if (HasIncomparablePreferences)
            {
                strSQLReturn = "EXEC dbo.prefSQL_SkylineHexagon '" + strFirstSQL + "', '" + strOperators + "', " + RecordAmountLimit + ", " + SortType + ", '" + strSelectDistinctIncomparable + "', '" + weightHexagonIncomparable + "'";
            }
            else
            {
                strSQLReturn = "EXEC dbo.prefSQL_SkylineHexagonLevel '" + strFirstSQL + "', '" + strOperators + "', " + RecordAmountLimit + ", " + SortType;
            }
            return strSQLReturn;
        }
        public override DataTable GetSkylineTable(String querySQL, String preferenceOperators)
        {
            DataTable dt;
            if (HasIncomparablePreferences)
            {
                //Hexagon incomparable needs additional parameters
                SPSkylineHexagon skyline = new SPSkylineHexagon();

                AdditionParameters[4] = AdditionParameters[4].Trim().Replace("''", "'").Trim('\'');
                //Change operators array and SQL query (replace INCOMPARABLES values)
                skyline.CalculateOperators(ref preferenceOperators, AdditionParameters, ConnectionString, Provider, ref querySQL);

                dt = skyline.GetSkylineTable(querySQL, preferenceOperators, RecordAmountLimit, true, ConnectionString, Provider, AdditionParameters, SortType);
                TimeMilliseconds = skyline.TimeInMs;
                NumberOfComparisons = skyline.NumberOfOperations;
                NumberOfMoves = skyline.NumberOfMoves;
            }
            else
            {
                SPSkylineHexagonLevel skyline = new SPSkylineHexagonLevel();
                dt = skyline.GetSkylineTable(querySQL, preferenceOperators, RecordAmountLimit, true, ConnectionString, Provider, AdditionParameters, SortType);
                TimeMilliseconds = skyline.TimeInMs;
                NumberOfComparisons = skyline.NumberOfOperations;
                NumberOfMoves = skyline.NumberOfMoves;
            }
            return dt;
        }

        internal override DataTable GetSkylineTable(IEnumerable<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string preferenceOperators)
        {
            throw new NotImplementedException();
        }




        

    }
}
