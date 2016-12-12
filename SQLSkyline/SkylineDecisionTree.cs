﻿//------------------------------------------------------------------------------
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
    public class SkylineDecisionTree : SkylineStrategy
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


        public override string GetStoredProcedureCommand(string strWhere, string strOrderBy, string strFirstSQL, string strOperators, string strOrderByAttributes)
        {
            strFirstSQL += strOrderByAttributes;
            //Quote quotes because it is a parameter of the stored procedure
            strFirstSQL = strFirstSQL.Replace("'", "''");
            string strSQLReturn;
            if (HasIncomparablePreferences)
            {
                strSQLReturn = "EXEC dbo.prefSQL_SkylineBNLSort '" + strFirstSQL + " ', '" + strOperators + "', " + RecordAmountLimit + ", " + SortType;
            }
            else
            {
                strSQLReturn = "EXEC dbo.prefSQL_SkylineBNLSortLevel '" + strFirstSQL + " ', '" + strOperators + "', " + RecordAmountLimit + ", " + SortType;
            }
            return strSQLReturn;

        }

        public override DataTable GetSkylineTable(String querySQL, String preferenceOperators)
        {
            //Calculate cardinality
            long thresholdCardinality = 300;

            TemplateStrategy strategy;
            if (Cardinality <= thresholdCardinality)
            {
                strategy = new SPSkylineDQ();    
            }
            else 
            {
                if (HasIncomparablePreferences)
                {
                    strategy = new SPSkylineBNLSort();
                } 
                else
                {
                    strategy = new SPSkylineBNLSortLevel();
                }
            }

            strategy.WindowHandling = WindowHandling;
            DataTable dt = strategy.GetSkylineTable(querySQL, preferenceOperators, RecordAmountLimit, true, ConnectionString, Provider, AdditionParameters, SortType);
            TimeMilliseconds = strategy.TimeInMs;
            NumberOfComparisons = strategy.NumberOfOperations;
            NumberOfMoves = strategy.NumberOfMoves;
            return dt;
        }

        internal override DataTable GetSkylineTable(IEnumerable<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string preferenceOperators)
        {
            throw new NotImplementedException();
        }


    }
}
