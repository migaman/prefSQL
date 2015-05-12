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

        public override DataTable GetSkylineTable(String querySQL, String preferenceOperators)
        {
            //Calculate cardinality
            long thresholdCardinality = 1000;

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

            DataTable dt = strategy.GetSkylineTable(querySQL, preferenceOperators, RecordAmountLimit, true, ConnectionString, Provider, AdditionParameters, SortType);
            TimeMilliseconds = strategy.TimeInMs;
            return dt;
        }

        internal override DataTable GetSkylineTableBackdoorSample(List<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            throw new NotImplementedException();
        }


    }
}
