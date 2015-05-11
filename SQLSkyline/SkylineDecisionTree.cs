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
    using Microsoft.SqlServer.Server;

    public class SkylineDecisionTree : SkylineStrategy
    {

        internal override DataTable getSkylineTable(List<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            throw new NotImplementedException();
        }

        public override bool isNative()
        {
            return false;
        }

        public override bool supportImplicitPreference()
        {
            return true;
        }

        public override bool supportIncomparable()
        {
            return true;
        }



        public override string getStoredProcedureCommand(string strSQLReturn, string strWHERE, string strOrderBy, int numberOfRecords, string strFirstSQL, string strOperators, int SkylineUpToLevel, bool hasIncomparable, string strOrderByAttributes, string[] additionalParameters)
        {
            strFirstSQL += strOrderByAttributes;
            //Quote quotes because it is a parameter of the stored procedure
            strFirstSQL = strFirstSQL.Replace("'", "''");

            if (hasIncomparable == true)
            {
                strSQLReturn = "EXEC dbo.SP_SkylineBNLSort '" + strFirstSQL + "', '" + strOperators + "', " + numberOfRecords;
            }
            else
            {
                strSQLReturn = "EXEC dbo.SP_SkylineBNLSortLevel '" + strFirstSQL + "', '" + strOperators + "', " + numberOfRecords;
            }
            return strSQLReturn;

        }

        public override DataTable getSkylineTable(String strConnection, String strQuery, String strOperators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            //Calculate cardinality
            long thresholdCardinality = 1000;

            TemplateStrategy strategy = null;
            if (cardinality <= thresholdCardinality)
            {
                strategy = new SP_SkylineDQ();
            }
            else 
            {
                strategy = new SP_SkylineBNLSort();
            }

            //var skyline = getSP_Skyline(hasIncomparable);
            DataTable dt = strategy.getSkylineTable(strQuery, strOperators, numberOfRecords, strConnection, Provider);
            timeMilliseconds = strategy.timeInMs;
            return dt;
        }



        private TemplateBNL getSP_Skyline(bool hasIncomparable)
        {
            if (hasIncomparable)
            {
                return new SP_SkylineBNLSort();
            }

            return new SP_SkylineBNLSortLevel();
        }
    }
}
