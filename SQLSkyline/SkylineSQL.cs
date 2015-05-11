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
    public class SkylineSQL : SkylineStrategy
    {
        public override bool IsNative()
        {
            return true;
        }

        public override bool SupportImplicitPreference()
        {
            return true;
        }

        public override bool SupportIncomparable()
        {
            return true;
        }

        internal override DataTable GetSkylineTable(List<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            throw new NotImplementedException();
        }

        public override string GetStoredProcedureCommand(string strSQLReturn, string strWhere, string strOrderBy, int numberOfRecords, string strFirstSQL, string strOperators, int skylineUpToLevel, bool hasIncomparable, string strOrderByAttributes, string[] additionalParameters)
        {
            //string strWHERE = sqlCriterion.getCriterionClause(prefSQL, strSQLReturn);
            strSQLReturn += strWhere;
            strSQLReturn += strOrderBy;
            return strSQLReturn;
        }
        public override DataTable GetSkylineTable(String strConnection, String strQuery, String strOperators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            return null;

        }
      
    }
}
