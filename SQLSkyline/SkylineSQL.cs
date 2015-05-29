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

        public override void PrepareDatabaseForAlgorithm(ref IEnumerable<object[]> useDatabase, List<int> subspace, int[] preferenceColumnIndex, bool[] isPreferenceIncomparable)
        {
            throw new NotImplementedException();
        }


        public override string GetStoredProcedureCommand(string strWhere, string strOrderBy, string strFirstSQL, string strOperators, string strOrderByAttributes)
        {
            //string strWHERE = sqlCriterion.getCriterionClause(prefSQL, strSQLReturn);
            string strSQLReturn = strFirstSQL + strWhere;
            strSQLReturn += strOrderBy;
            return strSQLReturn;
        }
        public override DataTable GetSkylineTable(String querySQL, String preferenceOperators)
        {
            return null;

        }

        internal override DataTable GetSkylineTable(IEnumerable<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string preferenceOperators)
        {
            throw new NotImplementedException();
        }
      
    }
}
