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
    

    public abstract class SkylineStrategy
    {
        public DataTable UseDataTable { get; set; }
        public long timeMilliseconds;
        public long sizeBTG;
        public abstract DataTable getSkylineTable(String strConnection, String strQuery, String strOperators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters);

        public abstract String getStoredProcedureCommand(string strSQLReturn, string strWHERE, string strOrderBy, int numberOfRecords, string strFirstSQL, string strOperators, int SkylineUpToLevel, bool hasIncomparable, string strOrderByAttributes, string[] additionalParameters);

        public abstract bool isNative();

        //If the algorithm can hande implicit preferences like 'red' >> 'blau' without an OTHER statement
        public abstract bool supportImplicitPreference();
        
        //If the algorithm supports incomparable values
        public abstract bool supportIncomparable();
    }
}
