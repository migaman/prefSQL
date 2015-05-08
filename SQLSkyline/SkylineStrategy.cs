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


    public abstract class SkylineStrategy
    {
        public string Provider { get; set; }
        public long timeMilliseconds;
        public abstract DataTable getSkylineTable(String strConnection, String strQuery, String strOperators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters);
        
        /// <summary>
        /// TODO: comment
        /// </summary>
        /// <param name="database"></param>
        /// <param name="dataRecordTemplate"></param>
        /// <param name="operators"></param>
        /// <param name="numberOfRecords"></param>
        /// <param name="hasIncomparable"></param>
        /// <param name="additionalParameters"></param>
        /// <param name="dataTableTemplate"></param>
        /// <returns></returns>
        internal abstract DataTable getSkylineTable(List<object[]> database, SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters, DataTable dataTableTemplate);

        public abstract String getStoredProcedureCommand(string strSQLReturn, string strWHERE, string strOrderBy, int numberOfRecords, string strFirstSQL, string strOperators, int SkylineUpToLevel, bool hasIncomparable, string strOrderByAttributes, string[] additionalParameters);

        public abstract bool isNative();

        //If the algorithm can hande implicit preferences like 'red' >> 'blau' without an OTHER statement
        public abstract bool supportImplicitPreference();
        
        //If the algorithm supports incomparable values
        public abstract bool supportIncomparable();
    }
}
