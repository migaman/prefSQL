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
    public abstract class SkylineStrategy
    {
        public string Provider { get; set; }
        public long TimeMilliseconds;
        public abstract DataTable GetSkylineTable(String strConnection, String strQuery, String strOperators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters);

        /// <summary>
        /// TODO: comment
        /// </summary>
        /// <param name="database"></param>
        /// <param name="dataTableTemplate"></param>
        /// <param name="dataRecordTemplate"></param>
        /// <param name="operators"></param>
        /// <param name="numberOfRecords"></param>
        /// <param name="hasIncomparable"></param>
        /// <param name="additionalParameters"></param>
        /// <returns></returns>
        internal abstract DataTable GetSkylineTable(List<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters);

        public abstract String GetStoredProcedureCommand(string strWhere, string strOrderBy, int numberOfRecords, string strFirstSQL, string strOperators, int skylineUpToLevel, bool hasIncomparable, string strOrderByAttributes, string[] additionalParameters);

        public abstract bool IsNative();

        //If the algorithm can hande implicit preferences like 'red' >> 'blau' without an OTHER statement
        public abstract bool SupportImplicitPreference();
        
        //If the algorithm supports incomparable values
        public abstract bool SupportIncomparable();

        public long Cardinality;
    }
}
