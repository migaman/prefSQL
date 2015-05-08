using System;
using System.Data;
using Microsoft.SqlServer.Server;
using System.Collections;


namespace prefSQL.SQLSkyline
{
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Diagnostics;
    using System.Linq;

    public abstract class TemplateStrategy
    {
        public long timeInMs = 0;

        public DataTable getSkylineTable(string strQuery, string strOperators, int numberOfRecords, string strConnection,
            string strProvider)
        {
            return getSkylineTable(strQuery, strOperators, numberOfRecords, true, strConnection, strProvider);
        }

        protected abstract DataTable getSkylineTable(String strQuery, String strOperators, int numberOfRecords, bool isIndependent, string strConnection, string strProvider);

        /// <summary>
        /// TODO: comment
        /// </summary>
        /// <param name="database"></param>
        /// <param name="dataRecordTemplate"></param>
        /// <param name="operators"></param>
        /// <param name="numberOfRecords"></param>
        /// <param name="isIndependent"></param>
        /// <param name="dataTableTemplate"></param>
        /// <returns></returns>
        protected abstract DataTable getSkylineTable(List<object[]> database, SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool isIndependent, DataTable dataTableTemplate);
    }
}