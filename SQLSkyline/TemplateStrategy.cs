using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.SqlServer.Server;

namespace prefSQL.SQLSkyline
{
    public abstract class TemplateStrategy
    {
        public long TimeInMs = 0;

        public DataTable GetSkylineTable(string strQuery, string strOperators, int numberOfRecords, string strConnection,
            string strProvider)
        {
            return GetSkylineTable(strQuery, strOperators, numberOfRecords, true, strConnection, strProvider);
        }

        protected abstract DataTable GetSkylineTable(String strQuery, String strOperators, int numberOfRecords, bool isIndependent, string strConnection, string strProvider);

        /// <summary>
        /// TODO: comment
        /// </summary>
        /// <param name="database"></param>
        /// <param name="dataTableTemplate"></param>
        /// <param name="dataRecordTemplate"></param>
        /// <param name="operators"></param>
        /// <param name="numberOfRecords"></param>
        /// <param name="isIndependent"></param>
        /// <returns></returns>
        protected abstract DataTable GetSkylineTable(List<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool isIndependent);
    }
}