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

        protected abstract DataTable getSkylineTable(List<object[]> listObjects, SqlDataRecord record, string strOperators, int numberOfRecords, bool isIndependent, DataTable dtResult);
    }
}