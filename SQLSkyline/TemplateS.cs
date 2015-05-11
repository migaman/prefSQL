using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using Microsoft.SqlServer.Server;

//!!!Caution: Attention small changes in this code can lead to remarkable performance issues!!!!
namespace prefSQL.SQLSkyline
{
    /// <summary>
    /// TODO: Work in progress
    /// </summary>
    /// <remarks>
    /// TODO: Work in progress
    /// </remarks>
    public abstract class TemplateS : TemplateStrategy
    {

        protected override DataTable GetSkylineFromAlgorithm(List<object[]> database, DataTable dataTableTemplate,
            SqlDataRecord dataRecordTemplate, string[] operators, string[] additionalParameters)
        {
            throw new NotImplementedException();
        }

        protected abstract bool TupleDomination(ArrayList resultCollection, ArrayList resultstringCollection, string[] operators, DataTable dtResult, int i);

        protected abstract void AddtoWindow(object[] dataReader, string[] operators, ArrayList resultCollection, ArrayList resultstringCollection, SqlDataRecord record, bool isFrameworkMode, DataTable dtResult);

    }
}
