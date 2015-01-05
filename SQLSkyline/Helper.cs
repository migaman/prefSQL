using Microsoft.SqlServer.Server;
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
    class Helper
    {
        //Only this parameters are different beteen SQL CLR function and Utility class
        public const string cnnStringSQLCLR = "context connection=true";
        public const string cnnStringLocalhost = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
        public const int MaxSize = 4000;

        public static List<SqlMetaData> buildRecordSchema(DataTable dt, string[] operators)
        {
            List<SqlMetaData> outputColumns = new List<SqlMetaData>(dt.Columns.Count);
            int iCol = 0;
            foreach (DataColumn col in dt.Columns)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol > operators.GetUpperBound(0))
                {
                    SqlMetaData OutputColumn;
                    if (col.DataType.Equals(typeof(Int32)) || col.DataType.Equals(typeof(DateTime)))
                    {
                        OutputColumn = new SqlMetaData(col.ColumnName, prefSQL.SQLSkyline.TypeConverter.ToSqlDbType(col.DataType));
                    }
                    else
                    {
                        OutputColumn = new SqlMetaData(col.ColumnName, prefSQL.SQLSkyline.TypeConverter.ToSqlDbType(col.DataType), col.MaxLength);
                    }
                    outputColumns.Add(OutputColumn);
                }
                iCol++;
            }
            return outputColumns;
        }



        /*
         * 0 = false
         * 1 = equal
         * 2 = greater than
         * */
        public static int compareValue(long value1, long value2)
        {

            if (value1 >= value2)
            {
                if (value1 > value2)
                    return 2;
                else
                    return 1;

            }
            else
            {
                return 0;
            }

        }
    }
}
