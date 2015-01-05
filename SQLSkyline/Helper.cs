using Microsoft.SqlServer.Server;
//------------------------------------------------------------------------------
// <copyright file="CSSqlClassFile.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace prefSQL.SQLSkyline
{
    class Helper
    {
        //Only this parameters are different beteen SQL CLR function and Utility class
        public const string cnnStringSQLCLR = "context connection=true";
        //TODO: cnnLocalhost wird es nicht mehr geben sobald alles konvertiert
        public const string cnnStringLocalhost = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
        public const int MaxSize = 4000;

        public static List<SqlMetaData> buildRecordSchema(DataTable dt, string[] operators)
        {
            DataTable dtSkylinetest = new DataTable();
            return buildRecordSchema(dt, operators, ref dtSkylinetest);
        }

        public static List<SqlMetaData> buildRecordSchema(DataTable dt, string[] operators, ref DataTable dtSkyline)
        {
            List<SqlMetaData> outputColumns = new List<SqlMetaData>(dt.Columns.Count - (operators.GetUpperBound(0)+1));
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
                    dtSkyline.Columns.Add(col.ColumnName, col.DataType);
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


        public static bool compare(DataTableReader sqlReader, string[] operators, long[] result, string[] stringResult)
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
            {
                long value = sqlReader.GetInt32(iCol);
                int comparison = Helper.compareValue(value, result[iCol]);

                if (comparison >= 1)
                {
                    if (comparison == 2)
                    {
                        //at least one must be greater than
                        greaterThan = true;
                    }
                }
                else
                {
                    //Value is smaller --> return false
                    return false;
                }
            }


            //all equal and at least one must be greater than
            //if (equalTo == true && greaterThan == true)
            if (greaterThan == true)
                return true;
            else
                return false;



        }


        public static void addToWindow(DataTableReader sqlReader, string[] operators, ref ArrayList resultCollection, ref ArrayList resultstringCollection, SqlDataRecord record, bool isDebug, ref DataTable dtResult)
        {
            //Erste Spalte ist die ID
            long[] recordInt = new long[operators.GetUpperBound(0) + 1];
            string[] recordstring = new string[operators.GetUpperBound(0) + 1];
            DataRow row = dtResult.NewRow();

            for (int iCol = 0; iCol < sqlReader.FieldCount; iCol++)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol <= operators.GetUpperBound(0))
                {
                    recordInt[iCol] = sqlReader.GetInt32(iCol);
                }
                else
                {
                    row[iCol - (operators.GetUpperBound(0) + 1)] = sqlReader[iCol];
                    record.SetValue(iCol - (operators.GetUpperBound(0) + 1), sqlReader[iCol]);
                }
            }

            if (isDebug == false)
            {
                SqlContext.Pipe.SendResultsRow(record);
            }
            else
            {
                dtResult.Rows.Add(row);
            }
            resultCollection.Add(recordInt);
            resultstringCollection.Add(recordstring);
        }
    }
}
