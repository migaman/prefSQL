using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using Microsoft.SqlServer.Server;
using prefSQL.SQLSkyline;

namespace Utility
{

    class PerformanceTestHelper
    {
        public static long MaxSize = 4000;
        public static string ConnectionString = ConfigurationManager.ConnectionStrings["localhost"].ConnectionString;
        public static string ProviderName = ConfigurationManager.ConnectionStrings["localhost"].ProviderName;

        /// <summary>
        /// Returns the TOP n first tupels of a datatable
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="numberOfRecords"></param>
        /// <returns></returns>
        public static DataTable GetAmountOfTuples(DataTable dt, int numberOfRecords)
        {
            if (numberOfRecords > 0)
            {
                for (int i = dt.Rows.Count - 1; i >= numberOfRecords; i--)
                {
                    dt.Rows.RemoveAt(i);
                }

            }
            return dt;
        }

        internal static int[] ResultToTupleMapping(string[] operators)
        {
            int[] resultToTupleMapping = new int[operators.Count(op => op != "IGNORE")];
            int next = 0;
            for (int j = 0; j < operators.Length; j++)
            {
                if (operators[j] != "IGNORE")
                {
                    resultToTupleMapping[next] = j;
                    next++;
                }
            }
            return resultToTupleMapping;
        }


        public static DataTable GetSkylineDataTable(string strQuery, string strConnection, string strProvider)
        {
            DbProviderFactory factory = DbProviderFactories.GetFactory(strProvider);
            DataTable dt = new DataTable();

            // use the factory object to create Data access objects.
            DbConnection connection = factory.CreateConnection();
            if (connection != null)
            {
                connection.ConnectionString = strConnection;

                

                try
                {
                    //Some checks
                    if (strQuery.Length == MaxSize)
                    {
                        throw new Exception("Query is too long. Maximum size is " + MaxSize);
                    }
                    connection.Open();

                    DbDataAdapter dap = factory.CreateDataAdapter();
                    DbCommand selectCommand = connection.CreateCommand();
                    selectCommand.CommandTimeout = 0; //infinite timeout
                    selectCommand.CommandText = strQuery;
                    if (dap != null)
                    {
                        dap.SelectCommand = selectCommand;
                        dt = new DataTable();

                        dap.Fill(dt);
                    }
                }
                catch (Exception ex)
                {
                    //Pack Errormessage in a SQL and return the result
                    string strError = "Fehler in SP_SkylineBNL: ";
                    strError += ex.Message;

                    Debug.WriteLine(strError);
                    
                }
                finally
                {
                    connection.Close();
                }

                
            }
            return dt;
        }


        public static List<object[]> FillObjectFromDataReader(DataTableReader reader)
        {
            List<object[]> listObjects = new List<object[]>();
            while (reader.Read())
            {
                object[] recordObject = new object[reader.FieldCount];
                for (int iCol = 0; iCol < reader.FieldCount; iCol++)
                {
                    recordObject[iCol] = reader[iCol];
                }
                listObjects.Add(recordObject);
            }
            reader.Close();
            return listObjects;
        }

        /// <summary>
        /// Adds every output column to a new datatable and creates the structure to return data over MSSQL CLR pipes
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="operators"></param>
        /// <param name="dtSkyline"></param>
        /// <returns></returns>
        public static SqlDataRecord BuildDataRecord(DataTable dt, string[] operators, DataTable dtSkyline)
        {
            List<SqlMetaData> outputColumns = BuildRecordSchema(dt, operators, dtSkyline);
            return new SqlDataRecord(outputColumns.ToArray());



        }



        /// <summary>
        /// Adds every output column to a new datatable and creates the structure to return data over MSSQL CLR pipes
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="operators"></param>
        /// <param name="dtSkyline"></param>
        /// <returns></returns>
        public static List<SqlMetaData> BuildRecordSchema(DataTable dt, string[] operators, DataTable dtSkyline)
        {
            List<SqlMetaData> outputColumns = new List<SqlMetaData>(dt.Columns.Count - (operators.GetUpperBound(0) + 1));
            int iCol = 0;
            foreach (DataColumn col in dt.Columns)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol > operators.GetUpperBound(0))
                {
                    SqlMetaData outputColumn;
                    if (col.DataType == typeof(Int32) || col.DataType == typeof(Int64) || col.DataType == typeof(DateTime))
                    {
                        outputColumn = new SqlMetaData(col.ColumnName, TypeConverter.ToSqlDbType(col.DataType));
                    }
                    else
                    {
                        outputColumn = new SqlMetaData(col.ColumnName, TypeConverter.ToSqlDbType(col.DataType), col.MaxLength);
                    }
                    outputColumns.Add(outputColumn);
                    dtSkyline.Columns.Add(col.ColumnName, col.DataType);
                }
                iCol++;
            }
            return outputColumns;
        }


        public static DataTable ExecuteStatement(String strSQL)
        {
            DataTable dt = new DataTable();

            //Generic database provider
            //Create the provider factory from the namespace provider, you could create any other provider factory.. for Oracle, MySql, etc...
            DbProviderFactory factory = DbProviderFactories.GetFactory(Helper.ProviderName);

            // use the factory object to create Data access objects.
            DbConnection connection = factory.CreateConnection(); // will return the connection object, in this case, SqlConnection ...
            if (connection != null)
            {
                connection.ConnectionString = Helper.ConnectionString;

                connection.Open();
                DbCommand command = connection.CreateCommand();
                command.CommandTimeout = 0; //infinite timeout
                command.CommandText = strSQL;
                DbDataAdapter db = factory.CreateDataAdapter();
                if (db != null)
                {
                    db.SelectCommand = command;
                    db.Fill(dt);
                }
            }

            return dt;
        }
    }
}
