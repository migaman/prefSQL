using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Microsoft.SqlServer.Server;

//------------------------------------------------------------------------------
// <copyright file="CSSqlClassFile.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------

namespace prefSQL.SQLSkyline
{
    /// <summary>
    /// 
    /// 
    /// </summary>
    /// <remarks>
    /// Profiling considersations:
    /// - Don't use getUpperBound inside a performanc critical method (i.e. IsTupleDominated) --> slows down performance
    /// </remarks>
    class Helper
    {
        //Only this parameters are different beteen SQL CLR function and Utility class
        public const string CnnStringSqlclr = "context connection=true";
        public const string ProviderClr = "System.Data.SqlClient";
        public const int MaxSize = 4000;

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

        /// <summary>
        /// Adds every output column to a new datatable and creates the structure to return data over MSSQL CLR pipes
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="operators"></param>
        /// <param name="dtSkyline"></param>
        /// <returns></returns>
        public static List<SqlMetaData> BuildRecordSchema(DataTable dt, string[] operators, DataTable dtSkyline)
        {
            List<SqlMetaData> outputColumns = new List<SqlMetaData>(dt.Columns.Count - (operators.Length));
            int iCol = 0;
            foreach (DataColumn col in dt.Columns)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol >= operators.Length)
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
        /// Compares a tuple against another tuple according to preference logic. Cannot handle incomparable values
        /// Better values are smaller!
        /// </summary>
        /// <returns></returns>
        public static bool IsTupleDominated(long[] windowTuple, long[] newTuple, int[] dimensions)
        {
            bool greaterThan = false;
            int nextComparisonIndex = 0;

            foreach (int iCol in dimensions)
            {
                //Profiling
                //Use explicit conversion (long)dataReader[iCol] instead of dataReader.GetInt64(iCol) is 20% faster!
                //Use long array instead of dataReader --> is 100% faster!!!
                //long value = dataReader.GetInt64(iCol);
                //long value = (long)dataReader[iCol];
                //long value = tupletoCheck[iCol].Value;
                long value = newTuple[nextComparisonIndex]; //.Value;

                int comparison = CompareValue(value, windowTuple[nextComparisonIndex]);

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

                nextComparisonIndex++;
            }


            //all equal and at least one must be greater than
            return greaterThan;

        }


        /// <summary>
        /// Same function as isTupleDominated, but values are interchanged
        /// 
        /// </summary>
        /// <param name="windowTuple"></param>
        /// <param name="newTuple"></param>
        /// <param name="dimensions"></param>
        /// <param name="operators"></param>
        /// <returns></returns>
        public static bool DoesTupleDominate(long[] windowTuple, long[] newTuple, int[] dimensions)
        {
            bool greaterThan = false;
            int nextComparisonIndex = 0;

            foreach (int iCol in dimensions)
            {
                //Use long array instead of dataReader --> is 100% faster!!!
                long value = newTuple[nextComparisonIndex];

                //interchange values for comparison
                int comparison = CompareValue(windowTuple[nextComparisonIndex], value);

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

                nextComparisonIndex++;
            }

            //all equal and at least one must be greater than
            //if (equalTo == true && greaterThan == true)
            return greaterThan;
        }


        /// <summary>
        /// Compares a tuple against another tuple according to preference logic. Can handle incomparable values
        /// Better values are smaller!
        /// </summary>
        /// <param name="dimensions"></param>
        /// <param name="operators"></param>
        /// <param name="windowTuple"></param>
        /// <param name="newTuple"></param>
        /// <param name="resultIncomparable"></param>
        /// <param name="newTupleAllValues"></param>
        /// <returns></returns>
        public static bool IsTupleDominated(long[] windowTuple, long[] newTuple, int[] dimensions, string[] operators, string[] resultIncomparable, object[] newTupleAllValues)
        {
            bool greaterThan = false;
            int nextComparisonIndex = 0;

            foreach (int iCol in dimensions)
            {
                string op = operators[iCol];
                //Compare only LOW attributes
                if (op.Equals("LOW"))
                {
                    long value = (long)newTuple[nextComparisonIndex];
                    long tmpValue = (long)windowTuple[nextComparisonIndex];
                    int comparison = CompareValue(value, tmpValue);

                    if (comparison >= 1)
                    {
                        if (comparison == 2)
                        {
                            //at least one must be greater than
                            greaterThan = true;
                        }
                        else
                        {
                            //It is the same long value
                            //Check if the value must be text compared
                            if (iCol + 1 < operators.Length && operators[iCol + 1].Equals("INCOMPARABLE"))
                            {
                                //string value is always the next field
                                string strValue = (string)newTupleAllValues[iCol + 1];
                                //If it is not the same string value, the values are incomparable!!
                                //If two values are comparable the strings will be empty!
                                if (strValue.Equals("INCOMPARABLE") || !strValue.Equals(resultIncomparable[nextComparisonIndex]))
                                {
                                    //Value is incomparable --> return false
                                    return false;
                                }
                            }
                        }
                    }
                    else
                    {
                        //Value is smaller --> return false
                        return false;
                    }

                    nextComparisonIndex++;
                }
            }


            //all equal and at least one must be greater than
            return greaterThan;

        }

        /// <summary>
        /// Same function as isTupleDominate, but values are interchanged
        /// </summary>
        /// <param name="dimensions"></param>
        /// <param name="operators"></param>
        /// <param name="stringResult"></param>
        /// <param name="windowTuple"></param>
        /// <param name="newTuple"></param>
        /// <param name="newTupleAllValues"></param>
        /// <returns></returns>
        public static bool DoesTupleDominate(long[] windowTuple, long[] newTuple, int[] dimensions, string[] operators, string[] stringResult, object[] newTupleAllValues)
        {
            bool greaterThan = false;
            int nextComparisonIndex = 0;

            foreach (int iCol in dimensions)
            {             
                string op = operators[iCol];
                //Compare only LOW attributes
                if (op.Equals("LOW"))
                {
                    long value = (long)newTuple[nextComparisonIndex];
                    long tmpValue = (long)windowTuple[nextComparisonIndex];

                    //interchange values for comparison
                    int comparison = CompareValue(tmpValue, value);

                    if (comparison >= 1)
                    {
                        if (comparison == 2)
                        {
                            //at least one must be greater than
                            greaterThan = true;
                        }
                        else
                        {
                            //It is the same long value
                            //Check if the value must be text compared
                            if (iCol + 1 < operators.Length && operators[iCol + 1].Equals("INCOMPARABLE"))
                            {
                                //string value is always the next field
                                string strValue = (string)newTupleAllValues[iCol + 1];
                                //If it is not the same string value, the values are incomparable!!
                                //If two values are comparable the strings will be empty!
                                if (!strValue.Equals(stringResult[nextComparisonIndex]))
                                {
                                    //Value is incomparable --> return false
                                    return false;
                                }
                            }
                        }
                    }
                    else
                    {
                        //Value is smaller --> return false
                        return false;
                    }

                    nextComparisonIndex++;
                }                
            }

            //all equal and at least one must be greater than
            //if (equalTo == true && greaterThan == true)
            return greaterThan;
        }

        /// <summary>
        /// Adds a tuple to the existing window. cannot handle incomparable values
        /// </summary>
        /// <param name="newTuple"></param>
        /// <param name="window"></param>
        /// <param name="dimensions"></param>
        /// <param name="operators"></param>
        /// <param name="dtResult"></param>
        public static void AddToWindow(object[] newTuple, List<long[]> window, int[] dimensions, string[] operators, DataTable dtResult)
        {
            long[] record = new long[operators.Count(op => op != "IGNORE" && op != "INCOMPARABLE")];
            int nextRecordIndex = 0;
            DataRow row = dtResult.NewRow();

            for (int iCol = 0; iCol < newTuple.Length; iCol++)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol < operators.Length)
                {
                    //IGNORE is used for sample skyline. Only attributes that are not ignored shold be tested
                    if (operators[iCol] == "IGNORE")
                    {
                        continue;
                    }

                    record[nextRecordIndex] = (long) newTuple[iCol];
                    nextRecordIndex++;
                }
                else
                {
                    row[iCol - operators.Length] = newTuple[iCol];
                }
            }


            //DataTable is for the returning values
            dtResult.Rows.Add(row);
            //Window contains the skyline values (for the algorithm)
            window.Add(record);

        }

        /// <summary>
        /// Adds a tuple to the existing window. Can handle incomparable values
        /// </summary>
        /// <param name="newTuple"></param>
        /// <param name="dimensions"></param>
        /// <param name="operators"></param>
        /// <param name="window"></param>
        /// <param name="resultstringCollection"></param>
        /// <param name="dtResult"></param>
        public static void AddToWindowIncomparable(object[] newTuple, List<long[]> window, int[] dimensions, string[] operators, ArrayList resultstringCollection, DataTable dtResult)
        {
            //long must be nullable (because of incomparable tupels)
            long[] recordInt = new long[operators.Count(op => op != "IGNORE" && op != "INCOMPARABLE")];
            string[] recordstring = new string[operators.Count(op => op != "IGNORE" && op != "INCOMPARABLE")];
            int nextRecordIndex = 0;
            DataRow row = dtResult.NewRow();

            for (int iCol = 0; iCol < newTuple.Length; iCol++)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol < operators.Length)
                {
                    //IGNORE is used for sample skyline. Only attributes that are not ignored shold be tested
                    if (operators[iCol] == "IGNORE")
                    {                       
                        continue;
                    }

                    string op = operators[iCol];

                    //LOW und HIGH Spalte in record abfüllen
                    if (op.Equals("LOW"))
                    {
                        recordInt[nextRecordIndex] = (long) newTuple[iCol];

                        //Check if long value is incomparable
                        if (iCol + 1 < operators.Length && operators[iCol + 1].Equals("INCOMPARABLE"))
                        {
                            //Incomparable field is always the next one
                            recordstring[nextRecordIndex] = (string) newTuple[iCol + 1];
                        }

                        nextRecordIndex++;
                    }                 
                }
                else
                {
                    row[iCol - operators.Length] = newTuple[iCol];
                }
            }         

            dtResult.Rows.Add(row);
            window.Add(recordInt);
            resultstringCollection.Add(recordstring);
        }

        /// <summary>
        /// Compares two values according to preference logic
        /// </summary>
        /// <param name="value1"></param>
        /// <param name="value2"></param>
        /// <returns>
        /// 0 = false
        /// 1 = equal
        /// 2 = greater than
        /// </returns>
        private static int CompareValue(long value1, long value2)
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

        public static List<object[]> GetObjectArrayFromDataTable(DataTable dataTable)
        {
            //Read all records only once. (SqlDataReader works forward only!!)
            List<DataRow> dataTableRowList = dataTable.Rows.Cast<DataRow>().ToList();
            //Write all attributes to a Object-Array
            //Profiling: This is much faster (factor 2) than working with the SQLReader
            return dataTableRowList.Select(dataRow => dataRow.ItemArray).ToList();
        }

        public static Dictionary<long, object[]> GetDictionaryFromDataTable(DataTable dataTable, int uniqueIdColumnIndex)
        {
            List<object[]> objectArrayFromDataTableOrig = GetObjectArrayFromDataTable(dataTable);
            return objectArrayFromDataTableOrig.ToDictionary(dataRow => Convert.ToInt64(dataRow[uniqueIdColumnIndex]));
        }

        public static DataTable GetDataTableFromSQL(string strQuery, string strConnection, string strProvider)
        {
            DbProviderFactory factory = DbProviderFactories.GetFactory(strProvider);
            DataTable dt = new DataTable();
            // use the factory object to create Data access objects.
            DbConnection connection = factory.CreateConnection(); // will return the connection object (i.e. SqlConnection ...)
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
                catch (Exception e)
                {
                    throw new Exception(e.Message);
                }
                finally
                {
                    connection.Close();
                }

                
            }
            return dt;
        }   

        //Sort BySum (for algorithms)
        public static DataTable SortBySum(DataTable dt, List<long[]> skylineValues)
        {
            //Add a column for each skyline attribute and a sort column
            long[] firstSkylineValues = skylineValues[0];
            int preferences = firstSkylineValues.GetUpperBound(0);

            for (int i = 0; i <= preferences; i++)
            {
                dt.Columns.Add("Skyline" + i, typeof(long));
            }
            dt.Columns.Add("SortOrder", typeof(int));

            //Add values to datatable
            for (int iRow = 0; iRow < dt.Rows.Count; iRow++)
            {
                long[] values = skylineValues[iRow];
                for (int i = 0; i <= preferences; i++)
                {
                    dt.Rows[iRow]["Skyline" + i] = values[i];
                }
            }
            dt = dt.DefaultView.ToTable();
            int preferenceStart = dt.Columns.Count - preferences - 2;

            //Now sort the table for each skyline table and calculate sortorder
            for (int iCol = preferenceStart; iCol < dt.Columns.Count - 1; iCol++)
            {
                //Sort by column and work with sorted table
                dt.DefaultView.Sort = dt.Columns[iCol].ColumnName + " ASC";
                dt = dt.DefaultView.ToTable();

                //Now replace values beginning from 0
                //int value = (int)dtResult.Rows[0][iCol];
                long rank = 0;
                long value = (long)dt.Rows[0][iCol];
                for (int iRow = 0; iRow < dt.Rows.Count; iRow++)
                {
                    if (value < (long)dt.Rows[iRow][iCol])
                    {
                        value = (long)dt.Rows[iRow][iCol];
                        rank++;
                    }
                    
                    if (dt.Rows[iRow]["SortOrder"] == DBNull.Value)
                    {
                        dt.Rows[iRow]["SortOrder"] = rank;
                    }
                    else
                    {
                        dt.Rows[iRow]["SortOrder"] = (int)dt.Rows[iRow]["SortOrder"] + rank;
                    }

                }
            }
            dt.DefaultView.Sort = "SortOrder ASC";
            dt = dt.DefaultView.ToTable();

            //Remove rows
            for (int i = 0; i <= preferences; i++)
            {
                dt.Columns.Remove("Skyline" + i);
            }
            dt.Columns.Remove("SortOrder");

            return dt;
        }


        //Sort ByRank (for algorithms)
        public static DataTable SortByRank(DataTable dt, List<long[]> skylineValues)
        {
            //Add a column for each skyline attribute and a sort column
            long[] firstSkylineValues = skylineValues[0];
            int preferences = firstSkylineValues.GetUpperBound(0);
            
            for (int i = 0; i <= preferences; i++)
            {
                dt.Columns.Add("Skyline" + i, typeof(long));
            }
            dt.Columns.Add("SortOrder", typeof(int));

            //Add values to datatable
            for(int iRow = 0; iRow < dt.Rows.Count; iRow++) {
                long[] values = skylineValues[iRow];
                for (int i = 0; i <= preferences; i++)
                {
                    dt.Rows[iRow]["Skyline" + i] = values[i];
                }
            }
            dt = dt.DefaultView.ToTable();
            int preferenceStart = dt.Columns.Count - preferences - 2;
            
            //Now sort the table for each skyline table and calculate sortorder
            for (int iCol = preferenceStart; iCol < dt.Columns.Count - 1; iCol++)
            {
                //Sort by column and work with sorted table
                dt.DefaultView.Sort = dt.Columns[iCol].ColumnName + " ASC";
                dt = dt.DefaultView.ToTable();

                //Now replace values beginning from 0
                //int value = (int)dtResult.Rows[0][iCol];
                long rank = 0;
                for (int iRow = 0; iRow < dt.Rows.Count; iRow++)
                {
                    rank++;
                    if (dt.Rows[iRow]["SortOrder"] == DBNull.Value || rank < (int)dt.Rows[iRow]["SortOrder"])
                    {
                        dt.Rows[iRow]["SortOrder"] = rank;
                    }

                }
            }
            dt.DefaultView.Sort = "SortOrder ASC";
            dt = dt.DefaultView.ToTable();

            //Remove rows
            for (int i = 0; i <= preferences; i++)
            {
                dt.Columns.Remove("Skyline" + i);
            }
            dt.Columns.Remove("SortOrder");

            return dt;
        }
    }
}
