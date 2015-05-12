using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
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
            List<SqlMetaData> outputColumns = new List<SqlMetaData>(dt.Columns.Count - (operators.GetUpperBound(0)+1));
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
        public static bool IsTupleDominated(long[] windowTuple, long[] newTuple, int dimensions)
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol < dimensions; iCol++)
            {
                //Profiling
                //Use explicit conversion (long)dataReader[iCol] instead of dataReader.GetInt64(iCol) is 20% faster!
                //Use long array instead of dataReader --> is 100% faster!!!
                //long value = dataReader.GetInt64(iCol);
                //long value = (long)dataReader[iCol];
                //long value = tupletoCheck[iCol].Value;
                long value = newTuple[iCol]; //.Value;

                int comparison = CompareValue(value, windowTuple[iCol]);

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
            if (greaterThan)
                return true;
            else
                return false; //werte sind genau gleich

        }


        /// <summary>
        /// Same function as isTupleDominated, but values are interchanged
        /// 
        /// </summary>
        /// <param name="dataReader"></param>
        /// <param name="operators"></param>
        /// <param name="result"></param>
        /// <param name="resultToTupleMapping"></param>
        /// <returns></returns>
        public static bool DoesTupleDominate(object[] dataReader, string[] operators, long[] result, int[] resultToTupleMapping, int dimensions)
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol <= dimensions; iCol++)
            {
                //Use long array instead of dataReader --> is 100% faster!!!
                long value = (long)dataReader[resultToTupleMapping[iCol]];

                //interchange values for comparison
                int comparison = CompareValue(result[iCol], value);

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
            if (greaterThan)
                return true;
            else
                return false;



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
        /// <returns></returns>
        public static bool IsTupleDominated(long[] windowTuple, long[] newTuple, int dimensions, string[] operators, string[] resultIncomparable)
        {
            /*bool greaterThan = false;

            for (int iCol = 0; iCol <= dimensions; iCol++)
            {
                string op = operators[iCol];
                //Compare only LOW attributes
                if (op.Equals("LOW"))
                {
                    long value = 0;
                    long tmpValue;
                    int comparison;

                    //check if value is incomparable
                    if (newTuple[iCol] == DBNull.Value)
                    //Profiling --> don't use dataReader
                    //if (dataReader.IsDBNull(iCol) == true)
                    {
                        //check if value is incomparable
                        if (windowTuple[iCol] == null)
                        {
                            //borh values are null --> compare text
                            //return false;
                            comparison = 1;
                        }

                        else
                        {
                            tmpValue = (long)windowTuple[iCol];
                            comparison = CompareValue(value, tmpValue);
                        }


                    }
                    else
                    {
                        //Profiling --> don't use dataReader
                        //value = (long)dataReader[iCol];
                        value = (long)newTuple[iCol];
                        //check if value is incomparable
                        if (windowTuple[iCol] == null)
                        {
                            return false;
                        }
                        else
                        {
                            tmpValue = (long)windowTuple[iCol];
                        }
                        comparison = CompareValue(value, tmpValue);
                    }

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
                            if (iCol + 1 <= dimensions && operators[iCol + 1].Equals("INCOMPARABLE"))
                            {
                                //string value is always the next field
                                //string strValue = (string)dataReader[iCol + 1];
                                string strValue = (string)newTuple[iCol + 1];
                                //If it is not the same string value, the values are incomparable!!
                                //If two values are comparable the strings will be empty!
                                if (strValue.Equals("INCOMPARABLE") || !strValue.Equals(resultIncomparable[iCol]))
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
                }
            }


            //all equal and at least one must be greater than
            //if (equalTo == true && greaterThan == true)
            if (greaterThan)
                return true;
            else*/
                return false;



        }

        /// <summary>
        /// Same function as isTupleDominate, but values are interchanged
        /// </summary>
        /// <param name="dataReader"></param>
        /// <param name="operators"></param>
        /// <param name="result"></param>
        /// <param name="stringResult"></param>
        /// <returns></returns>
        public static bool DoesTupleDominate(object[] dataReader, string[] operators, long?[] result, string[] stringResult, int dimensions)
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol <= dimensions; iCol++)
            {
                string op = operators[iCol];
                //Compare only LOW attributes
                if (op.Equals("LOW"))
                {
                    long value = 0; 
                    long tmpValue; 
                    int comparison; 


                    //check if value is incomparable
                    if (dataReader[iCol] == DBNull.Value)
                    {
                        //check if value is incomparable
                        if (result[iCol] == null)
                        {
                            //borh values are null --> compare text
                            //return false;
                            comparison = 1;
                        }

                        else
                        {
                            tmpValue = (long)result[iCol];
                            //Interchange values
                            comparison = CompareValue(value, tmpValue);
                        }


                    }
                    else
                    {
                        //
                        value = (long)dataReader[iCol];

                        //check if value is incomparable
                        if (result[iCol] == null)
                        {
                            return false;
                        }
                        else
                        {
                            tmpValue = (long)result[iCol];
                        }

                        
                        
                        //interchange values for comparison
                        comparison = CompareValue(tmpValue, value);
                    }



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
                            if (iCol + 1 <= dimensions && operators[iCol + 1].Equals("INCOMPARABLE"))
                            {
                                //string value is always the next field
                                //string strValue = (string)dataReader[iCol + 1];
                                string strValue = (string)dataReader[iCol + 1];
                                //If it is not the same string value, the values are incomparable!!
                                //If two values are comparable the strings will be empty!
                                if (!strValue.Equals(stringResult[iCol]))
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


                }
            }


            //all equal and at least one must be greater than
            //if (equalTo == true && greaterThan == true)
            if (greaterThan)
                return true;
            else
                return false;



        }


        /// <summary>
        /// Adds a tuple to the existing window. cannot handle incomparable values
        /// </summary>
        /// <param name="dataReader"></param>
        /// <param name="skylineDataReader"></param>
        /// <param name="resultCollection"></param>
        /// <param name="dimensions"></param>
        /// <param name="dtResult"></param>
        public static void AddToWindow(object[] dataReader, List<long[]> resultCollection, int dimensions, DataTable dtResult)
        {
            long[] record = new long[dimensions];
            DataRow row = dtResult.NewRow();

            for (int iCol = 0; iCol <= dataReader.GetUpperBound((0)); iCol++)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol <= dimensions-1)
                {
                    record[iCol] = (long)dataReader[iCol];
                }
                else
                {
                    row[iCol - dimensions] = dataReader[iCol];
                }
            }

            //DataTable is for the returning values
            dtResult.Rows.Add(row);
            //ResultCollection contains the skyline values (for the algorithm)
            resultCollection.Add(record);

        }

        /// <summary>
        /// Adds a tuple to the existing window. cannot handle incomparable values
        /// </summary>
        /// <param name="dataReader"></param>
        /// <param name="operators"></param>
        /// <param name="resultCollection"></param>
        /// <param name="record"></param>
        /// <param name="dtResult"></param>
        public static void AddToWindowSample(object[] dataReader, string[] operators, List<long[]> resultCollection, DataTable dtResult)
        {
            
            long[] recordInt = new long[operators.Count(op => op != "IGNORE")];
            int nextRecordIndex = 0;
            DataRow row = dtResult.NewRow();

            //for (int iCol = 0; iCol < dataReader.FieldCount; iCol++)
            for (int iCol = 0; iCol <= dataReader.GetUpperBound(0); iCol++)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol <= operators.GetUpperBound(0))
                {
                    //recordInt[iCol] = tupletoCheck[iCol].Value; // (long)dataReader[iCol];
                    if (operators[iCol] != "IGNORE")
                    {
                        recordInt[nextRecordIndex] = (long)dataReader[iCol];
                        nextRecordIndex++;
                    }
                }
                else
                {
                    row[iCol - operators.Length] = dataReader[iCol];
                }
            }



            dtResult.Rows.Add(row);
            resultCollection.Add(recordInt);

        }

        /// <summary>
        /// Adds a tuple to the existing window. Can handle incomparable values
        /// </summary>
        /// <param name="dataReader"></param>
        /// <param name="operators"></param>
        /// <param name="resultCollection"></param>
        /// <param name="resultstringCollection"></param>
        /// <param name="record"></param>
        /// <param name="dtResult"></param>
        public static void AddToWindow(object[] dataReader, string[] operators, List<long[]> resultCollection, ArrayList resultstringCollection, DataTable dtResult)
        {
            //long must be nullable (because of incomparable tupels)
            long?[] recordInt = new long?[operators.GetUpperBound(0) + 1];
            string[] recordstring = new string[operators.GetUpperBound(0) + 1];
            DataRow row = dtResult.NewRow();

            //for (int iCol = 0; iCol < dataReader.FieldCount; iCol++)
            for (int iCol = 0; iCol <= dataReader.GetUpperBound(0); iCol++)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol <= operators.GetUpperBound(0))
                {
                    //LOW und HIGH Spalte in record abf�llen
                    if (operators[iCol].Equals("LOW"))
                    {
                        //if (dataReader.IsDBNull(iCol) == true)
                        if (dataReader[iCol] == DBNull.Value)
                            recordInt[iCol] = null;
                        else
                        {
                            recordInt[iCol] = (long)dataReader[iCol];
                        }
                            
                            

                        //Check if long value is incomparable
                        if (iCol + 1 <= recordInt.GetUpperBound(0) && operators[iCol + 1].Equals("INCOMPARABLE"))
                        {
                            //Incomparable field is always the next one
                            recordstring[iCol] = (string)dataReader[iCol + 1];
                        }
                    }
                }
                else
                {
                    row[iCol - (operators.GetUpperBound(0) + 1)] = dataReader[iCol];
                }
            }

            dtResult.Rows.Add(row);
            //resultCollection.Add(recordInt);
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

        public static Dictionary<int, object[]> GetDictionaryFromDataTable(DataTable dataTable, int uniqueIdColumnIndex)
        {
            List<object[]> objectArrayFromDataTableOrig = GetObjectArrayFromDataTable(dataTable);
            return objectArrayFromDataTableOrig.ToDictionary(dataRow => (int)dataRow[uniqueIdColumnIndex]);
        }

        public static DataTable GetSkylineDataTable(string strQuery, string strConnection, string strProvider)
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


        //Sort BySum (for algorithms)
        public static DataTable SortBySum(DataTable dt, List<long[]> skylineValues)
        {
            //Add a column for each skyline attribute and a sort column
            long[] firstSkylineValues = (long[])skylineValues[0];
            int preferences = firstSkylineValues.GetUpperBound(0);

            for (int i = 0; i <= preferences; i++)
            {
                dt.Columns.Add("Skyline" + i, typeof(long));
            }
            dt.Columns.Add("SortOrder", typeof(int));

            //Add values to datatable
            for (int iRow = 0; iRow < dt.Rows.Count; iRow++)
            {
                long[] values = (long[])skylineValues[iRow];
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
            long[] firstSkylineValues = (long[])skylineValues[0];
            int preferences = firstSkylineValues.GetUpperBound(0);
            
            for (int i = 0; i <= preferences; i++)
            {
                dt.Columns.Add("Skyline" + i, typeof(long));
            }
            dt.Columns.Add("SortOrder", typeof(int));

            //Add values to datatable
            for(int iRow = 0; iRow < dt.Rows.Count; iRow++) {
                long[] values = (long[])skylineValues[iRow];
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
                    if (dt.Rows[iRow]["SortOrder"] == DBNull.Value || rank < (long)dt.Rows[iRow]["SortOrder"])
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
