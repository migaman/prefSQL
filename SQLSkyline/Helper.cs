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
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Linq;

    class Helper
    {
        //Only this parameters are different beteen SQL CLR function and Utility class
        public const string cnnStringSQLCLR = "context connection=true";
        public const string ProviderCLR = "System.Data.SqlClient";
        public const int MaxSize = 4000;

        /// <summary>
        /// Returns the TOP n first tupels of a datatable
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="numberOfRecords"></param>
        /// <returns></returns>
        public static DataTable getAmountOfTuples(DataTable dt, int numberOfRecords)
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
        public static List<SqlMetaData> buildRecordSchema(DataTable dt, string[] operators, DataTable dtSkyline)
        {
            List<SqlMetaData> outputColumns = new List<SqlMetaData>(dt.Columns.Count - (operators.GetUpperBound(0)+1));
            int iCol = 0;
            foreach (DataColumn col in dt.Columns)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol > operators.GetUpperBound(0))
                {
                    SqlMetaData OutputColumn;
                    if (col.DataType.Equals(typeof(Int32)) || col.DataType.Equals(typeof(Int64)) || col.DataType.Equals(typeof(DateTime)))
                    {
                        OutputColumn = new SqlMetaData(col.ColumnName, TypeConverter.ToSqlDbType(col.DataType));
                    }
                    else
                    {
                        OutputColumn = new SqlMetaData(col.ColumnName, TypeConverter.ToSqlDbType(col.DataType), col.MaxLength);
                    }
                    outputColumns.Add(OutputColumn);
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
        public static SqlDataRecord buildDataRecord(DataTable dt, string[] operators, DataTable dtSkyline)
        {
            List<SqlMetaData> outputColumns = new List<SqlMetaData>(dt.Columns.Count - (operators.GetUpperBound(0) + 1));
            int iCol = 0;
            foreach (DataColumn col in dt.Columns)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol > operators.GetUpperBound(0))
                {
                    SqlMetaData OutputColumn;
                    if (col.DataType.Equals(typeof(Int32)) || col.DataType.Equals(typeof(Int64)) || col.DataType.Equals(typeof(DateTime)))
                    {
                        OutputColumn = new SqlMetaData(col.ColumnName, TypeConverter.ToSqlDbType(col.DataType));
                    }
                    else
                    {
                        OutputColumn = new SqlMetaData(col.ColumnName, TypeConverter.ToSqlDbType(col.DataType), col.MaxLength);
                    }
                    outputColumns.Add(OutputColumn);
                    dtSkyline.Columns.Add(col.ColumnName, col.DataType);
                }
                iCol++;
            }
            return new SqlDataRecord(outputColumns.ToArray());
        }


        

        /// <summary>
        /// Compares a tuple against another tuple according to preference logic. Cannot handle incomparable values
        /// Better values are smaller!
        /// </summary>
        /// <param name="dataReader"></param>
        /// <param name="operators"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public static bool isTupleDominated(long[] result, object[] tupletoCheck, int[] resultToTupleMapping)
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
            {
                //Profiling
                //Use explicit conversion (long)dataReader[iCol] instead of dataReader.GetInt64(iCol) is 20% faster!
                //Use long array instead of dataReader --> is 100% faster!!!
                //long value = dataReader.GetInt64(iCol);
                //long value = (long)dataReader[iCol];
                //long value = tupletoCheck[iCol].Value;
                long value = (long)tupletoCheck[resultToTupleMapping[iCol]]; //.Value;
                

                int comparison = compareValue(value, result[iCol]);

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
            if (greaterThan == true)
                return true;
            else
                return false;

        }


        /// <summary>
        /// Same function as isTupleDominated, but values are interchanged
        /// 
        /// </summary>
        /// <param name="dataReader"></param>
        /// <param name="operators"></param>
        /// <param name="result"></param>
        /// <returns></returns>

        public static bool doesTupleDominate(object[] dataReader, string[] operators, long[] result, int[] resultToTupleMapping)
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
            {
                //Use long array instead of dataReader --> is 100% faster!!!
                long value = (long)dataReader[resultToTupleMapping[iCol]];

                //interchange values for comparison
                int comparison = compareValue(result[iCol], value);

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


        /// <summary>
        /// Compares a tuple against another tuple according to preference logic. Can handle incomparable values
        /// Better values are smaller!
        /// </summary>
        /// <param name="dataReader"></param>
        /// <param name="operators"></param>
        /// <param name="result"></param>
        /// <param name="stringResult"></param>
        /// <returns></returns>

        public static bool isTupleDominated(string[] operators, long?[] result, string[] stringResult, object[] tupletoCheck)
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
            {
                string op = operators[iCol];
                //Compare only LOW attributes
                if (op.Equals("LOW"))
                {
                    long value = 0;
                    long tmpValue = 0;
                    int comparison = 0;

                    //check if value is incomparable
                    if (tupletoCheck[iCol] == DBNull.Value)
                    //Profiling --> don't use dataReader
                    //if (dataReader.IsDBNull(iCol) == true)
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
                            comparison = compareValue(value, tmpValue);
                        }


                    }
                    else
                    {
                        //Profiling --> don't use dataReader
                        //value = (long)dataReader[iCol];
                        value = (long)tupletoCheck[iCol];
                        //check if value is incomparable
                        if (result[iCol] == null)
                        {
                            return false;
                        }
                        else
                        {
                            tmpValue = (long)result[iCol];
                        }
                        comparison = compareValue(value, tmpValue);
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
                            if (iCol + 1 <= result.GetUpperBound(0) && operators[iCol + 1].Equals("INCOMPARABLE"))
                            {
                                //string value is always the next field
                                //string strValue = (string)dataReader[iCol + 1];
                                string strValue = (string)tupletoCheck[iCol + 1];
                                //If it is not the same string value, the values are incomparable!!
                                //If two values are comparable the strings will be empty!
                                if (strValue.Equals("INCOMPARABLE") || !strValue.Equals(stringResult[iCol]))
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
            if (greaterThan == true)
                return true;
            else
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
        public static bool doesTupleDominate(object[] dataReader, string[] operators, long?[] result, string[] stringResult)
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
            {
                string op = operators[iCol];
                //Compare only LOW attributes
                if (op.Equals("LOW"))
                {
                    long value = 0; 
                    long tmpValue = 0; 
                    int comparison = 0; 


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
                            comparison = compareValue(value, tmpValue);
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
                        comparison = compareValue(tmpValue, value);
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
                            if (iCol + 1 <= result.GetUpperBound(0) && operators[iCol + 1].Equals("INCOMPARABLE"))
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
            if (greaterThan == true)
                return true;
            else
                return false;



        }



        /// <summary>
        /// Adds a tuple to the existing window. cannot handle incomparable values
        /// </summary>
        /// <param name="dataReader"></param>
        /// <param name="operators"></param>
        /// <param name="resultCollection"></param>
        /// <param name="record"></param>
        /// <param name="isFrameworkMode"></param>
        /// <param name="dtResult"></param>
        public static void addToWindow(object[] dataReader, string[] operators, ArrayList resultCollection, SqlDataRecord record, DataTable dtResult)
        {
            
            long[] recordInt = new long[operators.Count(op => op != "IGNORE")];
            var nextRecordIndex = 0;
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
                    record.SetValue(iCol - operators.Length, dataReader[iCol]);
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
        /// <param name="isFrameworkMode"></param>
        /// <param name="dtResult"></param>
        public static void addToWindow(object[] dataReader, string[] operators, ArrayList resultCollection, ArrayList resultstringCollection, SqlDataRecord record, DataTable dtResult)
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
                    //LOW und HIGH Spalte in record abfüllen
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
                    record.SetValue(iCol - (operators.GetUpperBound(0) + 1), dataReader[iCol]);
                }
            }

            dtResult.Rows.Add(row);
            resultCollection.Add(recordInt);
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
        private static int compareValue(long value1, long value2)
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
            var dataTableRowList = dataTable.Rows.Cast<DataRow>().ToList();
            //Write all attributes to a Object-Array
            //Profiling: This is much faster (factor 2) than working with the SQLReader
            return dataTableRowList.Select(dataRow => dataRow.ItemArray).ToList();
        }

        public static DataTable GetSkylineDataTable(string strQuery, bool isIndependent, string strConnection)
        {
            SqlConnection connection = null;
            if (isIndependent == false)
                connection = new SqlConnection(cnnStringSQLCLR);
            else
                connection = new SqlConnection(strConnection);

            var dt = new DataTable();

            try
            {
                //Some checks
                if (strQuery.ToString().Length == MaxSize)
                {
                    throw new Exception("Query is too long. Maximum size is " + MaxSize);
                }
                connection.Open();

                var dap = new SqlDataAdapter(strQuery.ToString(), connection);

                dap.Fill(dt);
            }
            catch (Exception ex)
            {
                //Pack Errormessage in a SQL and return the result
                var strError = "Fehler in SP_SkylineBNL: ";
                strError += ex.Message;

                if (isIndependent == true)
                {
                    Debug.WriteLine(strError);
                }
                else
                {
                    SqlContext.Pipe.Send(strError);
                }
            }
            finally
            {
                connection.Close();
            }

            return dt;
        }

        internal static int[] ResultToTupleMapping(string[] operators)
        {
            int[] resultToTupleMapping = new int[operators.Count(op => op != "IGNORE")];
            var next = 0;
            for (var j = 0; j < operators.Length; j++)
            {
                if (operators[j] != "IGNORE")
                {
                    resultToTupleMapping[next] = j;
                    next++;
                }
            }
            return resultToTupleMapping;
        }
    }
}
