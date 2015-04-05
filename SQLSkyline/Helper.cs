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



        

        /// <summary>
        /// Compares a tuple against another tuple according to preference logic. Cannot handle incomparable values
        /// Better values are smaller!
        /// </summary>
        /// <param name="sqlReader"></param>
        /// <param name="operators"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public static bool isTupleDominated(DataTableReader sqlReader, long[] result)
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
            {
                long value = (long)sqlReader[iCol];

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
        /// <param name="sqlReader"></param>
        /// <param name="operators"></param>
        /// <param name="result"></param>
        /// <returns></returns>

        public static bool doesTupleDominate(DataTableReader sqlReader, string[] operators, long[] result)
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
            {
                long value = (long)sqlReader[iCol];

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
        /// <param name="sqlReader"></param>
        /// <param name="operators"></param>
        /// <param name="result"></param>
        /// <param name="stringResult"></param>
        /// <returns></returns>

        public static bool isTupleDominated(DataTableReader sqlReader, string[] operators, long?[] result, string[] stringResult)
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
                    if (sqlReader.IsDBNull(iCol) == true)
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
                        value = (long)sqlReader[iCol];
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
                                string strValue = (string)sqlReader[iCol + 1];
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
        /// <param name="sqlReader"></param>
        /// <param name="operators"></param>
        /// <param name="result"></param>
        /// <param name="stringResult"></param>
        /// <returns></returns>
        public static bool doesTupleDominate(DataTableReader sqlReader, string[] operators, long?[] result, string[] stringResult)
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
                    if (sqlReader.IsDBNull(iCol) == true)
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
                        value = (long)sqlReader[iCol];

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
                                string strValue = (string)sqlReader[iCol + 1];
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
        /// <param name="sqlReader"></param>
        /// <param name="operators"></param>
        /// <param name="resultCollection"></param>
        /// <param name="record"></param>
        /// <param name="isFrameworkMode"></param>
        /// <param name="dtResult"></param>
        public static void addToWindow(DataTableReader sqlReader, string[] operators, ArrayList resultCollection, SqlDataRecord record, DataTable dtResult)
        {
            
            //Erste Spalte ist die ID
            long[] recordInt = new long[operators.GetUpperBound(0) + 1];
            DataRow row = dtResult.NewRow();

            for (int iCol = 0; iCol < sqlReader.FieldCount; iCol++)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol <= operators.GetUpperBound(0))
                {
                    recordInt[iCol] = (long)sqlReader[iCol];
                }
                else
                {
                    row[iCol - (operators.GetUpperBound(0) + 1)] = sqlReader[iCol];
                    record.SetValue(iCol - (operators.GetUpperBound(0) + 1), sqlReader[iCol]);
                }
            }



            dtResult.Rows.Add(row);
            resultCollection.Add(recordInt);
            //resultCollection.Insert(resultCollection.Count, recordInt);

        }

        /// <summary>
        /// Adds a tuple to the existing window. Can handle incomparable values
        /// </summary>
        /// <param name="sqlReader"></param>
        /// <param name="operators"></param>
        /// <param name="resultCollection"></param>
        /// <param name="resultstringCollection"></param>
        /// <param name="record"></param>
        /// <param name="isFrameworkMode"></param>
        /// <param name="dtResult"></param>
        public static void addToWindow(DataTableReader sqlReader, string[] operators, ArrayList resultCollection, ArrayList resultstringCollection, SqlDataRecord record, DataTable dtResult)
        {
            //long must be nullable (because of incomparable tupels)
            long?[] recordInt = new long?[operators.GetUpperBound(0) + 1];
            string[] recordstring = new string[operators.GetUpperBound(0) + 1];
            DataRow row = dtResult.NewRow();

            for (int iCol = 0; iCol < sqlReader.FieldCount; iCol++)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol <= operators.GetUpperBound(0))
                {
                    //LOW und HIGH Spalte in record abfüllen
                    if (operators[iCol].Equals("LOW"))
                    {
                        if (sqlReader.IsDBNull(iCol) == true)
                            recordInt[iCol] = null;
                        else
                        {
                            recordInt[iCol] = (long)sqlReader[iCol];
                        }
                            
                            

                        //Check if long value is incomparable
                        if (iCol + 1 <= recordInt.GetUpperBound(0) && operators[iCol + 1].Equals("INCOMPARABLE"))
                        {
                            //Incomparable field is always the next one
                            recordstring[iCol] = (string)sqlReader[iCol + 1];
                        }
                    }
                }
                else
                {
                    row[iCol - (operators.GetUpperBound(0) + 1)] = sqlReader[iCol];
                    record.SetValue(iCol - (operators.GetUpperBound(0) + 1), sqlReader[iCol]);
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
    }
}
