using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;
using System.Collections.Generic;

//Hinweis: Wenn mit startswith statt equals gearbeitet wird führt dies zu massiven performance problemen, z.B. large dataset 30 statt 3 Sekunden mit 13 Dimensionen!!
//WICHTIG: Vergleiche immer mit equals und nie mit z.B. startsWith oder Contains oder so.... --> Enorme Performance Unterschiede
namespace Utility
{
    class SkylineBNL
    {
        //Only this parameters are different beteen SQL CLR function and Utility class
        private const string connectionstring = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
        private const int MaxSize = 4000;
        private const string TempTable = "##MySkylineTable";
        private const int MaxVarcharSize = 100;

        
        [Microsoft.SqlServer.Server.SqlProcedure]
        public static void SP_SkylineBNL(SqlString strDimensions, SqlString strOperators, SqlString strQuery)
        {
            ArrayList resultCollection = new ArrayList();
            ArrayList resultstringCollection = new ArrayList();
            string[] operators = strOperators.ToString().Split(';');

            SqlConnection connection = new SqlConnection(connectionstring);
            try
            {
                //Some checks
                if (strDimensions.ToString().Length == MaxSize)
                {
                    throw new Exception("Query is too long. Maximum size is " + MaxSize);
                }


                connection.Open();


                SqlDataAdapter dap = new SqlDataAdapter(strQuery.ToString(), connection);
                DataTable dt = new DataTable(TempTable);
                dap.Fill(dt);
                string sqlsc = createTABLEStructure(dt);
                SqlCommand sqlCommand = new SqlCommand(sqlsc, connection);
                sqlCommand.ExecuteNonQuery();

                //Clones the structure of the DataTable, including all DataTable schemas and constraints.
                DataTable dtInsert = dt.Clone();


                sqlCommand = new SqlCommand(strDimensions.ToString(), connection);
                SqlDataReader sqlReader = sqlCommand.ExecuteReader();

            
                int iIndex = 0;
                //Read all records only once. (SqlDataReader works forward only!!)
                while (sqlReader.Read())
                {
                    //Check if window list is empty
                    if (resultCollection.Count == 0)
                    {
                        dtInsert.ImportRow(dt.Rows[iIndex]);                        
                        addToWindow(sqlReader, operators, ref resultCollection, ref resultstringCollection);
                    }
                    else
                    {
                        bool bDominated = false;

                        //check if record is dominated (compare against the records in the window)
                        for (int i = resultCollection.Count - 1; i >= 0; i--)
                        {
                            long[] result = (long[])resultCollection[i];
                            string[] strResult = (string[])resultstringCollection[i];

                            //Dominanz
                            if (compare(sqlReader, operators, result, strResult) == true)
                            {
                                //New point is dominated. No further testing necessary
                                bDominated = true;
                                break;
                            }


                            //Now, check if the new point dominates the one in the window
                            //--> It is not possible that the new point dominates the one in the window --> Raason data is ORDERED
                        }
                        if (bDominated == false)
                        {
                            dtInsert.ImportRow(dt.Rows[iIndex]);
                            addToWindow(sqlReader, operators, ref resultCollection, ref resultstringCollection);


                        }

                    }
                    iIndex++;
                }

                sqlReader.Close();


                //Bulk load into sql server
                dtInsert.AcceptChanges();


                System.Diagnostics.Debug.WriteLine(dtInsert.Rows.Count);
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = TempTable;
                    bulkCopy.WriteToServer(dtInsert);
                }


                string strSQL = "SELECT * FROM " + TempTable;

                sqlCommand = new SqlCommand(strSQL, connection);
                sqlReader = sqlCommand.ExecuteReader();
                

                //SqlDataReader reader = dtInsert.CreateDataReader();

                //SqlContext.Pipe.Send(sqlReader);

            }
            catch (Exception ex)
            {
                //Pack Errormessage in a SQL and return the result
                string strError = "SELECT 'Fehler in SP_SkylineBNL: ";
                strError += ex.Message.Replace("'", "''");
                strError += "'";

                //SqlContext.Pipe.Send(strError);

            }
            finally
            {
                if (connection != null)
                    connection.Close();
            }

        }


        private static void addToWindow(SqlDataReader sqlReader, string[] operators, ref ArrayList resultCollection, ref ArrayList resultstringCollection)
        {

            //Erste Spalte ist die ID
            long[] record = new long[sqlReader.FieldCount];
            string[] recordstring = new string[sqlReader.FieldCount];
            for (int i = 0; i <= record.GetUpperBound(0); i++)
            {
                //LOW und HIGH Spalte in record abfüllen
                if (operators[i].Equals("LOW"))
                {
                    Type type = sqlReader.GetFieldType(i);
                    if (type == typeof(int))
                    {
                        record[i] = sqlReader.GetInt32(i);
                    }
                    else if (type == typeof(DateTime))
                    {
                        //record[i] = sqlReader.GetDateTime(i).Ticks; 
                        record[i] = sqlReader.GetDateTime(i).Year * 10000 + sqlReader.GetDateTime(i).Month * 100 + sqlReader.GetDateTime(i).Day;
                    }

                    //Check if long value is incomparable
                    if (i+1 <= record.GetUpperBound(0) && operators[i+1].Equals("INCOMPARABLE"))
                    {
                        //Incomparable field is always the next one
                        type = sqlReader.GetFieldType(i+1);
                        if (type == typeof(string))
                        {
                            recordstring[i] = sqlReader.GetString(i + 1);
                        }

                    }




                }
               
            }
            resultCollection.Add(record);
            resultstringCollection.Add(recordstring);
        }


        private static bool compare(SqlDataReader sqlReader, string[] operators, long[] result, string[] stringResult) 
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
            {
                string op = operators[iCol];
                //Compare only LOW attributes
                if (op.Equals("LOW"))
                {
                    //Convert value if it is a date
                    long value = 0;
                    Type type = sqlReader.GetFieldType(iCol);
                    if (type == typeof(int))
                    {
                        value = sqlReader.GetInt32(iCol);
                    }
                    else if (type == typeof(DateTime))
                    {
                        //value = sqlReader.GetDateTime(iCol).Ticks;
                        value = sqlReader.GetDateTime(iCol).Year * 10000 + sqlReader.GetDateTime(iCol).Month * 100 + sqlReader.GetDateTime(iCol).Day;
                    }

                    int comparison = compareValue(value, result[iCol]);

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
                            if(iCol+1 <= result.GetUpperBound(0) && operators[iCol+1].Equals("INCOMPARABLE"))
                            {
                                //string value is always the next field
                                string strValue = sqlReader.GetString(iCol + 1);
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
        
        

        /*
         * 0 = false
         * 1 = equal
         * 2 = greater than
         * */
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


        private static string createTABLEStructure(DataTable table)
        {
            string sqlsc;

            sqlsc = "CREATE TABLE " + TempTable + "(";
            for (int i = 0; i < table.Columns.Count; i++)
            {
                sqlsc += "\n [" + table.Columns[i].ColumnName + "] ";
                if (table.Columns[i].DataType.ToString().Contains("System.Int32"))
                    sqlsc += " int ";
                else if (table.Columns[i].DataType.ToString().Contains("System.DateTime"))
                    sqlsc += " datetime ";
                else if (table.Columns[i].DataType.ToString().Contains("System.String"))
                    sqlsc += " nvarchar(" + MaxVarcharSize + ") ";
                else
                    sqlsc += " nvarchar(" + MaxVarcharSize + ") ";



                /*if (table.Columns[i].AutoIncrement)
                    sqlsc += " IDENTITY(" + table.Columns[i].AutoIncrementSeed.ToString() + "," + table.Columns[i].AutoIncrementStep.ToString() + ") ";
                if (!table.Columns[i].AllowDBNull)
                    sqlsc += " NOT NULL ";*/
                sqlsc += ",";

            }
            sqlsc = sqlsc.Substring(0, sqlsc.Length - 1) + ")";

            return sqlsc;

        }





    }
}
