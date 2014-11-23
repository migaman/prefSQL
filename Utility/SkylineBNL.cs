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
        //private const string TempTable = "##MySkylineTable";
        //private const int MaxVarcharSize = 100;

        [Microsoft.SqlServer.Server.SqlProcedure]
        public static void SP_SkylineBNL(SqlString strQuery, SqlString strOperators)
        {
            ArrayList resultCollection = new ArrayList();
            ArrayList resultstringCollection = new ArrayList();
            string[] operators = strOperators.ToString().Split(';');
            

            SqlConnection connection = new SqlConnection(connectionstring);
            try
            {
                //Some checks
                if (strQuery.ToString().Length == MaxSize)
                {
                    throw new Exception("Query is too long. Maximum size is " + MaxSize);
                }
                connection.Open();

                SqlDataAdapter dap = new SqlDataAdapter(strQuery.ToString(), connection);
                DataTable dt = new DataTable();
                dap.Fill(dt);


                // Build our record schema 
                List<SqlMetaData> OutputColumns = new List<SqlMetaData>(dt.Columns.Count);
                int iCol = 0;
                foreach (DataColumn col in dt.Columns)
                {
                    //Only the real columns (skyline columns are not output fields)
                    if (iCol > operators.GetUpperBound(0))
                    {
                        SqlMetaData OutputColumn;
                        if (col.DataType.Equals(typeof(Int32)) || col.DataType.Equals(typeof(DateTime)))
                        {
                            OutputColumn = new SqlMetaData(col.ColumnName, TypeConverter.ToSqlDbType(col.DataType));
                        }
                        else
                        {
                            OutputColumn = new SqlMetaData(col.ColumnName, TypeConverter.ToSqlDbType(col.DataType), col.MaxLength);
                        }

                        //SqlMetaData OutputColumn = new SqlMetaData(col.ColumnName, prefSQL.SQLSkyline.TypeConverter.ToSqlDbType(col.DataType), col.MaxLength); 
                        OutputColumns.Add(OutputColumn);
                    }
                    iCol++;
                    
                }
                SqlDataRecord record = new SqlDataRecord(OutputColumns.ToArray());
                //SqlContext.Pipe.SendResultsStart(record);

                //dt.CreateDataReader();
                /*string sqlsc = createTABLEStructure(dt);
                SqlCommand sqlCommand = new SqlCommand(sqlsc, connection);
                sqlCommand.ExecuteNonQuery();*/

                //Clones the structure of the DataTable, including all DataTable schemas and constraints.
                //DataTable dtInsert = dt.Clone();

                //SqlCommand sqlCommand = new SqlCommand(strDimensions.ToString(), connection);
                DataTableReader sqlReader = dt.CreateDataReader();
                

                //int iIndex = 0;
                //Read all records only once. (SqlDataReader works forward only!!)
                while (sqlReader.Read())
                {
                    //Check if window list is empty
                    if (resultCollection.Count == 0)
                    {
                        // Build our SqlDataRecord and start the results 

                        //dtInsert.ImportRow(dt.Rows[iIndex]);
                        //record.SetInt32(0, 10);
                        addToWindow(sqlReader, operators, ref resultCollection, ref resultstringCollection, record);
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
                            /*for (int col = 0; col < dt.Columns.Count; col++)
                            {
                                record.SetValue(col, dt.Rows[iIndex].ItemArray[col]);
                            }*/
                            //SqlContext.Pipe.SendResultsRow(record);

                            //dtInsert.ImportRow(dt.Rows[iIndex]);
                            addToWindow(sqlReader, operators, ref resultCollection, ref resultstringCollection, record);


                        }

                    }
                }

                sqlReader.Close();

                //SqlContext.Pipe.SendResultsEnd();

                //Bulk load into sql server
                //dtInsert.AcceptChanges();


                //System.Diagnostics.Debug.WriteLine(dtInsert.Rows.Count);
                /*using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = TempTable;
                    bulkCopy.WriteToServer(dtInsert);
                }*/

                //SendDataTableOverPipe(dtInsert);

                //SqlContext.Pipe.SendResultsEnd();
                /*
                string strSQL = "SELECT * FROM " + TempTable;

                sqlCommand = new SqlCommand(strSQL, connection);
                sqlReader = sqlCommand.ExecuteReader();
            
                */


                //DataTableReader reader = dtInsert.CreateDataReader();

                //TODO: only for real Stored Procedure
                //SqlContext.Pipe.Send(sqlReader);
                //SqlContext.Pipe.SendResultsEnd();



                /*
                strSQL = "DROP TABLE " + TempTable;
                sqlCommand = new SqlCommand(strSQL, connection);
                sqlCommand.ExecuteNonQuery();
                */

            }
            catch (Exception ex)
            {
                //Pack Errormessage in a SQL and return the result
                string strError = "SELECT 'Fehler in SP_SkylineBNL: ";
                strError += ex.Message.Replace("'", "''");
                strError += "'";

                //TODO: only for real Stored Procedure
                SqlContext.Pipe.Send(strError);

            }
            finally
            {
                if (connection != null)
                    connection.Close();
            }

        }


        private static void addToWindow(DataTableReader sqlReader, string[] operators, ref ArrayList resultCollection, ref ArrayList resultstringCollection, SqlDataRecord record)
        {

            //Erste Spalte ist die ID
            long[] recordInt = new long[operators.GetUpperBound(0)+1];
            string[] recordstring = new string[operators.GetUpperBound(0)+1];


            for (int iCol = 0; iCol < sqlReader.FieldCount; iCol++)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol <= operators.GetUpperBound(0))
                {
                    //LOW und HIGH Spalte in record abfüllen
                    if (operators[iCol].Equals("LOW"))
                    {
                        recordInt[iCol] = sqlReader.GetInt32(iCol);

                        //Check if long value is incomparable
                        if (iCol + 1 <= recordInt.GetUpperBound(0) && operators[iCol + 1].Equals("INCOMPARABLE"))
                        {
                            //Incomparable field is always the next one
                            recordstring[iCol] = sqlReader.GetString(iCol + 1);
                        }
                    }
                    
                }
                else
                {
                    record.SetValue(iCol - (operators.GetUpperBound(0)+1), sqlReader[iCol]);
                }
                

            }


           
            //SqlContext.Pipe.SendResultsRow(record);
            resultCollection.Add(recordInt);
            resultstringCollection.Add(recordstring);
        }


        private static bool compare(DataTableReader sqlReader, string[] operators, long[] result, string[] stringResult)
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
            {
                string op = operators[iCol];
                //Compare only LOW attributes
                if (op.Equals("LOW"))
                {
                    long value = sqlReader.GetInt32(iCol);
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
                            if (iCol + 1 <= result.GetUpperBound(0) && operators[iCol + 1].Equals("INCOMPARABLE"))
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

        /*
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
        /*      sqlsc += ",";

          }
          sqlsc = sqlsc.Substring(0, sqlsc.Length - 1) + ")";

          return sqlsc;



      }*/

        
        


    }
}
