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
    class SkylineBNLLevel
    {
        //Only this parameters are different beteen SQL CLR function and Utility class
        private const string connectionstring = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
        private const int MaxSize = 4000;
        private static DataTableReader sqlReader;
        private static string[] operators;
        private static ArrayList resultCollection;

        [Microsoft.SqlServer.Server.SqlProcedure]
        public static void SP_SkylineBNLLevel(SqlString strQuery, SqlString strOperators)
        {
            resultCollection = new ArrayList();
            operators = strOperators.ToString().Split(';');


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
                List<SqlMetaData> outputColumns = buildRecordSchema(dt, operators);

                SqlDataRecord record = new SqlDataRecord(outputColumns.ToArray());
          
                sqlReader = dt.CreateDataReader();



                //Read all records only once. (SqlDataReader works forward only!!)
                while (sqlReader.Read())
                {
                    //Check if window list is empty
                    if (resultCollection.Count == 0)
                    {
                        // Build our SqlDataRecord and start the results 
                        addToWindow(record);
                    }
                    else
                    {
                        bool bDominated = false;

                        //check if record is dominated (compare against the records in the window)
                        for (int i = resultCollection.Count - 1; i >= 0; i--)
                        {
                            int[] result = (int[])resultCollection[i];

                            //Dominanz
                            if (compare(result) == true)
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
                            addToWindow(record);


                        }

                    }
                }

                sqlReader.Close();


                System.Diagnostics.Debug.WriteLine(resultCollection.Count);


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


        private static void addToWindow(SqlDataRecord record)
        {

            //Erste Spalte ist die ID
            int[] recordInt = new int[operators.GetUpperBound(0) + 1];
            for (int iCol = 0; iCol < sqlReader.FieldCount; iCol++)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol <= operators.GetUpperBound(0))
                {
                    
                    recordInt[iCol] = sqlReader.GetInt32(iCol);
                }
                else
                {
                    record.SetValue(iCol - (operators.GetUpperBound(0) + 1), sqlReader[iCol]);
                }


            }


            resultCollection.Add(recordInt);
        }


        private static bool compare(int[] result)
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
            {
                //Compare only LOW attributes
                int value = sqlReader.GetInt32(iCol);
                int comparison = compareValue(value, result[iCol]);

                if (comparison == 2)
                {
                        
                    //at least one must be greater than
                    greaterThan = true;
                        
                }
                else if (comparison == 0)
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


        /*
         * 0 = false
         * 1 = equal
         * 2 = greater than
         * */
        private static int compareValue(int value1, int value2)
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


        private static List<SqlMetaData> buildRecordSchema(DataTable dt, string[] operators)
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
                        OutputColumn = new SqlMetaData(col.ColumnName, Utility.TypeConverter.ToSqlDbType(col.DataType));
                    }
                    else
                    {
                        OutputColumn = new SqlMetaData(col.ColumnName, Utility.TypeConverter.ToSqlDbType(col.DataType), col.MaxLength);
                    }
                    outputColumns.Add(OutputColumn);
                }
                iCol++;
            }
            return outputColumns;
        }



    }




}
