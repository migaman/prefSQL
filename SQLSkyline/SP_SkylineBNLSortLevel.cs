using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;
using System.Collections.Generic;


//Hinweis: Wenn mit startswith statt equals gearbeitet wird führt dies zu massiven performance problemen, z.B. large dataset 30 statt 3 Sekunden mit 13 Dimensionen!!
//WICHTIG: Vergleiche immer mit equals und nie mit z.B. startsWith oder Contains oder so.... --> Enorme Performance Unterschiede
namespace prefSQL.SQLSkyline
{
    public class SP_SkylineBNLSortLevel
    {
        /// <summary>
        /// Calculate the skyline points from a dataset
        /// </summary>
        /// <param name="strQuery"></param>
        /// <param name="strOperators"></param>
        [Microsoft.SqlServer.Server.SqlProcedure(Name = "SP_SkylineBNLSortLevel")]
        public static void getSkyline(SqlString strQuery, SqlString strOperators, SqlBoolean isDebug)
        {
            ArrayList resultCollection = new ArrayList();
            ArrayList resultstringCollection = new ArrayList();
            string[] operators = strOperators.ToString().Split(';');

            SqlConnection connection = null;
            if (isDebug == false)
                connection = new SqlConnection(Helper.cnnStringSQLCLR);
            else
                connection = new SqlConnection(Helper.cnnStringLocalhost);

            try
            {
                //Some checks
                if (strQuery.ToString().Length == Helper.MaxSize)
                {
                    throw new Exception("Query is too long. Maximum size is " + Helper.MaxSize);
                }
                connection.Open();

                SqlDataAdapter dap = new SqlDataAdapter(strQuery.ToString(), connection);
                DataTable dt = new DataTable();
                dap.Fill(dt);


                // Build our record schema 
                List<SqlMetaData> outputColumns = Helper.buildRecordSchema(dt, operators);

                SqlDataRecord record = new SqlDataRecord(outputColumns.ToArray());
                if (isDebug == false)
                {
                    SqlContext.Pipe.SendResultsStart(record);
                }


                DataTableReader sqlReader = dt.CreateDataReader();


                //int iIndex = 0;
                //Read all records only once. (SqlDataReader works forward only!!)
                while (sqlReader.Read())
                {
                    //Check if window list is empty
                    if (resultCollection.Count == 0)
                    {
                        // Build our SqlDataRecord and start the results 
                        addToWindow(sqlReader, operators, ref resultCollection, ref resultstringCollection, record, isDebug);
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
                            //--> It is not possible that the new point dominates the one in the window --> Reason data is ORDERED
                        }
                        if (bDominated == false)
                        {
                            //dtInsert.ImportRow(dt.Rows[iIndex]);
                            addToWindow(sqlReader, operators, ref resultCollection, ref resultstringCollection, record, isDebug);


                        }

                    }
                }

                sqlReader.Close();

                if (isDebug == true)
                {
                    System.Diagnostics.Debug.WriteLine(resultCollection.Count);
                }
                else
                {
                    SqlContext.Pipe.SendResultsEnd();
                }


            }
            catch (Exception ex)
            {
                //Pack Errormessage in a SQL and return the result
                string strError = "Fehler in SP_SkylineBNLSortLevel: ";
                strError += ex.Message;

                if (isDebug == true)
                {
                    System.Diagnostics.Debug.WriteLine(strError);

                }
                else
                {
                    SqlContext.Pipe.Send(strError);
                }

            }
            finally
            {
                if (connection != null)
                    connection.Close();
            }

        }


        private static void addToWindow(DataTableReader sqlReader, string[] operators, ref ArrayList resultCollection, ref ArrayList resultstringCollection, SqlDataRecord record, SqlBoolean isDebug)
        {

            //Erste Spalte ist die ID
            long[] recordInt = new long[operators.GetUpperBound(0) + 1];
            string[] recordstring = new string[operators.GetUpperBound(0) + 1];


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

            if (isDebug == false)
            {
                SqlContext.Pipe.SendResultsRow(record);
            }
            resultCollection.Add(recordInt);
            resultstringCollection.Add(recordstring);
        }


        private static bool compare(DataTableReader sqlReader, string[] operators, long[] result, string[] stringResult)
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

        


    }
}