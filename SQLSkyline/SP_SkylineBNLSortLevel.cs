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
        public static void getSkyline(SqlString strQuery, SqlString strOperators)
        {
            SP_SkylineBNLSortLevel skyline = new SP_SkylineBNLSortLevel();
            skyline.getSkylineTable(strQuery.ToString(), strOperators.ToString(), false, "");
        }

        public DataTable getSkylineTable(String strQuery, String strOperators, String strConnection)
        {
            return getSkylineTable(strQuery, strOperators, true, strConnection);
        }

        private DataTable getSkylineTable(String strQuery, String strOperators, bool isDebug, string strConnection)
        {
            ArrayList resultCollection = new ArrayList();
            ArrayList resultstringCollection = new ArrayList();
            string[] operators = strOperators.ToString().Split(';');
            DataTable dtResult = new DataTable();
            
           
            SqlConnection connection = null;
            if (isDebug == false)
                connection = new SqlConnection(Helper.cnnStringSQLCLR);
            else
                connection = new SqlConnection(strConnection);

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
                List<SqlMetaData> outputColumns = Helper.buildRecordSchema(dt, operators, ref dtResult);
                SqlDataRecord record = new SqlDataRecord(outputColumns.ToArray());
                if (isDebug == false)
                {
                    SqlContext.Pipe.SendResultsStart(record);
                }

                
                //Read all records only once. (SqlDataReader works forward only!!)
                DataTableReader sqlReader = dt.CreateDataReader();
                while (sqlReader.Read())
                {
                    //Check if window list is empty
                    if (resultCollection.Count == 0)
                    {
                        //first record is always added to collection
                        Helper.addToWindow(sqlReader, operators, ref resultCollection, ref resultstringCollection, record, isDebug, ref dtResult);
                    }
                    else
                    {
                        bool isDominated = false;

                        //check if record is dominated (compare against the records in the window)
                        for (int i = resultCollection.Count - 1; i >= 0; i--)
                        {
                            long[] result = (long[])resultCollection[i];
                            string[] strResult = (string[])resultstringCollection[i];

                            //Dominanz
                            if (Helper.compare(sqlReader, operators, result, strResult) == true)
                            {
                                //New point is dominated. No further testing necessary
                                isDominated = true;
                                break;
                            }

                            //Now, check if the new point dominates the one in the window
                            //--> It is not possible that the new point dominates the one in the window --> Reason data is ORDERED
                        }
                        if (isDominated == false)
                        {
                            Helper.addToWindow(sqlReader, operators, ref resultCollection, ref resultstringCollection, record, isDebug, ref dtResult);
                        }

                    }
                }

                sqlReader.Close();

                if (isDebug == false)
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
            return dtResult;
        }



        


        

        


    }
}