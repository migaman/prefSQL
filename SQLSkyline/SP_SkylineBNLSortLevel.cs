using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;
using System.Collections.Generic;


//Caution: Attention small changes in this code can lead to performance issues, i.e. using a startswith instead of an equal can increase by 10 times
//Important: Only use equal for comparing text (otherwise performance issues)
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

        private DataTable getSkylineTable(String strQuery, String strOperators, bool isIndependent, string strConnection)
        {
            ArrayList resultCollection = new ArrayList();
            string[] operators = strOperators.ToString().Split(';');
            DataTable dtResult = new DataTable();
            
           
            SqlConnection connection = null;
            if (isIndependent == false)
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
                if (isIndependent == false)
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
                        Helper.addToWindow(sqlReader, operators, ref resultCollection, record, isIndependent, ref dtResult);
                    }
                    else
                    {
                        bool isDominated = false;

                        //check if record is dominated (compare against the records in the window)
                        for (int i = resultCollection.Count - 1; i >= 0; i--)
                        {
                            long[] result = (long[])resultCollection[i];

                            //Dominanz
                            if (Helper.compare(sqlReader, operators, result) == true)
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
                            Helper.addToWindow(sqlReader, operators, ref resultCollection, record, isIndependent, ref dtResult);
                        }

                    }
                }

                sqlReader.Close();

                if (isIndependent == false)
                {
                    SqlContext.Pipe.SendResultsEnd();
                }

            }
            catch (Exception ex)
            {
                //Pack Errormessage in a SQL and return the result
                string strError = "Fehler in SP_SkylineBNLSortLevel: ";
                strError += ex.Message;

                if (isIndependent == true)
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