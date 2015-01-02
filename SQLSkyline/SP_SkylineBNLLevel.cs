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
    public class SP_SkylineBNLLevel
    {
        /// <summary>
        /// Calculate the skyline points from a dataset
        /// </summary>
        /// <param name="strQuery"></param>
        /// <param name="strOperators"></param>
        /// <param name="isDebug"></param>
        [Microsoft.SqlServer.Server.SqlProcedure(Name = "SP_SkylineBNLLevel")]
        public static void getSkyline(SqlString strQuery, SqlString strOperators, SqlBoolean isDebug)
        {
            ArrayList resultCollection = new ArrayList();
            string[] operators = strOperators.ToString().Split(';');
            ArrayList recordCollection = new ArrayList();

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

                DataTableReader sqlReader = dt.CreateDataReader();

                //Read all records only once. (SqlDataReader works forward only!!)
                while (sqlReader.Read())
                {
                    //Check if window list is empty
                    if (resultCollection.Count == 0)
                    {
                        // Build our SqlDataRecord and start the results 
                        addToWindow(sqlReader, operators, ref resultCollection, isDebug, ref recordCollection);
                    }
                    else
                    {
                        bool bDominated = false;

                        //check if record is dominated (compare against the records in the window)
                        for (int i = resultCollection.Count - 1; i >= 0; i--)
                        {
                            long[] result = (long[])resultCollection[i];

                            //Dominanz
                            if (compare(sqlReader, operators, result) == true)
                            {
                                //New point is dominated. No further testing necessary
                                bDominated = true;
                                break;
                            }


                            //Now, check if the new point dominates the one in the window
                            //This is only possible with not sorted data
                            if (compareDifferent(sqlReader, operators, result) == true)
                            {
                                //The new record dominates the one in the windows. Remove point from window and test further
                                resultCollection.RemoveAt(i);
                                recordCollection.RemoveAt(i);
                            }

                        }
                        if (bDominated == false)
                        {
                            addToWindow(sqlReader, operators, ref resultCollection, isDebug, ref recordCollection);
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
                    //Send results to client
                    SqlDataRecord record = new SqlDataRecord(outputColumns.ToArray());
                    SqlContext.Pipe.SendResultsStart(record);

                    //foreach (SqlDataRecord recSkyline in btg[iItem])
                    foreach (ArrayList recSkyline in recordCollection)
                    {
                        for (int i = 0; i < recSkyline.Count; i++)
                        {
                            record.SetValue(i, recSkyline[i]);
                        }
                        SqlContext.Pipe.SendResultsRow(record);
                    }
                    SqlContext.Pipe.SendResultsEnd();
                }


            }
            catch (Exception ex)
            {
                //Pack Errormessage in a SQL and return the result
                string strError = "SELECT 'Fehler in SP_SkylineBNL: ";
                strError += ex.Message.Replace("'", "''");
                strError += "'";

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


        private static void addToWindow(DataTableReader sqlReader, string[] operators, ref ArrayList resultCollection, SqlBoolean isDebug, ref ArrayList recordCollection)
        {

            //Erste Spalte ist die ID
            long[] recordInt = new long[operators.GetUpperBound(0) + 1];
            string[] recordstring = new string[operators.GetUpperBound(0) + 1];
            ArrayList al = new ArrayList();

            for (int iCol = 0; iCol < sqlReader.FieldCount; iCol++)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol <= operators.GetUpperBound(0))
                {
                    recordInt[iCol] = sqlReader.GetInt32(iCol);
                }
                else
                {
                    //record.SetValue(iCol - (operators.GetUpperBound(0) + 1), sqlReader[iCol]);
                    al.Add(sqlReader[iCol]);
                }


            }


            recordCollection.Add(al);
            resultCollection.Add(recordInt);
        }


        private static bool compare(DataTableReader sqlReader, string[] operators, long[] result)
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
            {
                string op = operators[iCol];
                //Compare only LOW attributes
                
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



        private static bool compareDifferent(DataTableReader sqlReader, string[] operators, long[] result)
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
            {
                string op = operators[iCol];
                
                long value = sqlReader.GetInt32(iCol);
                int comparison = Helper.compareValue(result[iCol], value);

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