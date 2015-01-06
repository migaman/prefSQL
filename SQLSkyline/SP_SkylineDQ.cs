//------------------------------------------------------------------------------
// <copyright file="CSSqlClassFile.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;


//Caution: Attention small changes in this code can lead to performance issues, i.e. using a startswith instead of an equal can increase by 10 times
//Important: Only use equal for comparing text (otherwise performance issues)
namespace prefSQL.SQLSkyline
{
    
    public class SP_SkylineDQ
    {
        [Microsoft.SqlServer.Server.SqlProcedure(Name = "SP_SkylineDQ")]
        public static void getSkyline(SqlString strQuery, SqlString strOperators)
        {
            SP_SkylineDQ skyline = new SP_SkylineDQ();
            skyline.getSkylineTable(strQuery.ToString(), strOperators.ToString(), false, "");
        }

        public DataTable getSkylineTable(String strQuery, String strOperators, String strConnection)
        {
            return getSkylineTable(strQuery, strOperators, true, strConnection);
        }


        private DataTable getSkylineTable(String strQuery, String strOperators, bool isDebug, string strConnection)
        {
            ArrayList resultCollection = new ArrayList();
            string[] operators = strOperators.ToString().Split(';');
            DataTable dtResult = new DataTable();
            
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


                computeSkyline(dt, operators); ;



                //Pivot := Median{dimension} // equi-partition the set
                /*(P1,P2) := Partition(M,Dimension,Pivot)
                S1 := SkylineBasic(P1,Dimension) // compute skyline recursively
                S2 := SkylineBasic(P2,Dimension)
                return S1 [_ Mer   geBasic(S1,S2,Dimension)
                */


                /*int iCount = 0;
                //Read all records only once. (SqlDataReader works forward only!!)
                while (sqlReader.Read())
                {
                    //Check if window list is empty
                    if (resultCollection.Count == 0)
                    {
                        // Build our SqlDataRecord and start the results 
                        iCount++;
                        addToWindow(sqlReader, operators, ref resultCollection, record);
                    }
                    else
                    {
                        bool bDominated = false;

                        //check if record is dominated (compare against the records in the window)
                        for (int i = resultCollection.Count - 1; i >= 0; i--)
                        {
                            int[] result = (int[])resultCollection[i];
                            

                            //Dominanz
                            if (compare(sqlReader, result) == true)
                            {
                                //New point is dominated. No further testing necessary
                                bDominated = true;
                                break;
                            }


                        }
                        if (bDominated == false)
                        {
                            iCount++;
                            addToWindow(sqlReader, operators, ref resultCollection, record);


                        }

                    }
                }

                sqlReader.Close();*/

                //System.Diagnostics.Debug.WriteLine("Rows:" + computeMedianPrice);



            }
            catch (Exception ex)
            {
                //Pack Errormessage in a SQL and return the result
                string strError = "Fehler in SP_SkylineDQ: ";
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

        private static void computeSkyline(DataTable dt, string[] operators)
        {
            //Von diesen Punkten Skyline berechnen
            DataTableReader sqlReader = dt.CreateDataReader();

            //compute first median for some dimension
            int computeMedianPrice = (int)dt.Rows[dt.Rows.Count / 2][0];


            ArrayList list1 = new ArrayList();
            ArrayList list2 = new ArrayList();

            //divide input intwo 2 partitions
            while (sqlReader.Read())
            {

                int[] recordInt = new int[operators.GetUpperBound(0) + 1];
                for (int iCol = 0; iCol <= recordInt.GetUpperBound(0); iCol++)
                {
                    //Only the real columns (skyline columns are not output fields)
                    recordInt[iCol] = sqlReader.GetInt32(iCol);
                }


                if ((int)sqlReader[0] < computeMedianPrice)
                {
                    list1.Add(recordInt);
                }
                else
                {
                    list2.Add(recordInt);
                }
            }

            //wieder median berechnen von listen



            //ArrayList Skyline1 = computeSkyline(list1);
            //ArrayList Skyline2 = computeSkyline(list2);



            return; // list;
        }


        private static void median(ArrayList left, ArrayList right, ref ArrayList[] data)
        {
            //compute first median for some dimension
            int[] recordInt = (int[])data.GetValue(data.GetUpperBound(0) / 2);
            int computeMedianPrice = recordInt[0];


            //divide input intwo 2 partitions
            for (int i = 0; i < data.GetUpperBound(0); i++)
            {
                if (recordInt[0] < computeMedianPrice)
                {
                    left.Add(data.GetValue(i));
                }
                else
                {
                    right.Add(data.GetValue(i));
                }
            }


            median(left, right, ref data);


            /*if (left < right)
            {
                int teiler = teile(links, rechts, ref daten);
                median(links, teiler - 1, ref daten);
                median(teiler + 1, rechts, ref daten);
            }*/
        }

        private static int teile(int links, int rechts, ref int[] daten)
        {
            return 1;
        }




        private static int GetMedian(int[] sourceNumbers)
        {
            //Framework 2.0 version of this method. there is an easier way in F4        
            if (sourceNumbers == null || sourceNumbers.Length == 0)
                return 0;

            //make sure the list is sorted, but use a new array
            double[] sortedPNumbers = (double[])sourceNumbers.Clone();
            sourceNumbers.CopyTo(sortedPNumbers, 0);
            Array.Sort(sortedPNumbers);

            //get the median
            int size = sortedPNumbers.Length;
            int mid = size / 2;
            int median = (size % 2 != 0) ? (int)sortedPNumbers[mid] : ((int)sortedPNumbers[mid] + (int)sortedPNumbers[mid - 1]) / 2;
            return median;
        }


        private static void addToWindow(DataTableReader sqlReader, string[] operators, ref ArrayList resultCollection, SqlDataRecord record)
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


        private static bool compare(DataTableReader sqlReader, int[] result)
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
            {
                if (sqlReader.GetInt32(iCol) < result[iCol])
                {
                    return false;
                }
                else if (sqlReader.GetInt32(iCol) > result[iCol])
                {
                    greaterThan = true;
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
