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


        private DataTable getSkylineTable(String strQuery, String strOperators, bool isIndependent, string strConnection)
        {
            ArrayList resultCollection = new ArrayList();
            string[] operators = strOperators.ToString().Split(';');
            DataTable dtResult = new DataTable();
            DataTable dtSkyline = new DataTable();

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


                dtSkyline = computeSkyline(dt, operators, operators.GetUpperBound(0));


                if (isIndependent == false)
                {
                    // Build our record schema 

                    List<SqlMetaData> outputColumns = Helper.buildRecordSchema(dt, operators, ref dtResult);
                    SqlDataRecord record = new SqlDataRecord(outputColumns.ToArray());
                    SqlContext.Pipe.SendResultsStart(record);

                    //foreach (SqlDataRecord recSkyline in btg[iItem])
                    foreach (DataRow row in dtSkyline.Rows)
                    {
                        for (int i = 0; i < dtSkyline.Columns.Count; i++)
                        {
                            //Only the real columns (skyline columns are not output fields)
                            if (i > operators.GetUpperBound(0))
                            {
                                record.SetValue(i - (operators.GetUpperBound(0) + 1), row[i]);
                            }
                        }

                        SqlContext.Pipe.SendResultsRow(record);
                    }

                    SqlContext.Pipe.SendResultsEnd();




                }



            }
            catch (Exception ex)
            {
                //Pack Errormessage in a SQL and return the result
                string strError = "Fehler in SP_SkylineDQ: ";
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
            return dtSkyline;
        }

        private DataTable computeSkyline(DataTable dt, string[] operators, int dim)
        {
            //Von diesen Punkten Skyline berechnen
            //DataTableReader sqlReader = dt.CreateDataReader();

            if(dt.Rows.Count == 1)
            {
                return dt;
            }

            //compute first median for some dimension
            int pivot = getMedian(dt, dim);
            DataTable list1 = dt.Clone();
            DataTable list2 = dt.Clone();


            //divide input intwo 2 partitions
            partition(dt, dim, pivot, ref list1, ref list2);
            

            //Rekursiv aufrufen
            DataTable Skyline1 = computeSkyline(list1, operators, dim);
            DataTable Skyline2 = computeSkyline(list2, operators, dim);


            DataTable dtMerge = mergeBasic(Skyline1, Skyline2, operators, operators.GetUpperBound(0));
            if (dtMerge.Rows.Count > 0)
            {
                foreach(DataRow row in dtMerge.Rows)
                {
                    Skyline1.ImportRow(row);
                }
            }

            return Skyline1 ;
        }


        private static void partition(DataTable dt, int dim, int pivot, ref DataTable list1, ref DataTable list2)
        {
            //divide input intwo 2 partitions
            for (int iRow = 0; iRow < dt.Rows.Count; iRow++)
            {
                if ((int)dt.Rows[iRow][dim] <= pivot)
                {
                    list1.ImportRow(dt.Rows[iRow]);
                }
                else
                {
                    list2.ImportRow(dt.Rows[iRow]);
                }
            }
        }


        private bool isBigger(DataRow row1, DataRow row2, string[] operators)
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol <= operators.GetUpperBound(0); iCol++)
            {
                if ((int)row1[iCol] < (int)row2[iCol])
                {
                    return false;
                }
                else if ((int)row1[iCol] > (int)row2[iCol])
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

        private DataTable mergeBasic(DataTable s1, DataTable s2, string[] operators, int dim)
        {
            DataTable dtSkyline = s1.Clone();
            if(s1.Rows.Count == 1)
            {
                DataRow p = s1.Rows[0];
                for(int i = 1; i < s2.Rows.Count; i++)
                {
                    DataRow q = s2.Rows[i];
                    if (isBigger(p,q, operators))
                    {
                        dtSkyline.ImportRow(q);
                    }
                }

            }
            else if(s2.Rows.Count == 1)
            {
                //dtSkyline = s2;
                DataRow p = s2.Rows[0];
                for (int i = 1; i < s1.Rows.Count; i++)
                {
                    DataRow q = s1.Rows[i];
                    if (isBigger(p, q, operators))
                    {
                        dtSkyline.ImportRow(q);
                    }
                }
            }
            else if(operators.GetUpperBound(0) == 1)
            {
                //Nur 2 Dimensionen
                //DataRow min = min(s1);
                DataRow min = s1.Rows[0];
                for(int i = 1; i < s2.Rows.Count; i++)
                {
                    DataRow q = s2.Rows[i];
                    if(isBigger(q, min, operators) == false)
                    {
                        dtSkyline.ImportRow(q);
                    }
                }
            }
            else
            {
                int pivot = getMedian(s2, dim - 1);
                DataTable s11 = s1.Clone();
                DataTable s12 = s1.Clone();
                DataTable s21 = s1.Clone();
                DataTable s22 = s1.Clone();
                partition(s1, dim - 1, pivot, ref s11, ref s12);
                partition(s2, dim - 1, pivot, ref s21, ref s22);
                
                DataTable r1 = mergeBasic(s11, s21, operators, dim);
                DataTable r2 = mergeBasic(s12, s22, operators, dim); ;
                DataTable r3 = mergeBasic(s11, r2, operators, dim - 1); ;
                dtSkyline.Merge(r1);
                dtSkyline.Merge(r3);
            }


            return dtSkyline;
        }


        private static int getMedian(DataTable dt, int dim)
        {
            //Framework 2.0 version of this method. there is an easier way in F4        
            if (dt == null || dt.Rows.Count == 0)
                return 0;

            int[] sourceNumbers = new int[dt.Rows.Count];
            //generate list of integers of this dimension
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                sourceNumbers[i] = (int)dt.Rows[i][dim];
            }


            //make sure the list is sorted, but use a new array
            int[] sortedPNumbers = (int[])sourceNumbers.Clone();
            sourceNumbers.CopyTo(sortedPNumbers, 0);
            Array.Sort(sortedPNumbers);

            //get the median
            int size = sortedPNumbers.Length;
            int mid = size / 2;
            int median = (size % 2 != 0) ? (int)sortedPNumbers[mid] : ((int)sortedPNumbers[mid] + (int)sortedPNumbers[mid - 1]) / 2;
            return median;
        }



    }
}
