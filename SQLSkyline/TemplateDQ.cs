using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;
using System.Diagnostics;

//!!!Caution: Attention small changes in this code can lead to remarkable performance issues!!!!
namespace prefSQL.SQLSkyline
{
    /// <summary>
    /// D&Q Algorithm implemented according to algorithm pseudocode in Börzsönyi et al. (2001) and Rost(2006)
    /// </summary>
    /// <remarks>
    /// Börzsönyi, Stephan; Kossmann, Donald; Stocker, Konrad (2001): The Skyline Operator. In : 
    /// Proceedings of the 17th International Conference on Data Engineering. Washington, DC, USA: 
    /// IEEE Computer Society, pp. 421–430. Available online at http://dl.acm.org/citation.cfm?id=645484.656550.
    /// 
    /// Rost, Steffen Thomas (2006): Skyline query processing: University Heidelberg; Fakultät für Mathematik und Informatik. Institut für Informatik.
    /// 
    /// Profiling considersations:
    /// - Always use equal when comparins test --> i.e. using a startswith instead of an equal can decrease performance by 10 times
    /// - Write objects from DataReader into an object[] an work with the object. 
    /// - Explicity convert (i.e. (int)reader[0]) value from DataReader and don't use the given methods (i.e. reader.getInt32(0))
    /// </remarks>
    public class TemplateDQ
    {
        public long timeInMs = 0;

        public DataTable getSkylineTable(String strQuery, String strOperators, int numberOfRecords, String strConnection)
        {
            return getSkylineTable(strQuery, strOperators, numberOfRecords, true, strConnection);
        }


        protected DataTable getSkylineTable(String strQuery, String strOperators, int numberOfRecords, bool isIndependent, string strConnection)
        {
            Stopwatch sw = new Stopwatch();
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

                //Time the algorithm needs (afer query to the database)
                sw.Start();

                // Build our record schema 
                List<SqlMetaData> outputColumns = Helper.buildRecordSchema(dt, operators, dtResult);
                SqlDataRecord record = new SqlDataRecord(outputColumns.ToArray());


                dtResult = computeSkyline(dt, operators, operators.GetUpperBound(0), false);

                //Remove certain amount of rows if query contains TOP Keyword
                Helper.getAmountOfTuples(dtResult, numberOfRecords);

                
                if (isIndependent == false)
                {                   
                    SqlContext.Pipe.SendResultsStart(record);

                    foreach (DataRow row in dtResult.Rows)
                    {
                        for (int i = 0; i < dtResult.Columns.Count; i++)
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

            sw.Stop();
            timeInMs = sw.ElapsedMilliseconds;
            return dtResult;
        }

        private DataTable computeSkyline(DataTable dt, string[] operators, int dim, bool stopRecursion)
        {
            if (dt.Rows.Count <= 1)
                return dt;


            //compute first median for some dimension
            double pivot = getMedian(dt, dim);
            DataTable list1 = dt.Clone();
            DataTable list2 = dt.Clone();


            //divide input intwo 2 partitions
            partition(dt, dim, pivot, ref list1, ref list2);


            //daten waren nicht weiter splittbar
            if (list1.Rows.Count == 0 || list2.Rows.Count == 0)
            {
                
                if (dt.Rows.Count == 1)
                {
                    return dt;
                }
                else
                {
                    //in dieser dimension nicht weiter splittbar --> versuchen in einer dimension tiefer zu splitten
                    if (dim > 0)
                    {
                        return computeSkyline(dt, operators, dim - 1, false);
                    }
                    else
                    {
                        //alle in skyline, keine weitere trennung möglich
                        return dt;
                    }
                }
            }


            //Wenn der Median keine klare Trennung mehr bringt Objekt auch zurückgeben
            bool bStop1 = false;
            bool bStop2 = false;
            if (list1.Rows.Count == 0)
            {
                bStop1 = true;
            }
            if (list2.Rows.Count == 0)
            {
                bStop2 = true;
            }




            //Rekursiv aufrufen
            DataTable Skyline1 = computeSkyline(list1, operators, dim, bStop2);
            
            DataTable Skyline2 = computeSkyline(list2, operators, dim, bStop1);


            DataTable dtMerge = mergeBasic(Skyline1, Skyline2, operators, operators.GetUpperBound(0));
            if (dtMerge.Rows.Count > 0)
            {
                foreach (DataRow row in dtMerge.Rows)
                {
                    Skyline1.ImportRow(row);
                }
            }

            return Skyline1;
        }


        private void partition(DataTable dt, int dim, double pivot, ref DataTable list1, ref DataTable list2)
        {
            //divide input intwo 2 partitions
            for (int iRow = 0; iRow < dt.Rows.Count; iRow++)
            {
                if ((long)dt.Rows[iRow][dim] <= pivot)
                {
                    list1.ImportRow(dt.Rows[iRow]);
                }
                else
                {
                    list2.ImportRow(dt.Rows[iRow]);
                }
            }
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="s1"></param>
        /// <param name="s2"></param>
        /// <param name="operators"></param>
        /// <param name="dim"></param>
        /// <returns>tuples from skyline table 2 that are not dominated</returns>
        private DataTable mergeBasic(DataTable s1, DataTable s2, string[] operators, int dim)
        {
            //Clone the structure (not the record) from table1
            DataTable dtSkyline = s1.Clone();

            if (dim == 0)
            {
                //No Operation, return only the left list
            }
            else if (s1.Rows.Count == 0) 
            {
                return s2;
                //return dtSkyline;
            }
            else if (s2.Rows.Count == 0)
            {
                return dtSkyline; // dtSkyline;
            } 
            else if (s1.Rows.Count == 1)
            {
                //OK, falls p nicht dominiert wird von q
                DataRow p = s1.Rows[0];
                for (int i = 0; i < s2.Rows.Count; i++)
                {
                    DataRow q = s2.Rows[i];

                    for (int iDim = dim - 1; iDim >= 0; iDim--)
                    {
                        if ((long)q[iDim] < (long)p[iDim])
                        {
                            dtSkyline.ImportRow(q);
                            break;
                        }
                    }

                }

            }
            else if (s2.Rows.Count == 1)
            {
                //wenn p von q dominiert weg --> list

                //Add tuple only if is is not dominated from one of the others
                dtSkyline.ImportRow(s2.Rows[0]);
                DataRow q = s2.Rows[0];

                for (int i = 0; i < s1.Rows.Count; i++)
                {
                    DataRow p = s1.Rows[i];
                    bool doesDominate = false;

                    for (int iDim = dim- 1; iDim >= 0; iDim--)
                    {
                        //Is better in at least one dimension!
                        //<= is wrong, otherwise tuples with equal values in one dimension are not removed from the skyline
                        if ((long)q[iDim] < (long)p[iDim])
                        {
                            doesDominate = true;
                            break;
                        }
                        else
                        {
                            doesDominate = false;
                        }
                    }

                    if (doesDominate == false)
                    {
                        dtSkyline.Rows.Clear();
                        break;
                    }
                    else
                    {

                    }
                    //}
                }
            }
            else if (operators.GetUpperBound(0) == 1)
            {
                //Nur 2 Dimensionen
                //DataRow min = min(s1);
                long min = (long)s1.Rows[0][dim - 1];
                for (int i = 1; i < s1.Rows.Count; i++)
                {
                    if ((long)s1.Rows[i][dim - 1] < min)
                        min = (long)s1.Rows[i][dim - 1];
                }

                for (int i = 0; i < s2.Rows.Count; i++)
                {
                    DataRow q = s2.Rows[i];
                    if ((long)q[dim - 1] < min)
                    {
                        dtSkyline.ImportRow(q);
                    }
                }
            }
            else
            {
                double pivot1 = getMedian(s1, dim - 1);
                DataTable s11 = s1.Clone();
                DataTable s12 = s1.Clone();
                DataTable s21 = s1.Clone();
                DataTable s22 = s1.Clone();

                partition(s1, dim - 1, pivot1, ref s11, ref s12);
                partition(s2, dim - 1, pivot1, ref s21, ref s22);

                if(s12.Rows.Count == 0 && s22.Rows.Count == 0)
                {
                    if(s11.Rows.Count > 1 && s21.Rows.Count > 1)
                    {
                        //all elements have same value --> compare all tuples against all tuples (BNL-Style)
                        
                        //compare all from s21 against s11
                        for (int i = 0; i < s21.Rows.Count; i++)
                        {
                            //Import row
                            bool isDominated = false;
                            for(int ii = 0; ii < s11.Rows.Count; ii++)
                            {
                                bool isNotDominated = false;
                                for (int iDim = dim - 1; iDim >= 0; iDim--)
                                {
                                    if ((long)s21.Rows[i][iDim] < (long)s11.Rows[ii][iDim])
                                    {
                                        isNotDominated = true;
                                        break;
                                    }
                                }
                                if (isNotDominated == false)
                                {
                                    isDominated = true;
                                    break;
                                }
                            }
                            if (isDominated == false)
                            {
                                dtSkyline.ImportRow(s21.Rows[i]);
                            }
                        }

                        //dtSkyline.Merge(s2);
                        return dtSkyline;
                    }                    
                }

                DataTable r1 = mergeBasic(s11, s21, operators, dim);
                DataTable r2 = mergeBasic(s12, s22, operators, dim); ;
                DataTable r3 = mergeBasic(s11, r2, operators, dim - 1); ;
                dtSkyline.Merge(r1);
                dtSkyline.Merge(r3);
            }

            if(dtSkyline.Rows.Count > 0)
                return dtSkyline;
            else
                return dtSkyline;
        }

        /// <summary>
        /// Computes the median of a datatable
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="dim"></param>
        /// <returns></returns>
        private double getMedian(DataTable dt, int dim)
        {
            //Framework 2.0 version of this method. there is an easier way in F4        
            if (dt == null || dt.Rows.Count == 0)
                return 0;


            //int[] sourceNumbers = new int[dt.Rows.Count];

            

            //HashSet<long> uniqueNumbers = new HashSet<long>();
            //HashSet is not supported with CLR --> use Dictionary and set all values true
            Dictionary<long, bool> uniqueNumbers = new Dictionary<long, bool>();
            //HashSet is verboten in MS SQL CLR
            //generate list of unique integers of this dimension
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                if (!uniqueNumbers.ContainsKey((long)dt.Rows[i][dim]))
                {
                    uniqueNumbers.Add((long)dt.Rows[i][dim], true);
                }
                
                
            }
            long[] sourceNumbers = new long[uniqueNumbers.Count];
            uniqueNumbers.Keys.CopyTo(sourceNumbers, 0);

            //make sure the list is sorted, but use a new array
            long[] sortedPNumbers = (long[])sourceNumbers.Clone();
            sourceNumbers.CopyTo(sortedPNumbers, 0);
            Array.Sort(sortedPNumbers);

            //get the median
            int size = sortedPNumbers.Length;
            int mid = size / 2;

            //compute the double value because if one element is 1 and the other 2, otherwise the tuples are not splitted

            double median = (size % 2 != 0) ? (long)sortedPNumbers[mid] : (double)(((long)sortedPNumbers[mid] + (long)sortedPNumbers[mid - 1])) / 2;


            return median;
        }


    }
}
