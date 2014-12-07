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
    class SkylineDQ
    {
        //Only this parameters are different beteen SQL CLR function and Utility class
        private const string connectionstring = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
        private const int MaxSize = 4000;

        [Microsoft.SqlServer.Server.SqlProcedure]
        public static void SP_SkylineDQ(SqlString strQuery, SqlString strOperators)
        {
            ArrayList resultCollection = new ArrayList();
            
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
                List<SqlMetaData> outputColumns = buildRecordSchema(dt, operators);
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
                        OutputColumn = new SqlMetaData(col.ColumnName, TypeConverter.ToSqlDbType(col.DataType));
                    }
                    else
                    {
                        OutputColumn = new SqlMetaData(col.ColumnName, TypeConverter.ToSqlDbType(col.DataType), col.MaxLength);
                    }
                    outputColumns.Add(OutputColumn);
                }
                iCol++;
            }
            return outputColumns;
        }


    }
}
