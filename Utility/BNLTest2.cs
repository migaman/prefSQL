using System;
using System.Data;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Data.Common;
using System.IO;
using System.Text;



//!!!Caution: Attention small changes in this code can lead to remarkable performance issues!!!!
namespace Utility
{

    /// <summary>
    /// BNL Algorithm implemented according to algorithm pseudocode in Börzsönyi et al. (2001)
    /// </summary>
    /// <remarks>
    /// Börzsönyi, Stephan; Kossmann, Donald; Stocker, Konrad (2001): The Skyline Operator. In : 
    /// Proceedings of the 17th International Conference on Data Engineering. Washington, DC, USA: 
    /// IEEE Computer Society, pp. 421–430. Available online at http://dl.acm.org/citation.cfm?id=645484.656550.
    /// 
    /// Profiling considersations:
    /// - Always use equal when comparins test --> i.e. using a startswith instead of an equal can decrease performance by 10 times
    /// - Write objects from DataReader into an object[] an work with the object. 
    /// - Explicity convert (i.e. (int)reader[0]) value from DataReader and don't use the given methods (i.e. reader.getInt32(0))
    /// </remarks>
    public class BNLTest2
    {
        public long timeInMs { get; set; }
        //For each tuple
        long imoves = 0;
        long iTotalComparisions = 0;


        public DataTable getSkylineTable(String strQuery, String strOperators, int numberOfRecords, bool isIndependent, string strConnection, string strProvider)
        {
            StringBuilder sb = new StringBuilder();

            string[] operators = strOperators.ToString().Split(';');
            var dt = PerformanceTestHelper.GetSkylineDataTable(strQuery, isIndependent, strConnection, strProvider);
            sb.AppendLine("query: " + strQuery);
            sb.AppendLine("isIndependent: " + isIndependent);
            sb.AppendLine("conn: " + strConnection);
            sb.AppendLine("prov: " + strProvider);
            sb.AppendLine("Rows: " + dt.Rows.Count);


            //Sortieren nach der summe der einträge (normalisierte beträge)
            /*dt.Columns.Add("Sort");
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                decimal summe = 0;
                for (int ii = 0; ii < 7; ii++)
                {
                    summe += (decimal)dt.Rows[i][ii];
                }
                dt.Rows[i]["Sort"] = summe;
            }
            //sortieren
            dt.DefaultView.Sort = "Sort DESC";
            dt = dt.DefaultView.ToTable();
            */
            var listObjects = PerformanceTestHelper.fillObjectFromDataReader(dt.CreateDataReader());


            DataTable dtResult = new DataTable();
            SqlDataRecord record = PerformanceTestHelper.buildDataRecord(dt, operators, dtResult);





            return getSkylineTable(listObjects, record, strOperators, numberOfRecords, isIndependent, dtResult);
        }


        private DataTable getSkylineTable(List<object[]> listObjects, SqlDataRecord record, string strOperators, int numberOfRecords, bool isIndependent, DataTable dtResult)
        {
            Stopwatch sw = new Stopwatch();
            //ArrayList resultCollection = new ArrayList();
            List<float[]> resultCollection = new List<float[]>();
            //ArrayList resultstringCollection = new ArrayList();
            string[] operators = strOperators.ToString().Split(';');
            var resultToTupleMapping = PerformanceTestHelper.ResultToTupleMapping(operators);






            try
            {





                int n = listObjects.Count;

                ArrayList floats = new ArrayList();
                for (int i = 0; i < n; i++)
                {
                    float[] test = new float[7];
                    for (int iCol = 0; iCol <= 6; iCol++)
                    {

                        test[iCol] = (float)(decimal)(listObjects[i][iCol]);
                    }
                    floats.Add(test);

                }


                //Time the algorithm needs (afer query to the database)
                sw.Start();


                foreach (float[] dataPoint in floats)
                {
                    bnlOperation(resultCollection, dataPoint);
                }

              


                sw.Stop();
                timeInMs = sw.ElapsedMilliseconds;




            }
            catch (Exception ex)
            {


                //Pack Errormessage in a SQL and return the result
                string strError = "Fehler in SP_SkylineBNL: ";
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

            sw.Stop();
            timeInMs = sw.ElapsedMilliseconds;
            return dtResult;
        }

        private void bnlOperation(List<float[]> resultCollection, float[] dataPoint)
        {
            bool isDominated = false;

            //check if record is dominated (compare against the records in the window)
            //TODO: sehr entscheidend ob man hinten oder vorne anfängt
            for (int i = resultCollection.Count - 1; i >= 0; i--)
            {
                if (isTupleDominated(resultCollection[i], dataPoint, 6))
                {
                    isDominated = true;
                    //break;
                    return;
                }
            }
            if (isDominated == false)
            {
                addToWindow(dataPoint, resultCollection);
            }


        }

        /*
        private void bnlOperation(List<float[]> resultCollection, float[] dataPoint)
        {

            //check if record is dominated (compare against the records in the window)
            //for (int i = resultCollection.Count - 1; i >= 0; i--)
            //for (int i = 0; i <= resultCollection.Count - 1; i++)
            bool isDominated = false;
            for (int i = 0; i <= resultCollection.Count - 1; i++)
            {
                iTotalComparisions++;
                float[] resultCol = resultCollection[i];


                //Variante CLOFI
                bool tupleDominated = isTupleDominated(resultCol, dataPoint, 6);
                if (tupleDominated == true)
                {
                    isDominated = true;
                    break;
                }

            }
            if (isDominated == false)
            {
                addToWindow(dataPoint, resultCollection);
            }
            

        }*/

        /// <summary>
        /// Adds a tuple to the existing window. cannot handle incomparable values
        /// </summary>
        /// <param name="dataReader"></param>
        /// <param name="operators"></param>
        /// <param name="resultCollection"></param>
        /// <param name="record"></param>
        /// <param name="isFrameworkMode"></param>
        /// <param name="dtResult"></param>
        private void addToWindow(float[] dataReader, List<float[]> resultCollection)
        {

            //Erste Spalte ist die ID
            float[] recordInt = new float[7];


            //for (int iCol = 0; iCol < dataReader.FieldCount; iCol++)
            for (int iCol = 0; iCol <= 6; iCol++)
            {
                recordInt[iCol] = dataReader[iCol];
            }



            //dtResult.Rows.Add(row);
            //resultCollection.Insert(0, recordInt);
            resultCollection.Add(recordInt);

        }







        /// <summary>
        /// Compares a tuple against another tuple according to preference logic. Cannot handle incomparable values
        /// Better values are smaller!
        /// </summary>
        /// <param name="dataReader"></param>
        /// <param name="operators"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        private bool isTupleDominated(float[] pointA, float[] pointB, int upperbound)
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol <= upperbound; iCol++)
            {
                //Profiling
                //Use explicit conversion (long)dataReader[iCol] instead of dataReader.GetInt64(iCol) is 20% faster!
                //Use long array instead of dataReader --> is 100% faster!!!
                //long value = dataReader.GetInt64(iCol);
                //long value = (long)dataReader[iCol];
                //long value = tupletoCheck[iCol].Value;
                float value = pointB[iCol]; //.Value;


                int comparison = compareValue(value, pointA[iCol]);

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
            if (greaterThan == true)
                return true;
            else
                return false;

        }



        private static int compareValue(float value1, float value2)
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




    }
}
