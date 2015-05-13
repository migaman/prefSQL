using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
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
    public class BNLTest3
    {
        public long TimeInMs { get; set; }

        public long Moves { get; set; }

        public long TotalComparisions { get; set; }

        //For each tuple


        public DataTable GetSkylineTable(String strQuery, String strOperators, int numberOfRecords, bool isIndependent, string strConnection, string strProvider)
        {
            StringBuilder sb = new StringBuilder();

            DataTable dt = PerformanceTestHelper.GetDataTableFromSQL(strQuery, strConnection, strProvider);
            sb.AppendLine("query: " + strQuery);
            sb.AppendLine("isIndependent: " + isIndependent);
            sb.AppendLine("conn: " + strConnection);
            sb.AppendLine("prov: " + strProvider);
            sb.AppendLine("Rows: " + dt.Rows.Count);


            //Sortieren nach der summe der einträge (normalisierte beträge)
            dt.Columns.Add("Sort");
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

            List<object[]> listObjects = PerformanceTestHelper.FillObjectFromDataReader(dt.CreateDataReader());


            DataTable dtResult = new DataTable();
            


            return GetSkylineTable(listObjects, numberOfRecords, dtResult);
        }


        private DataTable GetSkylineTable(List<object[]> listObjects, int numberOfRecords, DataTable dtResult)
        {
            Stopwatch sw = new Stopwatch();
            //ArrayList resultCollection = new ArrayList();
            List<float[]> resultCollection = new List<float[]>();
            //ArrayList resultstringCollection = new ArrayList();
           
            


            try
            {





                int n = listObjects.Count;

                ArrayList floats = new ArrayList();
                for (int i = 0; i < n; i++)
                {
                    float[] test = new float[7];
                    for (int iCol = 0; iCol <= 6; iCol++)
                    {

                        test[iCol] = (float)((decimal)listObjects[i][iCol]);
                    }
                    floats.Add(test);

                }

                
                sw.Start();
                
                foreach (float[] dataPoint in floats)
                {
                    //Je höher der wert je besser
                    BNLOperation(resultCollection, dataPoint);
                
                }

                sw.Stop();
                TimeInMs = sw.ElapsedMilliseconds;



                //Remove certain amount of rows if query contains TOP Keyword
                PerformanceTestHelper.GetAmountOfTuples(dtResult, numberOfRecords);


                //Sort ByRank
                //dtResult = Helper.sortByRank(dtResult, resultCollection);
                //dtResult = Helper.sortBySum(dtResult, resultCollection);

                
            }
            catch (Exception ex)
            {


                //Pack Errormessage in a SQL and return the result
                string strError = "Fehler in SP_SkylineBNL: ";
                strError += ex.Message;

                Debug.WriteLine(strError);
            }

            sw.Stop();
            TimeInMs = sw.ElapsedMilliseconds;
            return dtResult;
        }


        private void BNLOperation(List<float[]> resultCollection, float[] dataPoint)
        {

            //check if record is dominated (compare against the records in the window)
            //for (int i = resultCollection.Count - 1; i >= 0; i--)
            //for (int i = 0; i <= resultCollection.Count - 1; i++)
            for (int i = 0; i <= resultCollection.Count - 1; i++)
            {
                TotalComparisions++;
                float[] resultCol = resultCollection[i];

                //je höher der wert je besser
                bool tupleDominated = IsTupleDominated(dataPoint, resultCol , 6);
                if (tupleDominated)
                {
                    //Tuple wird dominiert --> Denjenigen tupel der dominant ist zuvorderst in die liste setzen!!
                    float[] headNext = resultCollection[0];
                    float[] current = resultCollection[i];
                    if (current != headNext)
                    {
                        //Tupel i an position 0
                        //Tupel 0 an position 1
                        float[] newFirst = current;
                        resultCollection.RemoveAt(i);
                        resultCollection.Insert(0, newFirst);
                        Moves++;
                    }
                    return;
                }

            }
            AddToWindow(dataPoint, resultCollection);

        }

        /// <summary>
        /// Adds a tuple to the existing window. cannot handle incomparable values
        /// </summary>
        /// <param name="dataReader"></param>
        /// <param name="resultCollection"></param>
        private void AddToWindow(float[] dataReader, List<float[]> resultCollection)
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
        /// <returns></returns>
        private bool IsTupleDominated(float[] pointA, float[] pointB, int upperbound)
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
                float value = pointA[iCol]; //.Value;


                int comparison = CompareValue(value, pointB[iCol]);

                if (comparison <= 1)
                {
                    if (comparison == 0)
                    {
                        //at least one must be smaller than
                        greaterThan = true;
                    }
                }
                else
                {
                    //Value is better --> return false
                    return false;
                }
            }


            //all equal and at least one must be greater than
            if (greaterThan)
                return true;
            else
                return false; //werte sind genau gleich

        }




        private static int CompareValue(float value1, float value2)
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
