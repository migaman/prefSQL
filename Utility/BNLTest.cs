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
    public class BNLTest
    {
        public long timeInMs { get; set; }
        //For each tuple
        long imoves = 0;
        long iTotalComparisions = 0;


        public DataTable GetSkylineTable(String strQuery, String strOperators, int numberOfRecords, bool isIndependent, string strConnection, string strProvider)
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

            var listObjects = PerformanceTestHelper.fillObjectFromDataReader(dt.CreateDataReader());


            DataTable dtResult = new DataTable();
            SqlDataRecord record = PerformanceTestHelper.buildDataRecord(dt, operators, dtResult);





            return GetSkylineTable(listObjects, record, strOperators, numberOfRecords, isIndependent, dtResult);
        }


        private DataTable GetSkylineTable(List<object[]> listObjects, SqlDataRecord record, string strOperators, int numberOfRecords, bool isIndependent, DataTable dtResult)
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

                        test[iCol] = (float)((decimal)listObjects[i][iCol]);
                    }
                    floats.Add(test);

                }
                

                //float[,] resultCollectionFloat = new float[0, 6];

                //Time the algorithm needs (afer query to the database)
                sw.Start();
                int iIndex = 0;
                foreach (float[] dataPoint in floats)
                {
                    bnlOperation(resultCollection, dataPoint);
                    iIndex++;
                }

                sw.Stop();
                timeInMs = sw.ElapsedMilliseconds;


                //Remove certain amount of rows if query contains TOP Keyword
                PerformanceTestHelper.getAmountOfTuples(dtResult, numberOfRecords);


                //Sort ByRank
                //dtResult = Helper.sortByRank(dtResult, resultCollection);
                //dtResult = Helper.sortBySum(dtResult, resultCollection);

                if (isIndependent == false)
                {
                    //Send results to client
                    SqlContext.Pipe.SendResultsStart(record);

                    foreach (DataRow recSkyline in dtResult.Rows)
                    {
                        for (int i = 0; i < recSkyline.Table.Columns.Count; i++)
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
            
            //check if record is dominated (compare against the records in the window)
            //for (int i = resultCollection.Count - 1; i >= 0; i--)
            //for (int i = 0; i <= resultCollection.Count - 1; i++)
            for (int i = 0; i <= resultCollection.Count - 1; i++)
            {
                iTotalComparisions++;
                float[] resultCol = resultCollection[i];


                //Variante CLOFI
                int tupleDominated = isTupleDominated(resultCol, dataPoint);
                switch (tupleDominated)
                {
                    case 3: //PointRelationship.IS_DOMINATED_BY;
                        {
                            //dominated by sollte im sort nie auftreten
                            tupleDominated = 3;
                            //break;
                            return;
                        }
                    case 2: //PointRelationship.DOMINATES
                        {
                            float[] headNext = resultCollection[0];
                            float[] current = resultCollection[i];
                            if (current != headNext)
                            {
                                //Tupel i an position 0
                                //Tupel 0 an position 1
                                float[] newFirst = current;
                                resultCollection.RemoveAt(i);
                                resultCollection.Insert(0, newFirst);
                                imoves++;
                            }
                            return;
                        }
                }

            }
           addToWindow(dataPoint, resultCollection);

        }

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

            //for (int iCol = 0; iCol <= upperbound; iCol++)
            int iCol = 0;
            while (iCol <= upperbound) //TODO: kann man auch umgekehrt. Performance dann massiv anders
            {
                if (pointB[iCol] < pointA[iCol])
                {
                    //Value is smaller --> return false
                    return true;
                }
                else if (pointB[iCol] > pointA[iCol])
                {
                    //at least one must be greater than
                    greaterThan = true;
                }
                iCol++;
            }


            //all equal and at least one must be greater than
            if (greaterThan == true)
                return false;
            else
                return true;

        }






        
        private int isTupleDominated(float[] pointA, float[] pointB)
        {

            int i = pointA.GetUpperBound(0);
            // check if equal
            while (pointA[i] == pointB[i])
            {
                i--;
                if (i < 0)
                {
                    // this is wrong! should be equal, but algorithms are to stupid to work with that
                    return 0; // PointRelationship.EQUALS;
                }
            }

            if (pointA[i] >= pointB[i])
            {
                while (--i >= 0)
                {
                    if (pointA[i] < pointB[i])
                    {
                        return 1; // PointRelationship.IS_INCOMPARABLE_TO;
                    }
                }
                return 2; // PointRelationship.DOMINATES;
            }
            else
            {
                while (--i >= 0)
                {
                    if (pointA[i] > pointB[i])
                    {
                        return 1; // PointRelationship.IS_INCOMPARABLE_TO;
                    }
                }
                return 3; // PointRelationship.IS_DOMINATED_BY;
            }
        }
        





    }
}
