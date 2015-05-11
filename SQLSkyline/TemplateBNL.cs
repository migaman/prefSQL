using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using Microsoft.SqlServer.Server;

//!!!Caution: Attention small changes in this code can lead to remarkable performance issues!!!!
namespace prefSQL.SQLSkyline
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
    public abstract class TemplateBNL : TemplateStrategy
    {
        protected override DataTable GetSkylineTable(String strQuery, String strOperators, int numberOfRecords,
            bool isIndependent, string strConnection, string strProvider)
        {
            string[] operators = strOperators.Split(';');
            DataTable dt = Helper.GetSkylineDataTable(strQuery, isIndependent, strConnection, strProvider);
            List<object[]> listObjects = Helper.GetObjectArrayFromDataTable(dt);
            DataTable dtResult = new DataTable();
            SqlDataRecord record = Helper.BuildDataRecord(dt, operators, dtResult);

            return GetSkylineTable(listObjects, dtResult, record, strOperators, numberOfRecords, isIndependent);
        }

        public DataTable GetSkylineTable(List<object[]> database, SqlDataRecord dataRecordTemplate, string operators,
            int numberOfRecords, DataTable dataTableTemplate)
        {
            return GetSkylineTable(database, dataTableTemplate, dataRecordTemplate, operators, numberOfRecords, true);
        }

        protected override DataTable GetSkylineTable(List<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool isIndependent)
        {
            DataTable dataTableReturn = dataTableTemplate.Clone();

            Stopwatch sw = new Stopwatch();
            ArrayList resultCollection = new ArrayList();
            ArrayList resultstringCollection = new ArrayList();
            string[] operatorsArray = operators.Split(';');
            int[] resultToTupleMapping = Helper.ResultToTupleMapping(operatorsArray);
        
            try
            {
                //Time the algorithm needs (afer query to the database)
                sw.Start();

                //For each tuple
                foreach (object[] dbValuesObject in database)
                {

                    //Check if window list is empty
                    if (resultCollection.Count == 0)
                    {
                        // Build our SqlDataRecord and start the results 
                        AddtoWindow(dbValuesObject, operatorsArray, resultCollection, resultstringCollection, dataRecordTemplate, true, dataTableReturn);
                    }
                    else
                    {
                        bool isDominated = false;

                        //check if record is dominated (compare against the records in the window)
                        for (int i = resultCollection.Count - 1; i >= 0; i--)
                        {
                            if (TupleDomination(dbValuesObject, resultCollection, resultstringCollection, operatorsArray, dataTableReturn, i, resultToTupleMapping))
                            {
                                isDominated = true;
                                break;
                            }
                        }
                        if (isDominated == false)
                        {
                            AddtoWindow(dbValuesObject, operatorsArray, resultCollection, resultstringCollection, dataRecordTemplate, true, dataTableReturn);
                        }

                    }
                }

                //Remove certain amount of rows if query contains TOP Keyword
                Helper.GetAmountOfTuples(dataTableReturn, numberOfRecords);

                
                //Sort ByRank
                //dtResult = Helper.sortByRank(dtResult, resultCollection);
                //dtResult = Helper.sortBySum(dtResult, resultCollection);

                if (isIndependent == false)
                {
                    //Send results to client
                    SqlContext.Pipe.SendResultsStart(dataRecordTemplate);

                    foreach (DataRow recSkyline in dataTableReturn.Rows)
                    {
                        for (int i = 0; i < recSkyline.Table.Columns.Count; i++)
                        {
                            dataRecordTemplate.SetValue(i, recSkyline[i]);
                        }
                        SqlContext.Pipe.SendResultsRow(dataRecordTemplate);
                    }
                    SqlContext.Pipe.SendResultsEnd();
                }
            }
            catch (Exception ex)
            {
                //Pack Errormessage in a SQL and return the result
                string strError = "Fehler in SP_SkylineBNL: ";
                strError += ex.Message;

                if (isIndependent)
                {
                    Debug.WriteLine(strError);
                }
                else
                {
                    SqlContext.Pipe.Send(strError);
                }
            }
         
            sw.Stop();
            TimeInMs = sw.ElapsedMilliseconds;
            return dataTableReturn;
        }



        protected abstract bool TupleDomination(object[] dataReader, ArrayList resultCollection, ArrayList resultstringCollection, string[] operators, DataTable dtResult, int i, int[] resultToTupleMapping);

        protected abstract void AddtoWindow(object[] dataReader, string[] operators, ArrayList resultCollection, ArrayList resultstringCollection, SqlDataRecord record, bool isFrameworkMode, DataTable dtResult);

    }
}
