using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using Microsoft.SqlServer.Server;

namespace prefSQL.SQLSkyline
{
    public abstract class TemplateStrategy
    {
        public long TimeInMs = 0;


        public DataTable GetSkylineTable(String strQuery, String strOperators, int numberOfRecords, bool isIndependent, string strConnection, string strProvider, string[] additionalParameters)
        {
            string[] operators = strOperators.Split(';');
            DataTable dt = Helper.GetSkylineDataTable(strQuery, isIndependent, strConnection, strProvider);
            List<object[]> listObjects = Helper.GetObjectArrayFromDataTable(dt);
            DataTable dtResult = new DataTable();
            SqlDataRecord record = Helper.BuildDataRecord(dt, operators, dtResult);

            return GetSkylineTable(listObjects, dtResult, record, strOperators, numberOfRecords, isIndependent, 0, additionalParameters);
        }

        /// <summary>
        /// Method exists only for the reason that method "GetSkylineTable" can stay private
        /// </summary>
        /// <param name="database"></param>
        /// <param name="dataTableTemplate"></param>
        /// <param name="dataRecordTemplate"></param>
        /// <param name="operators"></param>
        /// <param name="numberOfRecords"></param>
        /// <param name="isIndependent"></param>
        /// <param name="additionalParameters"></param>
        /// <returns></returns>
        public DataTable GetSkylineTableBackdoorSample(List<object[]> database, DataTable dataTableTemplate,
            SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool isIndependent,
            string[] additionalParameters)
        {
            return GetSkylineTable(database, dataTableTemplate, dataRecordTemplate, operators, numberOfRecords, isIndependent, 0, additionalParameters);
        }


        /// <summary>
        /// Override this method in specific algorithm
        /// </summary>
        /// <param name="database"></param>
        /// <param name="dataTableTemplate"></param>
        /// <param name="dataRecordTemplate"></param>
        /// <param name="operatorsArray"></param>
        /// <param name="additionalParameters"></param>
        /// <returns></returns>
        protected abstract DataTable GetSkylineFromAlgorithm(List<object[]> database, DataTable dataTableTemplate,
            SqlDataRecord dataRecordTemplate, string[] operatorsArray, string[] additionalParameters);

        /// <summary>
        /// Template function for each algorithm
        /// Performance measurements, CLR, sorting, and so on are handled here
        /// </summary>
        /// <param name="database"></param>
        /// <param name="dataTableTemplate"></param>
        /// <param name="dataRecordTemplate"></param>
        /// <param name="operators"></param>
        /// <param name="numberOfRecords"></param>
        /// <param name="isIndependent"></param>
        /// <param name="additionalParameters"></param>
        /// <returns></returns>
        private DataTable GetSkylineTable(List<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool isIndependent, int sortType, string[] additionalParameters)
        {
            ArrayList resultCollection = new ArrayList();
            ArrayList resultstringCollection = new ArrayList();
            string[] operatorsArray = operators.Split(';');
            int[] resultToTupleMapping = Helper.ResultToTupleMapping(operatorsArray);
            DataTable dataTableReturn = new DataTable();
            Stopwatch sw = new Stopwatch();

            try
            {
                //Time the algorithm needs (afer query to the database)
                sw.Start();

                //Run the specific algorithm
                dataTableReturn = GetSkylineFromAlgorithm(database, dataTableTemplate, dataRecordTemplate, operatorsArray, additionalParameters);
                

                //Remove certain amount of rows if query contains TOP Keyword
                Helper.GetAmountOfTuples(dataTableReturn, numberOfRecords);


                //Sort ByRank
                if (sortType == 1)
                {
                    dataTableReturn = Helper.SortByRank(dataTableReturn, resultCollection);
                } 
                else if(sortType == 2)
                {
                    dataTableReturn = Helper.SortBySum(dataTableReturn, resultCollection);    
                }
                
                

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



    }
}