using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using Microsoft.SqlServer.Server;

namespace prefSQL.SQLSkyline
{
    public abstract class TemplateStrategy
    {
        public int WindowHandling { get; set; }
        public long TimeInMs { get; set; }
        public long NumberOfOperations { get; set; }
        public long NumberOfMoves { get; set; }
        internal List<long[]> SkylineValues { get; set; } //ordering is done in this class. Ordering needs the skyline values
        
        public DataTable GetSkylineTable(String strQuery, String strOperators, int numberOfRecords, bool isIndependent, string strConnection, string strProvider, string[] additionalParameters, int sortType, bool addLevel)
        {
            string[] operators = strOperators.Split(';');
            DataTable dtResult = new DataTable();
            SqlDataRecord record = null;
            List<object[]> listObjects = Helper.GetObjectArrayFromSQLWithLevel(strQuery, strConnection, strProvider, dtResult, operators, out record);
            return GetSkylineTable(listObjects, dtResult, record, strOperators, numberOfRecords, isIndependent, sortType, additionalParameters);
        }

        public DataTable GetSkylineTable(String strQuery, String strOperators, int numberOfRecords, bool isIndependent, string strConnection, string strProvider, string[] additionalParameters, int sortType)
        {
            string[] operators = strOperators.Split(';');
            DataTable dtResult = new DataTable();
            SqlDataRecord record = null;
            List<object[]> listObjects = Helper.GetObjectArrayFromSQL(strQuery, strConnection, strProvider, dtResult, operators, out record);
            return GetSkylineTable(listObjects, dtResult, record, strOperators, numberOfRecords, isIndependent, sortType, additionalParameters);
        }

        /// <summary>
        /// Override this method in specific algorithm
        /// </summary>
        /// <param name="database"></param>
        /// <param name="dataTableTemplate"></param>
        /// <param name="operatorsArray"></param>
        /// <param name="additionalParameters"></param>
        /// <returns></returns>
        protected abstract DataTable GetSkylineFromAlgorithm(IEnumerable<object[]> database, DataTable dataTableTemplate, string[] operatorsArray, string[] additionalParameters);

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
        /// <param name="sortType"></param>
        /// <param name="additionalParameters"></param>
        /// <returns></returns>
        internal DataTable GetSkylineTable(IEnumerable<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool isIndependent, int sortType, string[] additionalParameters)
        {
            string[] operatorsArray = operators.Split(';');
            DataTable dataTableReturn = new DataTable();
            Stopwatch sw = new Stopwatch();

            try
            {
                //Time the algorithm needs (afer query to the database)
                sw.Start();


                //Run the specific algorithm
                dataTableReturn = GetSkylineFromAlgorithm(database, dataTableTemplate, operatorsArray, additionalParameters);
                

                
                //Sort ByRank
                if (sortType == 1)
                {
                    dataTableReturn = Helper.SortBySum(dataTableReturn, SkylineValues);    
                } 
                else if(sortType == 2)
                {
                    dataTableReturn = Helper.SortByRank(dataTableReturn, SkylineValues);
                }

                //Remove certain amount of rows if query contains TOP Keyword
                Helper.GetAmountOfTuples(dataTableReturn, numberOfRecords);


                //Send results if working with the CLR
                if (isIndependent == false)
                {
                    
                    if (SqlContext.Pipe != null)
                    {
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
            }
            catch (Exception ex)
            {
                //Pack Errormessage in a SQL and return the result
                string strError = "Unexpected Error. Msg:";
                strError += ex.Message;
                strError += "\n StackTrace: " + ex.StackTrace;

                if (isIndependent)
                {
                    Debug.WriteLine(strError);
                }
                else
                {
                    if (SqlContext.Pipe != null)
                    {
                        SqlContext.Pipe.Send(strError);
                    }
                }
            }

            //Report time the execution time of the algorithm
            sw.Stop();
            TimeInMs = sw.ElapsedMilliseconds;

            return dataTableReturn;
        }



    }
}