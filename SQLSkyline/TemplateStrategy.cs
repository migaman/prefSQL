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

            return GetSkylineTable(listObjects, dtResult, record, strOperators, numberOfRecords, isIndependent, additionalParameters);
        }


        public DataTable GetSkylineTableIndependent(string strQuery, string strOperators, int numberOfRecords, string strConnection, string strProvider, string[] additionalParameters)
        {
            string[] operators = strOperators.Split(';');
            DataTable dt = Helper.GetSkylineDataTable(strQuery, true, strConnection, strProvider);
            List<object[]> listObjects = Helper.GetObjectArrayFromDataTable(dt);
            DataTable dtResult = new DataTable();
            SqlDataRecord record = Helper.BuildDataRecord(dt, operators, dtResult);

            return GetSkylineTable(listObjects, dtResult, record, strOperators, numberOfRecords, true, additionalParameters);

            //return GetSkylineTable(strQuery, strOperators, numberOfRecords, true, strConnection, strProvider);
        }

        //protected abstract DataTable GetSkylineTable(String strQuery, String strOperators, int numberOfRecords, bool isIndependent, string strConnection, string strProvider);

        protected abstract DataTable GetCompleteSkylineTable(List<object[]> database, DataTable dataTableTemplate,
            SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool isIndependent, string[] additionalParameters);

        public DataTable GetSkylineTable(List<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string operators, int numberOfRecords, bool isIndependent, string[] additionalParameters)
        {
            //DataTable dataTableReturn = dataTableTemplate.Clone();

            Stopwatch sw = new Stopwatch();
            ArrayList resultCollection = new ArrayList();
            ArrayList resultstringCollection = new ArrayList();
            string[] operatorsArray = operators.Split(';');
            int[] resultToTupleMapping = Helper.ResultToTupleMapping(operatorsArray);
            DataTable dataTableReturn = new DataTable();
            try
            {
                //Time the algorithm needs (afer query to the database)
                sw.Start();

                //Run the specific algorithm
                dataTableReturn = GetCompleteSkylineTable(database, dataTableTemplate, dataRecordTemplate, operators, numberOfRecords, isIndependent, additionalParameters);
                

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



    }
}