using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using Microsoft.SqlServer.Server;

//!!!Caution: Attention small changes in this code can lead to remarkable performance issues!!!!
namespace prefSQL.SQLSkyline
{
    /// <summary>
    /// TODO: Work in progress
    /// </summary>
    /// <remarks>
    /// TODO: Work in progress
    /// </remarks>
    public abstract class TemplateS : TemplateStrategy
    {

        protected override DataTable GetSkylineTable(String strQuery, String strOperators, int numberOfRecords, bool isIndependent, string strConnection, string strProvider)
        {
            Stopwatch sw = new Stopwatch();
            ArrayList resultCollection = new ArrayList();
            ArrayList resultstringCollection = new ArrayList();
            string[] operators = strOperators.Split(';');
            DataTable dtResult = new DataTable();

            DbProviderFactory factory = DbProviderFactories.GetFactory(strProvider);

            // use the factory object to create Data access objects.
            DbConnection connection = factory.CreateConnection();
            if (connection != null)
            {
                connection.ConnectionString = strConnection;

                try
                {
                    //Some checks
                    if (strQuery.Length == Helper.MaxSize)
                    {
                        throw new Exception("Query is too long. Maximum size is " + Helper.MaxSize);
                    }
                    connection.Open();

                    DbDataAdapter dap = factory.CreateDataAdapter();
                    DbCommand selectCommand = connection.CreateCommand();
                    selectCommand.CommandTimeout = 0; //infinite timeout
                    selectCommand.CommandText = strQuery;
                    if (dap != null)
                    {
                        dap.SelectCommand = selectCommand;
                        DataTable dt = new DataTable();
                        dap.Fill(dt);

                        //Time the algorithm needs (afer query to the database)
                        sw.Start();


                        // Build our record schema 
                        List<SqlMetaData> outputColumns = Helper.BuildRecordSchema(dt, operators, dtResult);
                        SqlDataRecord record = new SqlDataRecord(outputColumns.ToArray());

                        List<object[]> listObjects = Helper.GetObjectArrayFromDataTable(dt);

                        foreach (object[] dbValuesObject in listObjects)
                        {
                            //Check if window list is empty
                            if (resultCollection.Count == 0)
                            {
                                // Build our SqlDataRecord and start the results 
                                AddtoWindow(dbValuesObject, operators, resultCollection, resultstringCollection, record, true, dtResult);
                            }
                            else
                            {
                                bool isDominated = false;

                                //check if record is dominated (compare against the records in the window)
                                for (int i = resultCollection.Count - 1; i >= 0; i--)
                                {
                                    if (TupleDomination(resultCollection, resultstringCollection, operators, dtResult, i))
                                    {
                                        isDominated = true;
                                        break;
                                    }
                                }
                                if (isDominated == false)
                                {
                                    AddtoWindow(dbValuesObject, operators, resultCollection, resultstringCollection, record, true, dtResult);
                                }

                            }
                        }


                        //Remove certain amount of rows if query contains TOP Keyword
                        Helper.GetAmountOfTuples(dtResult, numberOfRecords);

                        if (isIndependent == false)
                        {
                            //Send results to client
                            SqlContext.Pipe.SendResultsStart(record);

                            //foreach (SqlDataRecord recSkyline in btg[iItem])
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
                finally
                {
                    if (connection != null)
                        connection.Close();
                }
            }

            sw.Stop();
            TimeInMs = sw.ElapsedMilliseconds;
            return dtResult;
        }

        protected abstract bool TupleDomination(ArrayList resultCollection, ArrayList resultstringCollection, string[] operators, DataTable dtResult, int i);

        protected abstract void AddtoWindow(object[] dataReader, string[] operators, ArrayList resultCollection, ArrayList resultstringCollection, SqlDataRecord record, bool isFrameworkMode, DataTable dtResult);

    }
}
