using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;

//Caution: Attention small changes in this code can lead to performance issues, i.e. using a startswith instead of an equal can increase by 10 times
//Important: Only use equal for comparing text (otherwise performance issues)
namespace prefSQL.SQLSkyline
{
    public abstract class TemplateBNL
    {
        public DataTable UseDataTable { private get; set; }

        public long timeInMs = 0;

        public DataTable getSkylineTable(String strQuery, String strOperators, int numberOfRecords, String strConnection)
        {
            return getSkylineTable(strQuery, strOperators, numberOfRecords, true, strConnection);
        }

        protected DataTable getSkylineTable(String strQuery, String strOperators, int numberOfRecords, bool isIndependent, string strConnection)
        {
            Stopwatch sw = new Stopwatch();
            ArrayList resultCollection = new ArrayList();
            ArrayList resultstringCollection = new ArrayList();
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

                if (UseDataTable != null)
                {
                    var cmd = new SqlCommand
                    {
                        CommandText = "select top 0 * into #mytemptable from cars_small",
                        CommandType = CommandType.Text,
                        Connection = connection
                    };                 
                    cmd.ExecuteNonQuery();
                    using (var bulkCopy = new SqlBulkCopy(connection))
                    {
                        bulkCopy.DestinationTableName = "#mytemptable";
                        bulkCopy.WriteToServer(UseDataTable);
                    }
                }

                SqlDataAdapter dap = new SqlDataAdapter(strQuery.ToString(), connection);
                DataTable dt = new DataTable();              
                dap.Fill(dt);


                //Time the algorithm needs (afer query to the database)
                sw.Start();


                // Build our record schema 
                List<SqlMetaData> outputColumns = Helper.buildRecordSchema(dt, operators, dtResult);
                SqlDataRecord record = new SqlDataRecord(outputColumns.ToArray());


                //Read all records only once. (SqlDataReader works forward only!!)
                DataTableReader dataTableReader = dt.CreateDataReader();
                
                //Write all attributes to a Object-Array
                //Profiling: This is much faster (factor 2) than working with the SQLReader
                List<object[]> listObjects = Helper.fillObjectFromDataReader(dataTableReader);

                //For each tuple
                foreach (object[] dbValuesObject in listObjects)
                {

                    //Check if window list is empty
                    if (resultCollection.Count == 0)
                    {
                        // Build our SqlDataRecord and start the results 
                        addtoWindow(dbValuesObject, operators, resultCollection, resultstringCollection, record, true, dtResult);
                    }
                    else
                    {
                        bool isDominated = false;

                        //check if record is dominated (compare against the records in the window)
                        for (int i = resultCollection.Count - 1; i >= 0; i--)
                        {
                            if (tupleDomination(dbValuesObject, resultCollection, resultstringCollection, operators, dtResult, i) == true)
                            {
                                isDominated = true;
                                break;
                            }
                        }
                        if (isDominated == false)
                        {
                            addtoWindow(dbValuesObject, operators, resultCollection, resultstringCollection, record, true, dtResult);
                        }

                    }
                }

                

                //Remove certain amount of rows if query contains TOP Keyword
                Helper.getAmountOfTuples(dtResult, numberOfRecords);

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
            finally
            {
                if (connection != null)
                    connection.Close();
            }

            sw.Stop();
            timeInMs = sw.ElapsedMilliseconds;
            return dtResult;
        }


        protected abstract bool tupleDomination(object[] dataReader, ArrayList resultCollection, ArrayList resultstringCollection, string[] operators, DataTable dtResult, int i);

        protected abstract void addtoWindow(object[] dataReader, string[] operators, ArrayList resultCollection, ArrayList resultstringCollection, SqlDataRecord record, bool isFrameworkMode, DataTable dtResult);

    }
}
