using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;


//Caution: Attention small changes in this code can lead to performance issues, i.e. using a startswith instead of an equal can increase by 10 times
//Important: Only use equal for comparing text (otherwise performance issues)
namespace prefSQL.SQLSkyline
{
    public class SP_MultipleSkylineBNLLevel
    {
        /// <summary>
        /// Calculate the skyline points from a dataset
        /// </summary>
        /// <param name="strQuery"></param>
        /// <param name="strOperators"></param>
        [Microsoft.SqlServer.Server.SqlProcedure(Name = "SP_MultipleSkylineBNLLevel")]
        public static void getSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords, SqlInt32 upToLevel)
        {
            int up = upToLevel.Value;
            SP_MultipleSkylineBNLLevel skyline = new SP_MultipleSkylineBNLLevel();
            skyline.getSkylineTable(strQuery.ToString(), strOperators.ToString(), numberOfRecords.Value, false, Helper.cnnStringSQLCLR, Helper.ProviderCLR, up);

        }


        public DataTable getSkylineTable(string strQuery, string strOperators, string strConnection, string strProvider, int numberOfRecords, int upToLevel)
        {
            return getSkylineTable(strQuery, strOperators, numberOfRecords, true, strConnection, strProvider, upToLevel);
        }


        private DataTable getSkylineTable(String strQuery, String strOperators, int numberOfRecords, bool isIndependent, string strConnection, string strProvider, int upToLevel)
        {
            ArrayList resultCollection = new ArrayList();
            string[] operators = strOperators.ToString().Split(';');
            DataTable dtResult = new DataTable();

            DbProviderFactory factory = null;
            DbConnection connection = null;
            factory = DbProviderFactories.GetFactory(strProvider);

            // use the factory object to create Data access objects.
            connection = factory.CreateConnection(); // will return the connection object (i.e. SqlConnection ...)
            connection.ConnectionString = strConnection;

            try
            {
                //Some checks
                if (strQuery.ToString().Length == Helper.MaxSize)
                {
                    throw new Exception("Query is too long. Maximum size is " + Helper.MaxSize);
                }
                connection.Open();

                DbDataAdapter dap = factory.CreateDataAdapter();
                DbCommand selectCommand = connection.CreateCommand();
                selectCommand.CommandTimeout = 0; //infinite timeout
                selectCommand.CommandText = strQuery.ToString();
                dap.SelectCommand = selectCommand;
                DataTable dt = new DataTable();
                dap.Fill(dt);


                //trees erstellen mit n nodes (n = anzahl tupels)
                int[] graph = new int[dt.Rows.Count];
                //int[] levels = new int[dt.Rows.Count];
                List<int> levels = new List<int>();
                int[,] values = new int[dt.Rows.Count, operators.GetUpperBound(0)];


                // Build our record schema 
                List<SqlMetaData> outputColumns = Helper.buildRecordSchema(dt, operators, dtResult);
                //Add Level column
                SqlMetaData OutputColumnLevel = new SqlMetaData("Level", SqlDbType.Int);
                outputColumns.Add(OutputColumnLevel);
                dtResult.Columns.Add("level", typeof(int));
                SqlDataRecord record = new SqlDataRecord(outputColumns.ToArray());
                if (isIndependent == false)
                {
                    SqlContext.Pipe.SendResultsStart(record);
                }

                int iMaxLevel = 0;
                //Read all records only once. (SqlDataReader works forward only!!)
                DataTableReader dataTableReader = dt.CreateDataReader();

                //Write all attributes to a Object-Array
                //Profiling: This is much faster (factor 2) than working with the SQLReader
                List<object[]> listObjects = Helper.fillObjectFromDataReader(dataTableReader);
                


                foreach (object[] dbValuesObject in listObjects)
                {

                    //Check if window list is empty
                    if (resultCollection.Count == 0)
                    {
                        // Build our SqlDataRecord and start the results 
                        levels.Add(0);
                        iMaxLevel = 0;
                        addToWindow(dbValuesObject, operators, ref resultCollection, record, isIndependent, levels[levels.Count - 1], ref dtResult);
                    }
                    else
                    {

                        //Insert the new record to the tree
                        bool bFound = false;

                        //Start wie level 0 nodes (until uptolevels or maximum levels)
                        for (int iLevel = 0; iLevel <= iMaxLevel && bFound == false && iLevel < upToLevel; iLevel++)
                        {
                            bool isDominated = false;
                            for (int i = 0; i < resultCollection.Count; i++)
                            {
                                if (levels[i] == iLevel)
                                {
                                    long[] result = (long[])resultCollection[i];

                                    //Dominanz
                                    if (Helper.isTupleDominated(result, dbValuesObject) == true)
                                    {
                                        //Dominated in this level. Next level
                                        isDominated = true;
                                        break;
                                    }
                                }
                            }
                            //Check if the record is dominated in this level
                            if (isDominated == false)
                            {
                                levels.Add(iLevel);
                                bFound = true;
                                break;
                            }
                        }
                        if (bFound == false)
                        {
                            iMaxLevel++;
                            if (iMaxLevel < upToLevel)
                            {
                                levels.Add(iMaxLevel);
                                addToWindow(dbValuesObject, operators, ref resultCollection, record, isIndependent, levels[levels.Count - 1], ref dtResult);
                            }
                        }
                        else
                        {
                            addToWindow(dbValuesObject, operators, ref resultCollection, record, isIndependent, levels[levels.Count - 1], ref dtResult);
                        }
                    }
                }


                if (isIndependent == false)
                {
                    SqlContext.Pipe.SendResultsEnd();
                }


            }
            catch (Exception ex)
            {
                //Pack Errormessage in a SQL and return the result
                string strError = "Fehler in SP_MultipleSkylineBNL: ";
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
            return dtResult;
        }


        private void addToWindow(object[] dataReader, string[] operators, ref ArrayList resultCollection, SqlDataRecord record, SqlBoolean isFrameworkMode, int level, ref DataTable dtResult)
        {

            //Erste Spalte ist die ID
            long[] recordInt = new long[operators.GetUpperBound(0) + 1];
            DataRow row = dtResult.NewRow();

            for (int iCol = 0; iCol <= dataReader.GetUpperBound(0); iCol++)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol <= operators.GetUpperBound(0))
                {
                    recordInt[iCol] = (long)dataReader[iCol];
                }
                else
                {
                    row[iCol - (operators.GetUpperBound(0) + 1)] = dataReader[iCol];
                    record.SetValue(iCol - (operators.GetUpperBound(0) + 1), dataReader[iCol]);
                }
            }
            row[record.FieldCount - 1] = level;
            record.SetValue(record.FieldCount - 1, level);

            if (isFrameworkMode == true)
            {
                dtResult.Rows.Add(row);
            }
            else
            {
                SqlContext.Pipe.SendResultsRow(record);
            }
            resultCollection.Add(recordInt);
        }




    }
}