using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Diagnostics;
using Microsoft.SqlServer.Server;

//Caution: Attention small changes in this code can lead to performance issues, i.e. using a startswith instead of an equal can increase by 10 times
//Important: Only use equal for comparing text (otherwise performance issues)
namespace prefSQL.SQLSkyline
{
    public class SPMultipleSkylineBNLLevel
    {
        /// <summary>
        /// Calculate the skyline points from a dataset
        /// </summary>
        /// <param name="strQuery"></param>
        /// <param name="strOperators"></param>
        /// <param name="numberOfRecords"></param>
        /// <param name="upToLevel"></param>
        [SqlProcedure(Name = "SP_MultipleSkylineBNLLevel")]
        public static void GetSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords, SqlInt32 upToLevel)
        {
            int up = upToLevel.Value;
            SPMultipleSkylineBNLLevel skyline = new SPMultipleSkylineBNLLevel();
            skyline.GetSkylineTable(strQuery.ToString(), strOperators.ToString(), numberOfRecords.Value, false, Helper.CnnStringSqlclr, Helper.ProviderClr, up);

        }


        public DataTable GetSkylineTable(string strQuery, string strOperators, string strConnection, string strProvider, int numberOfRecords, int upToLevel)
        {
            return GetSkylineTable(strQuery, strOperators, numberOfRecords, true, strConnection, strProvider, upToLevel);
        }


        private DataTable GetSkylineTable(String strQuery, String strOperators, int numberOfRecords, bool isIndependent, string strConnection, string strProvider, int upToLevel)
        {
            ArrayList resultCollection = new ArrayList();
            string[] operators = strOperators.Split(';');
            DataTable dtResult = new DataTable();
            int[] resultToTupleMapping = Helper.ResultToTupleMapping(operators);

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


                        //trees erstellen mit n nodes (n = anzahl tupels)
                        //int[] levels = new int[dt.Rows.Count];
                        List<int> levels = new List<int>();


                        // Build our record schema 
                        List<SqlMetaData> outputColumns = Helper.BuildRecordSchema(dt, operators, dtResult);
                        //Add Level column
                        SqlMetaData outputColumnLevel = new SqlMetaData("Level", SqlDbType.Int);
                        outputColumns.Add(outputColumnLevel);
                        dtResult.Columns.Add("level", typeof(int));
                        SqlDataRecord record = new SqlDataRecord(outputColumns.ToArray());
                        if (isIndependent == false)
                        {
                            SqlContext.Pipe.SendResultsStart(record);
                        }

                        int iMaxLevel = 0;
                
                        List<object[]> listObjects = Helper.GetObjectArrayFromDataTable(dt);

                        foreach (object[] dbValuesObject in listObjects)
                        {

                            //Check if window list is empty
                            if (resultCollection.Count == 0)
                            {
                                // Build our SqlDataRecord and start the results 
                                levels.Add(0);
                                iMaxLevel = 0;
                                AddToWindow(dbValuesObject, operators, ref resultCollection, record, isIndependent, levels[levels.Count - 1], ref dtResult);
                            }
                            else
                            {

                                //Insert the new record to the tree
                                bool bFound = false;

                                //Start wie level 0 nodes (until uptolevels or maximum levels)
                                for (int iLevel = 0; iLevel <= iMaxLevel && iLevel < upToLevel; iLevel++)
                                {
                                    bool isDominated = false;
                                    for (int i = 0; i < resultCollection.Count; i++)
                                    {
                                        if (levels[i] == iLevel)
                                        {
                                            long[] result = (long[])resultCollection[i];

                                            //Dominanz
                                            if (Helper.IsTupleDominated(result, dbValuesObject, resultToTupleMapping))
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
                                        AddToWindow(dbValuesObject, operators, ref resultCollection, record, isIndependent, levels[levels.Count - 1], ref dtResult);
                                    }
                                }
                                else
                                {
                                    AddToWindow(dbValuesObject, operators, ref resultCollection, record, isIndependent, levels[levels.Count - 1], ref dtResult);
                                }
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
                    connection.Close();
                }
            }
            return dtResult;
        }


        private void AddToWindow(object[] dataReader, string[] operators, ref ArrayList resultCollection, SqlDataRecord record, SqlBoolean isFrameworkMode, int level, ref DataTable dtResult)
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