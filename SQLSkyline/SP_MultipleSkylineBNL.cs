using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;
using System.Collections.Generic;


//Caution: Attention small changes in this code can lead to performance issues, i.e. using a startswith instead of an equal can increase by 10 times
//Important: Only use equal for comparing text (otherwise performance issues)
namespace prefSQL.SQLSkyline
{
    public class SP_MultipleSkylineBNL
    {
        /// <summary>
        /// Calculate the skyline points from a dataset
        /// </summary>
        /// <param name="strQuery"></param>
        /// <param name="strOperators"></param>
        [Microsoft.SqlServer.Server.SqlProcedure(Name = "SP_MultipleSkylineBNL")]
        public static void getSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords, SqlInt32 upToLevel)
        {
            int up = upToLevel.Value;
            SP_MultipleSkylineBNL skyline = new SP_MultipleSkylineBNL();
            skyline.getSkylineTable(strQuery.ToString(), strOperators.ToString(), numberOfRecords.Value, false, "", up);

        }


        public DataTable getSkylineTable(String strQuery, String strOperators, String strConnection, int numberOfRecords, int upToLevel)
        {
            return getSkylineTable(strQuery, strOperators, numberOfRecords, true, strConnection, upToLevel);
        }

        public DataTable getSkylineTable(DataTable dataTable, String strOperators, int numberOfRecords, int upToLevel)
        {
            return getSkylineTable(dataTable, strOperators, numberOfRecords, true, upToLevel);
        }

        private DataTable getSkylineTable(DataTable dt, String strOperators, int numberOfRecords, bool isIndependent, int upToLevel)
        {
            ArrayList resultCollection = new ArrayList();
            ArrayList resultstringCollection = new ArrayList();
            string[] operators = strOperators.ToString().Split(';');
            DataTable dtResult = new DataTable();
        
            try
            {
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
                List<object[]> listObjects = Helper.GetObjectArrayFromDataTable(dt);
                


                foreach (object[] dbValuesObject in listObjects)
                {
                    //Check if window list is empty
                    if (resultCollection.Count == 0)
                    {
                        // Build our SqlDataRecord and start the results 
                        levels.Add(0);
                        iMaxLevel = 0;
                        addToWindow(dbValuesObject, operators, resultCollection, resultstringCollection, record, isIndependent, levels[levels.Count - 1], dtResult);
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
                                    long?[] result = (long?[])resultCollection[i];
                                    string[] strResult = (string[])resultstringCollection[i];

                                    //Dominanz
                                    if (Helper.isTupleDominated(operators, result, strResult, dbValuesObject) == true)
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
                                addToWindow(dbValuesObject, operators, resultCollection, resultstringCollection, record, isIndependent, levels[levels.Count - 1], dtResult);
                            }
                        }
                        else
                        {
                            addToWindow(dbValuesObject, operators, resultCollection, resultstringCollection, record, isIndependent, levels[levels.Count - 1], dtResult);
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
          
            return dtResult;
        }

        private DataTable getSkylineTable(String strQuery, String strOperators, int numberOfRecords, bool isIndependent, string strConnection, int upToLevel)
        {         
            SqlConnection connection = null;
            if (isIndependent == false)
                connection = new SqlConnection(Helper.cnnStringSQLCLR);
            else
                connection = new SqlConnection(strConnection);

            DataTable dt = new DataTable();

            try
            {
                //Some checks
                if (strQuery.ToString().Length == Helper.MaxSize)
                {
                    throw new Exception("Query is too long. Maximum size is " + Helper.MaxSize);
                }
                connection.Open();

                SqlDataAdapter dap = new SqlDataAdapter(strQuery.ToString(), connection);
                dap.Fill(dt);
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
                connection.Close();
            }

            return getSkylineTable(dt, strOperators, numberOfRecords, isIndependent, upToLevel);
        }

        private void addToWindow(object[] dataReader, string[] operators, ArrayList resultCollection, ArrayList resultstringCollection, SqlDataRecord record, SqlBoolean isFrameworkMode, int level, DataTable dtResult)
        {

            //Erste Spalte ist die ID
            long?[] recordInt = new long?[operators.GetUpperBound(0) + 1];
            string[] recordstring = new string[operators.GetUpperBound(0) + 1];
            DataRow row = dtResult.NewRow();

            for (int iCol = 0; iCol <= dataReader.GetUpperBound(0); iCol++)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol <= operators.GetUpperBound(0))
                {
                    //LOW und HIGH Spalte in record abfüllen
                    if (operators[iCol].Equals("LOW"))
                    {
                        if (dataReader[iCol] == DBNull.Value)
                            recordInt[iCol] = null;
                        else
                            recordInt[iCol] = (long)dataReader[iCol];
                        
                        //Check if long value is incomparable
                        if (iCol + 1 <= recordInt.GetUpperBound(0) && operators[iCol + 1].Equals("INCOMPARABLE"))
                        {
                            //Incomparable field is always the next one
                            recordstring[iCol] = (string)dataReader[iCol + 1];
                        }
                    }

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
            resultstringCollection.Add(recordstring);
        }




    }
}