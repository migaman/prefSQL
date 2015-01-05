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
        public static void getSkyline(SqlString strQuery, SqlString strOperators, SqlBoolean isDebug, SqlInt32 upToLevel)
        {
            ArrayList resultCollection = new ArrayList();
            string[] operators = strOperators.ToString().Split(';');

            SqlConnection connection = null;
            if (isDebug == false)
                connection = new SqlConnection(Helper.cnnStringSQLCLR);
            else
                connection = new SqlConnection(Helper.cnnStringLocalhost);

            try
            {
                //Some checks
                if (strQuery.ToString().Length == Helper.MaxSize)
                {
                    throw new Exception("Query is too long. Maximum size is " + Helper.MaxSize);
                }
                connection.Open();

                SqlDataAdapter dap = new SqlDataAdapter(strQuery.ToString(), connection);
                DataTable dt = new DataTable();
                dap.Fill(dt);


                //trees erstellen mit n nodes (n = anzahl tupels)
                int[] graph = new int[dt.Rows.Count];
                //int[] levels = new int[dt.Rows.Count];
                List<int> levels = new List<int>();
                int[,] values = new int[dt.Rows.Count, operators.GetUpperBound(0)];


                // Build our record schema 
                List<SqlMetaData> outputColumns = Helper.buildRecordSchema(dt, operators);
                //Add Level column
                SqlMetaData OutputColumnLevel = new SqlMetaData("Level", SqlDbType.Int);
                outputColumns.Add(OutputColumnLevel);

                SqlDataRecord record = new SqlDataRecord(outputColumns.ToArray());
                if (isDebug == false)
                {
                    SqlContext.Pipe.SendResultsStart(record);
                }


                DataTableReader sqlReader = dt.CreateDataReader();


                int iIndex = 0;
                int iMaxLevel = 0;
                //Read all records only once. (SqlDataReader works forward only!!)
                while (sqlReader.Read())
                {
                    //Check if window list is empty
                    if (resultCollection.Count == 0)
                    {
                        // Build our SqlDataRecord and start the results 
                        levels.Add(0);
                        //levels[iIndex] = 0; //root level
                        iMaxLevel = 0;
                        addToWindow(sqlReader, operators, ref resultCollection, record, isDebug, levels[levels.Count - 1]);
                    }
                    else
                    {

                        //Insert the new record to the tree
                        bool bFound = false;

                        //Start wie level 0 nodes (until uptolevels or maximum levels)
                        for (int iLevel = 0; iLevel <= iMaxLevel && bFound == false && iLevel < upToLevel; iLevel++)
                        {
                            bool bDominated = false;
                            for (int i = 0; i < resultCollection.Count; i++)
                            {
                                if (levels[i] == iLevel)
                                {
                                    long[] result = (long[])resultCollection[i];

                                    //Dominanz
                                    if (compare(sqlReader, operators, result) == true)
                                    {
                                        //Dominated in this level. Next level
                                        bDominated = true;
                                        break;
                                    }
                                }
                            }
                            //Check if the record is dominated in this level
                            if (bDominated == false)
                            {
                                //levels[iIndex] = iLevel;
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
                                addToWindow(sqlReader, operators, ref resultCollection, record, isDebug, levels[levels.Count - 1]);
                            }
                        }
                        else
                        {
                            addToWindow(sqlReader, operators, ref resultCollection, record, isDebug, levels[levels.Count - 1]);
                        }
                    }
                    iIndex++;
                }

                sqlReader.Close();

                if (isDebug == true)
                {
                    System.Diagnostics.Debug.WriteLine(resultCollection.Count);
                }
                else
                {
                    SqlContext.Pipe.SendResultsEnd();
                }


            }
            catch (Exception ex)
            {
                //Pack Errormessage in a SQL and return the result
                string strError = "Fehler in SP_MultipleSkylineBNL: ";
                strError += ex.Message;

                if (isDebug == true)
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

        }


        private static void addToWindow(DataTableReader sqlReader, string[] operators, ref ArrayList resultCollection, SqlDataRecord record, SqlBoolean isDebug, int level)
        {

            //Erste Spalte ist die ID
            long[] recordInt = new long[operators.GetUpperBound(0) + 1];
            string[] recordstring = new string[operators.GetUpperBound(0) + 1];


            for (int iCol = 0; iCol < sqlReader.FieldCount; iCol++)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol <= operators.GetUpperBound(0))
                {
                    recordInt[iCol] = sqlReader.GetInt32(iCol);
                }
                else
                {
                    record.SetValue(iCol - (operators.GetUpperBound(0) + 1), sqlReader[iCol]);
                }
            }
            record.SetValue(record.FieldCount - 1, level);

            if (isDebug == false)
            {
                SqlContext.Pipe.SendResultsRow(record);
            }
            resultCollection.Add(recordInt);
        }


        private static bool compare(DataTableReader sqlReader, string[] operators, long[] result)
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
            {
                string op = operators[iCol];
                //Compare only LOW attributes
                if (op.Equals("LOW"))
                {
                    long value = sqlReader.GetInt32(iCol);
                    int comparison = Helper.compareValue(value, result[iCol]);

                    if (comparison >= 1)
                    {
                        if (comparison == 2)
                        {
                            //at least one must be greater than
                            greaterThan = true;
                        }
                    }
                    else
                    {
                        //Value is smaller --> return false
                        return false;
                    }


                }
            }


            //all equal and at least one must be greater than
            //if (equalTo == true && greaterThan == true)
            if (greaterThan == true)
                return true;
            else
                return false;



        }



    }
}