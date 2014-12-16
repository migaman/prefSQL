using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;
using System.Collections.Generic;



namespace prefSQL.SQLSkyline
{

    public class SP_SkylineBNLLevel
    {
        //Only this parameters are different beteen SQL CLR function and Utility class
        private const string connectionstring = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
        private const int MaxSize = 4000;


        public static void getSkylineBNLLevel(SqlString strQuery, SqlString strOperators, SqlBoolean isDebug)
        {
            ArrayList resultCollection = new ArrayList();
            string[] operators = strOperators.ToString().Split(';');


            SqlConnection connection = new SqlConnection(connectionstring);
            try
            {
                //Some checks
                if (strQuery.ToString().Length == MaxSize)
                {
                    throw new Exception("Query is too long. Maximum size is " + MaxSize);
                }
                connection.Open();

                SqlDataAdapter dap = new SqlDataAdapter(strQuery.ToString(), connection);
                DataTable dt = new DataTable();
                dap.Fill(dt);


                // Build our record schema 
                List<SqlMetaData> outputColumns = buildRecordSchema(dt, operators);

                SqlDataRecord record = new SqlDataRecord(outputColumns.ToArray());

                DataTableReader sqlReader = dt.CreateDataReader();



                //Read all records only once. (SqlDataReader works forward only!!)
                while (sqlReader.Read())
                {
                    //Check if window list is empty
                    if (resultCollection.Count == 0)
                    {
                        // Build our SqlDataRecord and start the results 
                        addToWindow(record, sqlReader, operators, ref resultCollection);
                    }
                    else
                    {
                        bool bDominated = false;

                        //check if record is dominated (compare against the records in the window)
                        for (int i = resultCollection.Count - 1; i >= 0; i--)
                        {
                            int[] result = (int[])resultCollection[i];

                            //Dominanz
                            if (compare(result, sqlReader) == true)
                            {
                                //New point is dominated. No further testing necessary
                                bDominated = true;
                                break;
                            }


                            //Now, check if the new point dominates the one in the window
                            //--> It is not possible that the new point dominates the one in the window --> Raason data is ORDERED
                        }
                        if (bDominated == false)
                        {
                            addToWindow(record, sqlReader, operators, ref resultCollection);


                        }

                    }
                }

                sqlReader.Close();


                System.Diagnostics.Debug.WriteLine(resultCollection.Count);


            }
            catch (Exception ex)
            {
                //Pack Errormessage in a SQL and return the result
                string strError = "SELECT 'Fehler in SP_SkylineBNL: ";
                strError += ex.Message.Replace("'", "''");
                strError += "'";


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


        private static void addToWindow(SqlDataRecord record, DataTableReader sqlReader, string[] operators, ref ArrayList resultCollection)
        {

            //Erste Spalte ist die ID
            int[] recordInt = new int[operators.GetUpperBound(0) + 1];
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


            resultCollection.Add(recordInt);
        }


        private static bool compare(int[] result, DataTableReader sqlReader)
        {
            bool greaterThan = false;

            for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
            {
                //Compare only LOW attributes
                int value = sqlReader.GetInt32(iCol);
                int comparison = compareValue(value, result[iCol]);

                if (comparison == 2)
                {

                    //at least one must be greater than
                    greaterThan = true;

                }
                else if (comparison == 0)
                {
                    //Value is smaller --> return false
                    return false;
                }

            }


            //all equal and at least one must be greater than
            //if (equalTo == true && greaterThan == true)
            if (greaterThan == true)
                return true;
            else
                return false;



        }


        /*
         * 0 = false
         * 1 = equal
         * 2 = greater than
         * */
        private static int compareValue(int value1, int value2)
        {

            if (value1 >= value2)
            {
                if (value1 > value2)
                    return 2;
                else
                    return 1;

            }
            else
            {
                return 0;
            }

        }


        private static List<SqlMetaData> buildRecordSchema(DataTable dt, string[] operators)
        {
            List<SqlMetaData> outputColumns = new List<SqlMetaData>(dt.Columns.Count);
            int iCol = 0;
            foreach (DataColumn col in dt.Columns)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol > operators.GetUpperBound(0))
                {
                    SqlMetaData OutputColumn;
                    if (col.DataType.Equals(typeof(Int32)) || col.DataType.Equals(typeof(DateTime)))
                    {
                        OutputColumn = new SqlMetaData(col.ColumnName, prefSQL.SQLSkyline.TypeConverter.ToSqlDbType(col.DataType));
                    }
                    else
                    {
                        OutputColumn = new SqlMetaData(col.ColumnName, prefSQL.SQLSkyline.TypeConverter.ToSqlDbType(col.DataType), col.MaxLength);
                    }
                    outputColumns.Add(OutputColumn);
                }
                iCol++;
            }
            return outputColumns;
        }


    }
}