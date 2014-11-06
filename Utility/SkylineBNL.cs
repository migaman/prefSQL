using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;
using System.Diagnostics;

namespace Utility
{

    //same as the SP SkyineBNL --> for testing the performance and debugging
    class SkylineBNL
    {
        //Only this parameters are different to the SQL CLR function
        private const bool bSQLCLR = false;
        private const string connectionString = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
        private const int MaxSize = 4000;

        [Microsoft.SqlServer.Server.SqlProcedure]
        public static void SP_SkylineBNL(SqlString strQuery, SqlString strOperators, SqlString strQueryNative, String strTable)
        {
            ArrayList idCollection = new ArrayList();
            ArrayList resultCollection = new ArrayList();
            ArrayList resultStringCollection = new ArrayList();
            String[] operators = strOperators.ToString().Split(';');


            SqlConnection connection = new SqlConnection(connectionString);
            try
            {
                connection.Open();


                //Some checks
                if (strQuery.ToString().Length == MaxSize)
                {
                    throw new Exception("Query is too long. Maximum size is " + MaxSize);
                }


                SqlCommand sqlCommand = new SqlCommand(strQuery.ToString(), connection);
                SqlDataReader sqlReader = sqlCommand.ExecuteReader();

                //Read all records only once. (SqlDataReader works forward only!!)
                while (sqlReader.Read())
                {

                    //Check if window list is empty
                    if (resultCollection.Count == 0)
                    {
                        addToWindow(sqlReader, operators, ref resultCollection, ref idCollection, ref resultStringCollection);
                    }
                    else
                    {
                        Boolean bDominated = false;

                        //check if record is dominated (compare against the records in the window)
                        for (int i = resultCollection.Count - 1; i >= 0; i--)
                        {
                            long[] result = (long[])resultCollection[i];
                            string[] strResult = (string[])resultStringCollection[i];

                            //Dominanz
                            Boolean equalThan = false;
                            Boolean greaterThan = false;
                            compare(sqlReader, operators, result, strResult, ref equalThan, ref greaterThan, false);
                            
                            if (equalThan == true && greaterThan == true)
                            {
                                //New point is dominated. No further testing necessary
                                bDominated = true;
                                break;
                            }


                            //Now, check if the new point dominates the one in the window
                            equalThan = false;
                            greaterThan = false;
                            compare(sqlReader, operators, result, strResult, ref equalThan, ref greaterThan, true);

                            if (equalThan == true && greaterThan == true)
                            {
                                //The new record dominates the one in the windows. Remove point from window and test further
                                resultCollection.RemoveAt(i);
                                idCollection.RemoveAt(i);
                            }


                        }
                        if (bDominated == false)
                        {
                            addToWindow(sqlReader, operators, ref resultCollection, ref idCollection, ref resultStringCollection);


                        }

                    }
                }

                sqlReader.Close();

                //TODO: Debug is forbidden in SQL CRL
                Debug.WriteLine("Total in Skyline: " + idCollection.Count);


                //OTHER Idea: Store current collection in temporary table and return the result of the table

                //SQLDataReader wokrs only forward. There read new with parameters
                string cmdText = strQueryNative.ToString() + " WHERE ({0})";

                ArrayList paramNames = new ArrayList();
                String strIN = "";
                String inClause = "";
                int amountOfSplits = 0;
                for (int i = 0; i < idCollection.Count; i++)
                {
                    if (i % 2000 == 0)
                    {
                        if (amountOfSplits > 0)
                        {
                            //Add OR after IN
                            strIN += " OR ";
                            //Remove first comma
                            inClause = inClause.Substring(1);
                            strIN = string.Format(strIN, inClause);
                            inClause = "";
                        }
                        strIN += strTable + ".id IN ({0})";


                        amountOfSplits++;
                    }

                    inClause += ", " + idCollection[i];

                }
                //Remove first comman
                inClause = inClause.Substring(1);
                strIN = string.Format(strIN, inClause);


                sqlCommand = new SqlCommand(string.Format(cmdText, strIN), connection);
                sqlReader = sqlCommand.ExecuteReader();


                if (bSQLCLR == true)
                {
                    SqlContext.Pipe.Send(sqlReader);
                }
                


            }
            catch (Exception ex)
            {
                //Pack Errormessage in a SQL and return the result

                String strError = "SELECT 'Fehler in SP_SkylineBNL: ";
                strError += ex.Message.Replace("'", "''");
                strError += "'";

                SqlCommand sqlCommand = new SqlCommand(strError, connection);
                SqlDataReader sqlReader = sqlCommand.ExecuteReader();

                SqlContext.Pipe.Send(sqlReader);


            }
            finally
            {
                if (connection != null)
                    connection.Close();
            }

        }


        private static void addToWindow(SqlDataReader sqlReader, String[] operators, ref ArrayList resultCollection, ref ArrayList idCollection, ref ArrayList resultStringCollection)
        {
            //Liste ist leer --> Das heisst erster Eintrag ins Window werfen
            //Erste Spalte ist die ID
            long[] record = new long[sqlReader.FieldCount];
            string[] recordString = new String[sqlReader.FieldCount];
            for (int i = 0; i <= record.GetUpperBound(0); i++)
            {
                //LOW und HIGH Spalte in record abfüllen
                if (operators[i].StartsWith("LOW") || operators[i].StartsWith("HIGH"))
                {
                    Type type = sqlReader.GetFieldType(i);
                    if (type == typeof(int))
                    {
                        record[i] = sqlReader.GetInt32(i);
                    }
                    else if (type == typeof(DateTime))
                    {
                        record[i] = sqlReader.GetDateTime(i).Ticks;
                    }

                    //Check if long value is incomparable

                    if (operators[i].Contains("INCOMPARABLE"))
                    {
                        //Incomparable field is always the next one
                        type = sqlReader.GetFieldType(i+1);
                        if (type == typeof(string))
                        {
                            recordString[i] = sqlReader.GetString(i+1);
                        }

                    }


                }
               
            }
            resultCollection.Add(record);
            idCollection.Add(sqlReader.GetInt32(0));
            resultStringCollection.Add(recordString);
        }


        private static void compare(SqlDataReader sqlReader, String[] operators, long[] result, string[] stringResult, ref Boolean equalThan, ref Boolean greaterThan, Boolean bSecondMethod) 
        {
            //Boolean equalThan = false;
            //Boolean greaterThan = false;
            for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
            {
                String op = operators[iCol];
                //Compare only LOW and HIGH attributes
                if (op.StartsWith("LOW") || op.StartsWith("HIGH"))
                {
                    //Convert value if it is a date
                    long value = 0;
                    Type type = sqlReader.GetFieldType(iCol);
                    if (type == typeof(int))
                    {
                        value = sqlReader.GetInt32(iCol);
                    }
                    else if (type == typeof(DateTime))
                    {
                        value = sqlReader.GetDateTime(iCol).Ticks;
                    }

                    if (compareValues(op, value, result[iCol], false, bSecondMethod) == true)
                    {
                        equalThan = true;
                        if (compareValues(op, value, result[iCol], true, bSecondMethod) == true)
                        {
                            //at least one must be greater than
                            greaterThan = true;
                        }
                        else
                        {
                            //It is the same long value
                            //Check if the value must be text compared
                            if(op.Contains("INCOMPARABLE"))
                            {
                                //String value is always the next field
                                String strValue = sqlReader.GetString(iCol + 1);
                                //If it is not the same String value, the values are incomparable!!
                                if (strValue != stringResult[iCol])
                                {
                                    equalThan = false;
                                    break;
                                }

                                
                            }
                        }
                    }
                    else
                    {
                        equalThan = false;
                        break;
                    }
                }
            }






        }

        private static Boolean compareValues(String op, long value1, long value2, bool greaterThan, bool backwards)
        {
            if (backwards == true)
            {
                if (op.StartsWith("LOW"))
                    op.Replace("LOW", "HIGH");
                else
                    op.Replace("HIGH", "LOW");
            }

            if (greaterThan == false)
            {
                if (op.StartsWith("LOW"))
                {
                    if (value1 >= value2)
                    {
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
                else if (op.StartsWith("HIGH"))
                {
                    if (value1 <= value2)
                    {
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }
            else
            {
                if (op.StartsWith("LOW"))
                {
                    if (value1 > value2)
                    {
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
                else if (op.StartsWith("HIGH"))
                {
                    if (value1 < value2)
                    {
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }
            return false;

        }


    }
}
