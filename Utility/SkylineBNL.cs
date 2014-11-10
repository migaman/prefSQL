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
    //Hinweis: Wenn mit startswith statt equals gearbeitet wird führt dies zu massiven performance problemen, z.B. large dataset 30 statt 3 Sekunden mit 13 Dimensionen!!
    //WICHTIG: Vergleiche immer mit equals und nie mit z.B. startsWith oder Contains oder so.... --> Enorme Performance Unterschiede

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
                        Debug.Print(resultCollection.Count.ToString());
                        Boolean bDominated = false;

                        //check if record is dominated (compare against the records in the window)
                        for (int i = resultCollection.Count - 1; i >= 0; i--)
                        {
                            long[] result = (long[])resultCollection[i];
                            string[] strResult = (string[])resultStringCollection[i];

                            //Dominanz
                            if (compare(sqlReader, operators, result, strResult) == true)
                            {
                                //New point is dominated. No further testing necessary
                                bDominated = true;
                                break;
                            }


                            //Now, check if the new point dominates the one in the window
                            //It is not possible that the new point dominates the one in the window --> Raason data is ORDERED


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



                


            }
            catch (Exception ex)
            {
                //Pack Errormessage in a SQL and return the result

                String strError = "SELECT 'Fehler in SP_SkylineBNL: ";
                strError += ex.Message.Replace("'", "''");
                strError += "'";

                SqlCommand sqlCommand = new SqlCommand(strError, connection);
                SqlDataReader sqlReader = sqlCommand.ExecuteReader();



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
                if (operators[i].Equals("LOW") || operators[i].Equals("HIGH"))
                {
                    Type type = sqlReader.GetFieldType(i);
                    if (type == typeof(int))
                    {
                        record[i] = sqlReader.GetInt32(i);
                    }
                    else if (type == typeof(DateTime))
                    {
                        record[i] = sqlReader.GetDateTime(i).Year * 10000 + sqlReader.GetDateTime(i).Month * 100 + sqlReader.GetDateTime(i).Day;
                    }

                    //Check if long value is incomparable
                    if (i+1 <= record.GetUpperBound(0) && operators[i+1].Equals("INCOMPARABLE"))
                    {
                        //Incomparable field is always the next one
                        type = sqlReader.GetFieldType(i+1);
                        if (type == typeof(string))
                        {
                            recordString[i] = sqlReader.GetString(i + 1);
                        }

                    }




                }
               
            }
            resultCollection.Add(record);
            idCollection.Add(sqlReader.GetInt32(0));
            resultStringCollection.Add(recordString);
        }


        private static bool compare(SqlDataReader sqlReader, string[] operators, long[] result, string[] stringResult) 
        {
            bool greaterThan = false;

            //Boolean equalThan = false;
            //Boolean greaterThan = false;
            for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
            {
                String op = operators[iCol];
                //Compare only LOW and HIGH attributes
                if (op.Equals("LOW") || op.Equals("HIGH"))
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
                        value = sqlReader.GetDateTime(iCol).Year * 10000 + sqlReader.GetDateTime(iCol).Month * 100 + sqlReader.GetDateTime(iCol).Day;
                    }

                    int comparison = compareValue(op, value, result[iCol]);

                    if (comparison >= 1)
                    {
                        if (comparison == 2)
                        {
                            //at least one must be greater than
                            greaterThan = true;
                        }
                        else
                        {
                            //It is the same long value
                            //Check if the value must be text compared
                            if(iCol+1 <= result.GetUpperBound(0) && operators[iCol+1].Equals("INCOMPARABLE"))
                            {
                                //String value is always the next field
                                String strValue = sqlReader.GetString(iCol + 1);
                                //If it is not the same String value, the values are incomparable!!
                                if (!strValue.Equals(stringResult[iCol]))
                                {
                                    //Value is incomparable --> return false
                                    return false;
                                }

                                
                            }
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
        



        [Microsoft.SqlServer.Server.SqlProcedure]
        public static void SP_SkylineBNL_Level(SqlString strQuery, SqlString strOperators, SqlString strQueryNative, String strTable)
        {
            ArrayList idCollection = new ArrayList();
            ArrayList resultCollection = new ArrayList();
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
                        addToWindow(sqlReader, operators, ref resultCollection, ref idCollection);
                    }
                    else
                    {
                        Boolean bDominated = false;

                        //check if record is dominated (compare against the records in the window)
                        for (int i = resultCollection.Count - 1; i >= 0; i--)
                        {
                            long[] result = (long[])resultCollection[i];

                            //Dominanz
                            if (compareLevel(sqlReader, operators, result) == true)
                            {
                                //New point is dominated. No further testing necessary
                                bDominated = true;
                                break;

                            }

                            //Now, check if the new point dominates the one in the window
                            //It is not possible that the new point dominates the one in the window --> Raason data is ORDERED

                        }
                        if (bDominated == false)
                        {
                            addToWindow(sqlReader, operators, ref resultCollection, ref idCollection);
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


        private static void addToWindow(SqlDataReader sqlReader, String[] operators, ref ArrayList resultCollection, ref ArrayList idCollection)
        {
            //Liste ist leer --> Das heisst erster Eintrag ins Window werfen
            //Erste Spalte ist die ID
            long[] record = new long[sqlReader.FieldCount];
            string[] recordString = new String[sqlReader.FieldCount];
            for (int i = 0; i <= record.GetUpperBound(0); i++)
            {
                //LOW und HIGH Spalte in record abfüllen
                if (operators[i].Equals("LOW") || operators[i].Equals("HIGH"))
                {
                    Type type = sqlReader.GetFieldType(i);
                    if (type == typeof(int))
                    {
                        record[i] = sqlReader.GetInt32(i);
                    }
                    else if (type == typeof(DateTime))
                    {
                        record[i] = sqlReader.GetDateTime(i).Ticks; // .GetDateTime(i).Year * 10000 + sqlReader.GetDateTime(i).Month * 100 + sqlReader.GetDateTime(i).Day;
                    }


                }

            }
            resultCollection.Add(record);
            idCollection.Add(sqlReader.GetInt32(0));
        }



        private static bool compareLevel(SqlDataReader sqlReader, String[] operators, long[] result)
        {
            bool greaterThan = false;
            
            for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
            {
                String op = operators[iCol];
                //Compare only LOW and HIGH attributes
                if (op.Equals("LOW") || op.Equals("HIGH"))
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
                        value = sqlReader.GetDateTime(iCol).Ticks; // sqlReader.GetDateTime(iCol).Year * 10000 + sqlReader.GetDateTime(iCol).Month * 100 + sqlReader.GetDateTime(iCol).Day;
                    }

                    int comparison = compareValue(op, value, result[iCol]);
                    
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
                        return false;
                    }
                }
            }

            //all equal and at least one must be greater than
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
        private static int compareValue(String op, long value1, long value2)
        {

            //Switch numbers on certain case
            if(op.Equals("HIGH"))
            {
                long tmpValue = value1;
                value1 = value2;
                value2 = tmpValue;
            }


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








    }
}
