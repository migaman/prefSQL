//------------------------------------------------------------------------------
// <copyright file="CSSqlStoredProcedure.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------
using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;


public partial class StoredProcedures
{
    private const string connectionstring = "context connection=true";
    private const int MaxSize = 4000;

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void SP_SkylineBNL(SqlString strQuery, SqlString strOperators, SqlString strQueryNative, string strTable)
    {
        ArrayList idCollection = new ArrayList();
        ArrayList resultCollection = new ArrayList();
        ArrayList resultstringCollection = new ArrayList();
        string[] operators = strOperators.ToString().Split(';');


        SqlConnection connection = new SqlConnection(connectionstring);
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
                    addToWindow(sqlReader, operators, ref resultCollection, ref idCollection, ref resultstringCollection);
                }
                else
                {
                    bool bDominated = false;

                    //check if record is dominated (compare against the records in the window)
                    for (int i = resultCollection.Count - 1; i >= 0; i--)
                    {
                        long[] result = (long[])resultCollection[i];
                        string[] strResult = (string[])resultstringCollection[i];

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
                        addToWindow(sqlReader, operators, ref resultCollection, ref idCollection, ref resultstringCollection);
                    }

                }
            }

            sqlReader.Close();


            //OTHER Idea: Store current collection in temporary table and return the result of the table

            //SQLDataReader wokrs only forward. There read new with parameters
            string cmdText = "";
            if (strQueryNative.ToString().IndexOf("WHERE") > 0)
                cmdText = strQueryNative.ToString() + " AND ({0})";
            else
                cmdText = strQueryNative.ToString() + " WHERE ({0})";


            ArrayList paramNames = new ArrayList();
            string strIN = "";
            string inClause = "";
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
            if (inClause.Length > 0)
            {
                inClause = inClause.Substring(1);
                strIN = string.Format(strIN, inClause);
            }
            else
            {
                strIN = "0 = 1";
            }


            sqlCommand = new SqlCommand(string.Format(cmdText, strIN), connection);
            sqlReader = sqlCommand.ExecuteReader();


            SqlContext.Pipe.Send(sqlReader);


        }
        catch (Exception ex)
        {
            //Pack Errormessage in a SQL and return the result

            string strError = "SELECT 'Fehler in SP_SkylineBNL: ";
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


    private static void addToWindow(SqlDataReader sqlReader, string[] operators, ref ArrayList resultCollection, ref ArrayList idCollection, ref ArrayList resultstringCollection)
    {
        //Liste ist leer --> Das heisst erster Eintrag ins Window werfen
        //Erste Spalte ist die ID
        long[] record = new long[sqlReader.FieldCount];
        string[] recordstring = new string[sqlReader.FieldCount];
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
                    record[i] = sqlReader.GetDateTime(i).Ticks; // sqlReader.GetDateTime(i).Year * 10000 + sqlReader.GetDateTime(i).Month * 100 + sqlReader.GetDateTime(i).Day;
                }

                //Check if long value is incomparable
                if (i + 1 <= record.GetUpperBound(0) && operators[i + 1].Equals("INCOMPARABLE"))
                {
                    //Incomparable field is always the next one
                    type = sqlReader.GetFieldType(i + 1);
                    if (type == typeof(string))
                    {
                        recordstring[i] = sqlReader.GetString(i + 1);
                    }

                }




            }

        }
        resultCollection.Add(record);
        idCollection.Add(sqlReader.GetInt32(0));
        resultstringCollection.Add(recordstring);
    }


    private static bool compare(SqlDataReader sqlReader, string[] operators, long[] result, string[] stringResult)
    {
        //bool equalTo = false;
        bool greaterThan = false;
        
        for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
        {
            string op = operators[iCol];
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
                    //equalTo = true;
                    if (comparison == 2)
                    {
                        //at least one must be greater than
                        greaterThan = true;
                    }
                    else
                    {
                        //It is the same long value
                        //Check if the value must be text compared
                        if (iCol + 1 <= result.GetUpperBound(0) && operators[iCol + 1].Equals("INCOMPARABLE"))
                        {
                            //string value is always the next field
                            string strValue = sqlReader.GetString(iCol + 1);
                            //If it is not the same string value, the values are incomparable!!
                            if (strValue != stringResult[iCol])
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

    /*
         * 0 = false
         * 1 = equal
         * 2 = greater than
         * */
    private static int compareValue(string op, long value1, long value2)
    {

        //Switch numbers on certain case
        if (op.Equals("HIGH"))
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
