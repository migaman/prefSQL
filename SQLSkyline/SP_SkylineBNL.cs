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

    private static Boolean compareValues(String op, int value1, int value2, bool greaterThan, bool umgekehrt)
    {
        if (umgekehrt == true)
        {
            if (op == "LOW")
                op = "HIGH";
            else
                op = "LOW";
        }

        if (greaterThan == false)
        {
            if (op == "LOW")
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
            else if (op == "HIGH")
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
            if (op == "LOW")
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
            else if (op == "HIGH")
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


    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void SP_SkylineBNL(SqlString strQuery, SqlString strOperators)
    {
        ArrayList idCollection = new ArrayList();
        ArrayList resultCollection = new ArrayList();
        String[] operators = strOperators.ToString().Split(';');


        SqlConnection connection = new SqlConnection("context connection=true");
        try
        {
            connection.Open();
            SqlCommand sqlCommand = new SqlCommand(strQuery.ToString(), connection);



            SqlDataReader sqlReader = sqlCommand.ExecuteReader();

            //Alle Objekte nur einmal lesen (SqlDataReader kann nur vorwärts lesen!!)
            while (sqlReader.Read())
            {
                //
                if (resultCollection.Count == 0)
                {
                    //Liste ist leer --> Das heisst erster Eintrag ins Window werfen
                    //Erste Spalte ist die ID
                    int[] record = new int[sqlReader.FieldCount];
                    for (int i = 1; i < record.GetUpperBound(0) + 1; i++)
                    {
                        record[i] = sqlReader.GetInt32(i);
                    }
                    resultCollection.Add(record);
                    idCollection.Add(sqlReader.GetInt32(0));
                }
                else
                {
                    Boolean bDominated = false;

                    //Prüfen ob Datensatz dominiert wird (mit denen im Window)
                    for (int i = resultCollection.Count - 1; i >= 0; i--)
                    {
                        int[] result = (int[])resultCollection[i];

                        //Dominanz
                        Boolean equalThan = false;
                        Boolean greaterThan = false;
                        for (int iCol = 1; iCol <= result.GetUpperBound(0); iCol++)
                        {
                            String op = operators[iCol];
                            if (compareValues(op, sqlReader.GetInt32(iCol), result[iCol], false, false) == true)
                            {
                                equalThan = true;
                                if (compareValues(op, sqlReader.GetInt32(iCol), result[iCol], true, false) == true)
                                {
                                    //minimum einer muss grösser sein
                                    greaterThan = true;
                                }
                            }
                            else
                            {
                                equalThan = false;
                                break;
                            }
                        }

                        if (equalThan == true && greaterThan == true)
                        {
                            //Neuer Punkt wird dominiert. Keine weiteren Tests nötig
                            bDominated = true;
                            break;
                        }
                        //Nun noch prüfen ob der neue Punkt den im Window dominiert
                        /*else if (sqlReader.GetInt32(1) <= result[1] && sqlReader.GetInt32(2) <= result[2] && sqlReader.GetInt32(3) >= result[3]
                            && (sqlReader.GetInt32(1) < result[1] || sqlReader.GetInt32(2) < result[2] || sqlReader.GetInt32(3) > result[3]))
                        {
                            //Der neue Punkt dominiert den im Window --> Punkt im Window löschen und weitertesten
                            resultCollection.RemoveAt(i);
                            idCollection.RemoveAt(i);
                        }*/



                        //Dominiert werden
                        equalThan = false;
                        greaterThan = false;
                        for (int iCol = 1; iCol <= result.GetUpperBound(0); iCol++)
                        {
                            String op = operators[iCol];
                            if (compareValues(op, sqlReader.GetInt32(iCol), result[iCol], false, true) == true)
                            {
                                equalThan = true;
                                if (compareValues(op, sqlReader.GetInt32(iCol), result[iCol], true, true) == true)
                                {
                                    //minimum einer muss grösser sein
                                    greaterThan = true;
                                }
                            }
                            else
                            {
                                equalThan = false;
                                break;
                            }
                        }

                        if (equalThan == true && greaterThan == true)
                        {
                            //Der neue Punkt dominiert den im Window --> Punkt im Window löschen und weitertesten
                            resultCollection.RemoveAt(i);
                            idCollection.RemoveAt(i);
                        }


                    }
                    if (bDominated == false)
                    {
                        //Neuer Punkt wird nicht dominiert. In das Window aufnehmen
                        //resultCollection.Add(new CarResult(sqlReader.GetSqlInt32(0), sqlReader.GetSqlInt32(1), sqlReader.GetSqlInt32(2), sqlReader.GetSqlInt32(3)));
                        int[] record = new int[sqlReader.FieldCount];
                        for (int i = 1; i < record.GetUpperBound(0) + 1; i++)
                        {
                            record[i] = sqlReader.GetInt32(i);
                        }
                        resultCollection.Add(record);
                        idCollection.Add(sqlReader.GetInt32(0));
                    }

                }
            }

            sqlReader.Close();




            //Da SQLDataReader nur vorwärts lesen kann, neu lesen mit Parametersn
            string cmdText = strQuery.ToString() + " WHERE id IN ({0})";

            ArrayList paramNames = new ArrayList();
            String inClause = "";
            for (int i = 0; i < idCollection.Count; i++)
            {
                paramNames.Add("@tag" + i);
                inClause += ", @tag" + i;

            }
            //ERstes komma abschneiden
            inClause = inClause.Substring(1);

            sqlCommand = new SqlCommand(string.Format(cmdText, inClause), connection);

            for (int i = 0; i < paramNames.Count; i++)
            {
                sqlCommand.Parameters.AddWithValue(paramNames[i].ToString(), idCollection[i]);
            }


            sqlReader = sqlCommand.ExecuteReader();



            SqlContext.Pipe.Send(sqlReader);

        }
        catch (Exception ex)
        {
            throw ex;
        }
        finally
        {
            if (connection != null)
                connection.Close();
        }

    }
}
