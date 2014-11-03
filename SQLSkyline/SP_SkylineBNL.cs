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
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void SP_SkylineBNL(SqlString strQuery, SqlString strOperators, String strQueryNative)
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
                
                //Prüfen ob Liste noch leer ist
                if (resultCollection.Count == 0)
                {
                    //Liste ist leer --> Das heisst erster Eintrag ins Window werfen
                    //Erste Spalte ist die ID
                    int[] record = new int[sqlReader.FieldCount];
                    for (int i = 0; i <= record.GetUpperBound(0); i++)
                    {
                        //LOW und HIGH Spalte in record abfüllen
                        if (operators[i].Equals("LOW") || operators[i].Equals("HIGH"))
                        {
                            record[i] = sqlReader.GetInt32(i);
                        }
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
                        for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
                        {
                            String op = operators[iCol];
                            //Nur LOW und HIGH Attribute vergleichen
                            if (op.Equals("LOW") || op.Equals("HIGH"))
                            {
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
                        }

                        if (equalThan == true && greaterThan == true)
                        {
                            //Neuer Punkt wird dominiert. Keine weiteren Tests nötig
                            bDominated = true;
                            break;
                        }
                        

                        //Nun noch prüfen ob der neue Punkt den im Window dominiert
                        equalThan = false;
                        greaterThan = false;
                        for (int iCol = 0; iCol <= result.GetUpperBound(0); iCol++)
                        {
                            String op = operators[iCol];
                            //Nur LOW und HIGH Attribute vergleichen
                            if (op.Equals("LOW") || op.Equals("HIGH"))
                            {
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
                        for (int i = 0; i <= record.GetUpperBound(0); i++)
                        {
                            //LOW und HIGH Spalte in record abfüllen
                            if (operators[i].Equals("LOW") || operators[i].Equals("HIGH"))
                            {
                                record[i] = sqlReader.GetInt32(i);
                            }
                        }
                        resultCollection.Add(record);
                        idCollection.Add(sqlReader.GetInt32(0));


                    }

                }
            }

            sqlReader.Close();


            //Store current collection in temporary table and return the result of the table

            //Da SQLDataReader nur vorwärts lesen kann, neu lesen mit Parametersn
            string cmdText = strQueryNative.ToString() + " WHERE cars.id IN ({0})";

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

}
