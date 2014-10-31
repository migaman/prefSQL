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

    private class CarResult
    {
        public SqlInt32 CarId { get; set; }
        public SqlInt32 Price { get; set; }
        public SqlInt32 Mileage { get; set; }

        public SqlInt32 Horsepower { get; set; }

        public CarResult(SqlInt32 carId, SqlInt32 price, SqlInt32 mileage, SqlInt32 horsepower)
        {
            CarId = carId;
            Price = price;
            Mileage = mileage;
            Horsepower = horsepower;
        }

    }

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void SP_Skyline()
    {
        ArrayList resultCollection = new ArrayList();

        SqlConnection connection = new SqlConnection("context connection=true");
        try
        {
            connection.Open();
            SqlCommand sqlCommand = new SqlCommand("SELECT id, price, Mileage, horsepower FROM cars ORDER BY Price ASC, Mileage ASC, horsepower ASC", connection);
            


            SqlDataReader sqlReader = sqlCommand.ExecuteReader();
            
            //Alle Objekte nur einmal lesen (SqlDataReader kann nur vorwärts lesen!!)
            while (sqlReader.Read())
            {
                //
                if (resultCollection.Count == 0)
                {
                    //Liste ist leer --> Das heisst erster Eintrag ins Windows werfen
                    resultCollection.Add(new CarResult(sqlReader.GetSqlInt32(0), sqlReader.GetSqlInt32(1), sqlReader.GetSqlInt32(2), sqlReader.GetSqlInt32(3)));
                }
                else
                {
                    Boolean bDominated = false;

                    //Prüfen ob Datensatz dominiert wird (mit denen im Window)
                    for (int i = resultCollection.Count - 1; i >= 0; i--)
                    //foreach (CarResult result in resultCollection)
                    {

                        CarResult result = (CarResult)resultCollection[i];

                        //Preis vergleichen
                        if (sqlReader.GetInt32(1) >= result.Price && sqlReader.GetInt32(2) >= result.Mileage && sqlReader.GetInt32(3) <= result.Horsepower
                            && (sqlReader.GetInt32(1) > result.Price || sqlReader.GetInt32(2) > result.Mileage || sqlReader.GetInt32(3) < result.Horsepower))
                        {
                            //Neuer Punkt wird dominiert. Keine weiteren Tests nötig
                            bDominated = true;
                            break;
                        }
                        else if (sqlReader.GetInt32(1) <= result.Price && sqlReader.GetInt32(2) <= result.Mileage && sqlReader.GetInt32(3) >= result.Horsepower
                            && (sqlReader.GetInt32(1) < result.Price || sqlReader.GetInt32(2) < result.Mileage || sqlReader.GetInt32(3) > result.Horsepower))
                        {
                            //Der neue Punkt dominiert den im Window --> Punkt im Window löschen und weitertesten
                            resultCollection.RemoveAt(i);
                        }
                    }
                    if (bDominated == false)
                    {
                        //Neuer Punkt wird nicht dominiert. In das Window aufnehmen
                        resultCollection.Add(new CarResult(sqlReader.GetSqlInt32(0), sqlReader.GetSqlInt32(1), sqlReader.GetSqlInt32(2), sqlReader.GetSqlInt32(3)));
                    }

                }
            }

            sqlReader.Close();




            //Da SQLDataReader nur vorwärts lesen kann, neu lesen mit Parametersn
            string cmdText = "SELECT id, price, mileage FROM cars WHERE id IN ({0})";
            
            ArrayList paramNames = new ArrayList();
            String inClause = "";
            for(int i = 0; i < resultCollection.Count; i++)
            {
                paramNames.Add("@tag" + i);
                inClause += ", @tag" + i;

            }
            //ERstes komma abschneiden
            inClause = inClause.Substring(1);

            sqlCommand = new SqlCommand(string.Format(cmdText, inClause), connection);
            
            for (int i = 0; i < paramNames.Count; i++)
            {
                sqlCommand.Parameters.AddWithValue(paramNames[i].ToString(), ((CarResult)resultCollection[i]).CarId);
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
