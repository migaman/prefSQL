//------------------------------------------------------------------------------
// <copyright file="CSSqlFunction.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------
using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;

public partial class UserDefinedFunctions
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

    public static void SkylineBNL_FillRow(
        object carResultObject,
        out SqlInt32 carID)
    {
        var carResult = (CarResult)carResultObject;

        carID = carResult.CarId;
    }


    [SqlFunction
        (
            DataAccess = DataAccessKind.Read,
            TableDefinition = "carID int",
            FillRowMethodName = "SkylineBNL_FillRow"
        )
    ]
    
    
    //Implementation of Skyline with a Block Nested Loop Algorithm
    public static IEnumerable SkylineBNL()
    {
        ArrayList resultCollection = new ArrayList();
        
       
        using (SqlConnection connection = new SqlConnection("context connection=true"))
        {
            connection.Open();

            using (SqlCommand selectCars = new SqlCommand(
                "SELECT id, price, Mileage, horsepower FROM cars ORDER BY Price ASC, Mileage ASC, horsepower ASC", connection))
            {
                //SqlParameter modifiedSinceParam = selectEmails.Parameters.Add("@modifiedSince", SqlDbType.DateTime);
                //modifiedSinceParam.Value = modifiedSince;

                using (SqlDataReader carsReader = selectCars.ExecuteReader())
                {
                    //Alle Objekte nur einmal lesen
                    while (carsReader.Read())
                    {
                        //
                        if (resultCollection.Count == 0)
                        {
                            //Liste ist leer --> Das heisst erster Eintrag ins Windows werfen
                            resultCollection.Add(new CarResult(carsReader.GetSqlInt32(0), carsReader.GetSqlInt32(1), carsReader.GetSqlInt32(2), carsReader.GetSqlInt32(3)));
                        }
                        else
                        {
                            Boolean bDominated = false;
                            
                            //Prüfen ob Datensatz dominiert wird (mit denen im Window)
                            for (int i = resultCollection.Count - 1; i >= 0; i-- )
                                //foreach (CarResult result in resultCollection)
                            {

                                CarResult result = (CarResult)resultCollection[i];

                                //Preis vergleichen
                                if (carsReader.GetInt32(1) >= result.Price && carsReader.GetInt32(2) >= result.Mileage && carsReader.GetInt32(3) <= result.Horsepower
                                    && (carsReader.GetInt32(1) > result.Price || carsReader.GetInt32(2) > result.Mileage || carsReader.GetInt32(3) < result.Horsepower))
                                {
                                    //Neuer Punkt wird dominiert. Keine weiteren Tests nötig
                                    bDominated = true;
                                    break;
                                }
                                else if (carsReader.GetInt32(1) <= result.Price && carsReader.GetInt32(2) <= result.Mileage && carsReader.GetInt32(3) >= result.Horsepower
                                    && (carsReader.GetInt32(1) < result.Price || carsReader.GetInt32(2) < result.Mileage || carsReader.GetInt32(3) > result.Horsepower))
                                {
                                    //Der neue Punkt dominiert den im Window --> Punkt im Window löschen und weitertesten
                                    resultCollection.RemoveAt(i);

                                    /*if (list[i] > 5)
                                        list.RemoveAt(i);*/


                                    //break;
                                }
                                /*else
                                {
                                    bDominated = false;
                                }*/
                            }
                            if(bDominated == false)
                            {
                                //Neuer Punkt wird nicht dominiert. In das Window aufnehmen
                                resultCollection.Add(new CarResult(carsReader.GetSqlInt32(0), carsReader.GetSqlInt32(1), carsReader.GetSqlInt32(2), carsReader.GetSqlInt32(3)));
                            }

                        }
                                                
                    }
                }
            }
        }

        return resultCollection;
    }


}
