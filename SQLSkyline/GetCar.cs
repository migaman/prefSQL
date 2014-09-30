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

public partial class StoredProcedures
{
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void GetCar ()
    {
        SqlConnection connection =
              new SqlConnection("context connection=true");
        try
        {
            connection.Open();
            SqlCommand sqlCommand = new SqlCommand("SELECT Title, Price FROM cars", connection);
            SqlDataReader sqlReader = sqlCommand.ExecuteReader();
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
