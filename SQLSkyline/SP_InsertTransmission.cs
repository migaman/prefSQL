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
    public static void InsertTransmission(SqlString name)
    {
        //Just for proof of concept
        using (SqlConnection conn = new SqlConnection("context connection=true"))
        {
            SqlCommand InsertCommand = new SqlCommand();
            SqlParameter nameParam = new SqlParameter("@Name", SqlDbType.NVarChar);

            nameParam.Value = name;

            InsertCommand.Parameters.Add(nameParam);

            InsertCommand.CommandText = "INSERT Transmissions (Name) VALUES(@Name)";

            InsertCommand.Connection = conn;

            conn.Open();
            InsertCommand.ExecuteNonQuery();
            conn.Close();
        }
    }
}
