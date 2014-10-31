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
    private class TableType
    {
        public SqlInt32 Sequence { get; set; }
        public SqlString Text { get; set; }
    }

    public static void TableUDF_FillRow(
        object tableTypeObject,
        out SqlInt32 sequence,
        out SqlString text)
    {
        var tableType = (TableType)tableTypeObject;

        sequence = tableType.Sequence;
        text = tableType.Text;
    }


    [SqlFunction(
        DataAccess = DataAccessKind.Read,
        TableDefinition = "Sequence int, Text nvarchar(100)",
        FillRowMethodName = "TableUDF_FillRow")]
    public static IEnumerable TableUDF()
    {
        //Just for proof of concept
        var tableResults = new ArrayList
        {
            new TableType {Sequence = 1, Text = "Hello"},
            new TableType {Sequence = 2, Text = "World"},
            new TableType {Sequence = 3, Text = "!"}
        };

        return tableResults;
    }


}
