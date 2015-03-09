//------------------------------------------------------------------------------
// <copyright file="CSSqlClassFile.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;

namespace prefSQL.SQLSkyline
{
    public class SkylineDQ : SkylineStrategy
    {
        public override bool isNative()
        {
            return false;
        }

        public override string getStoredProcedureCommand(string strSQLReturn, string strWHERE, string strOrderBy, int numberOfRecords, string strFirstSQL, string strOperators, int SkylineUpToLevel, bool hasIncomparable, string strOrderByAttributes, string[] additionalParameters)
        {
            //usual sort clause
            strFirstSQL += strOrderBy;
            //Quote quotes because it is a parameter of the stored procedure
            strFirstSQL = strFirstSQL.Replace("'", "''");

            strSQLReturn = "EXEC dbo.SP_SkylineDQ '" + strFirstSQL + "', '" + strOperators + "'," + numberOfRecords;
            return strSQLReturn;
        }

        public override DataTable getSkylineTable(String strConnection, String strQuery, String strOperators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            if (hasIncomparable)
            {
                throw new Exception("D&Q does not support incomparale tuples");
            }
            else
            {
                //D&Q algorithm neads a higher stack (much recursions). Therefore start it with a new thread
                //Default stack size is 1MB (1024000) --> Increase to 8MB. Otherwise the program might end in a stackoverflow
                DataTable dt = null;
                prefSQL.SQLSkyline.SP_SkylineDQ skyline = new SQLSkyline.SP_SkylineDQ();
                var thread = new Thread(
                () =>
                {
                    dt = skyline.getSkylineTable(strQuery, strOperators, numberOfRecords, strConnection);
                }, 8000000);

                thread.Start();

                //Join method to block the current thread  until the object's thread terminates.
                thread.Join();

                return dt; // skyline.getSkylineTable(strQuery, strOperators, numberOfRecords, strConnection);
            }

        }
    }
}
