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
using System.Threading.Tasks;

namespace prefSQL.SQLSkyline
{
    public class SkylineHexagon : SkylineStrategy
    {
        public override bool isNative()
        {
            return false;
        }

        public override string getStoredProcedureCommand(string strSQLReturn, string strWHERE, string strOrderBy, int numberOfRecords, string strFirstSQL, string strOperators, int SkylineUpToLevel, bool hasIncomparable, string strOrderByAttributes, string[] additionalParameters)
        {

            strFirstSQL = additionalParameters[0];
            strOperators = additionalParameters[1];
            string strHexagon = additionalParameters[2];
            string strSelectDistinctIncomparable = additionalParameters[3];
            int weightHexagonIncomparable = int.Parse(additionalParameters[4]);

            if (hasIncomparable == true)
            {
                strSQLReturn = "EXEC dbo.SP_SkylineHexagon '" + strFirstSQL + "', '" + strOperators + "', " + numberOfRecords + ", '" + strHexagon + "', '" + strSelectDistinctIncomparable + "'," + weightHexagonIncomparable;
            }
            else
            {
                strSQLReturn = "EXEC dbo.SP_SkylineHexagonLevel '" + strFirstSQL + "', '" + strOperators + "', " + numberOfRecords + ", '" + strHexagon + "'";
            }
            return strSQLReturn;
        }
        public override DataTable getSkylineTable(String strConnection, String strQuery, String strOperators, int numberOfRecords, bool hasIncomparable, string[] additionalParameters)
        {
            //Hexagon needs additional parameters
            string strQueryConstruction = additionalParameters[3].Trim().Replace("''", "'").Trim('\'');
            
            DataTable dt = null;
            if (hasIncomparable == true)
            {
                String strHexagonSelectIncomparable = additionalParameters[4].Trim().Replace("''", "'").Trim('\'');
                int weightHexagonIncomparable = int.Parse(additionalParameters[5].Trim());
                prefSQL.SQLSkyline.SP_SkylineHexagon skyline = new SQLSkyline.SP_SkylineHexagon();
                

                //Hexagon algorithm neads a higher stack (much recursions). Therefore start it with a new thread
                //Default stack size is 1MB (1024000) --> Increase to 8MB. Otherwise the program might end in a stackoverflow
                var thread = new Thread(
                    () =>
                    {
                        dt = skyline.getSkylineTable(strQuery, strOperators, numberOfRecords, strQueryConstruction, strConnection, strHexagonSelectIncomparable, weightHexagonIncomparable);
                    }, 8000000);


                thread.Start();

                //Join method to block the current thread  until the object's thread terminates.
                thread.Join();
            }
            else
            {
                prefSQL.SQLSkyline.SP_SkylineHexagonLevel skyline = new SQLSkyline.SP_SkylineHexagonLevel();
                

                //Hexagon algorithm neads a higher stack (much recursions). Therefore start it with a new thread

                //Default stack size is 1MB (1024000) --> Increase to 8MB. Otherwise the program might end in a stackoverflow
                var thread = new Thread(
                    () =>
                    {
                        dt = skyline.getSkylineTable(strQuery, strOperators, numberOfRecords, strConnection, strQueryConstruction);
                    }, 8000000);


                thread.Start();

                //Join method to block the current thread  until the object's thread terminates.
                thread.Join();
            }
            return dt;
        }
    }
}
