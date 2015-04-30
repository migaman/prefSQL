using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Data.Common;


//Caution: Attention small changes in this code can lead to performance issues, i.e. using a startswith instead of an equal can increase by 10 times
//Important: Only use equal for comparing text (otherwise performance issues)
namespace prefSQL.SQLSkyline
{

    public class SP_SkylineHexagonLevel : TemplateHexagon
    {
        [Microsoft.SqlServer.Server.SqlProcedure(Name = "SP_SkylineHexagonLevel")]
        public static void getSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords)
        {
            SP_SkylineHexagonLevel skyline = new SP_SkylineHexagonLevel();
            skyline.getSkylineTable(strQuery.ToString(), strOperators.ToString(), numberOfRecords.Value, false, Helper.cnnStringSQLCLR, Helper.ProviderCLR, "", 0);
        }

        protected override void calculateOperators(ref string strOperators, string strSelectIncomparable, DbProviderFactory factory, DbConnection connection, ref string strSQL)
        {
            //No Operation
            return;
        }

        protected override void add(object[] dataReader, int amountOfPreferences, string[] operators, ref ArrayList[] btg, ref int[] weight, ref long maxID, int weightHexagonIncomparable)
        {
            ArrayList al = new ArrayList();

            //create int array from dataTableReader
            long[] tuple = new long[operators.GetUpperBound(0) + 1];
            for (int iCol = 0; iCol <= dataReader.GetUpperBound(0); iCol++)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol <= operators.GetUpperBound(0))
                {
                    tuple[iCol] = (long)dataReader[iCol];
                }
                else
                {
                    al.Add(dataReader[iCol]);
                }

            }

            //1: procedure add(tuple)
            // compute the node ID for the tuple
            long id = 0;
            for (int i = 0; i < amountOfPreferences; i++)
            {
                //id = id + levelPi(tuple) * weight(i);
                id = id + tuple[i] * weight[i];
            }

            // add tuple to its node
            if (btg[id] == null)
            {
                btg[id] = new ArrayList();
            }
            btg[id].Add(al);


            if (id > maxID)
            {
                maxID = id;
            }
        }


    }
}
