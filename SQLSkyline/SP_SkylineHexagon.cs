using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

//Caution: Attention small changes in this code can lead to performance issues, i.e. using a startswith instead of an equal can increase by 10 times
//Important: Only use equal for comparing text (otherwise performance issues)
namespace prefSQL.SQLSkyline
{

    public class SP_SkylineHexagon : TemplateHexagon
    {        
        [SqlProcedure(Name = "SP_SkylineHexagon")]
        public static void GetSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords, SqlString strSelectIncomparable, int weightHexagonIncomparable)
        {
            SP_SkylineHexagon skyline = new SP_SkylineHexagon();
            skyline.GetSkylineTable(strQuery.ToString(), strOperators.ToString(), numberOfRecords.Value, false, Helper.CnnStringSqlclr, Helper.ProviderClr, strSelectIncomparable.ToString(), weightHexagonIncomparable);
        }


        protected override void CalculateOperators(ref string strOperators, string strSelectIncomparable, DbProviderFactory factory, DbConnection connection, ref string strSQL)
        {
            if (!strSelectIncomparable.Equals(""))
            {
                //Check amount of incomparables
                connection.Open();

                int posOfFrom;
                posOfFrom = strSQL.IndexOf("FROM", StringComparison.Ordinal);
                string strSQLIncomparable = "SELECT DISTINCT " + strSelectIncomparable + " " + strSQL.Substring(posOfFrom);

                DbDataAdapter dap = factory.CreateDataAdapter();
                DbCommand selectCommand = connection.CreateCommand();
                selectCommand.CommandTimeout = 0; //infinite timeout
                selectCommand.CommandText = strSQLIncomparable;
                if (dap != null)
                {
                    dap.SelectCommand = selectCommand;
                    DataTable dt = new DataTable();
                    dap.Fill(dt);


                    //Create hexagon single value statement for incomparable tuples
                    string strHexagonIncomparable = "CASE ";
                    string strHexagonFieldName = "colors.name";
                    int amountOfIncomparable = 0;
                    string strAddOperators = "";
                    int iIndexRow = 0;
                    foreach (DataRow row in dt.Rows)
                    {
                        string strCategory = (string)row[0];
                        if (!strCategory.Equals(""))
                        {
                            //string strBitPattern = new String('0', dt.Rows.Count - 1);
                            string strBitPattern = new String('0', dt.Rows.Count - 1);
                            strBitPattern = strBitPattern.Substring(0, amountOfIncomparable) + "1" + strBitPattern.Substring(amountOfIncomparable + 1);
                            strHexagonIncomparable += " WHEN " + strHexagonFieldName + " = '" + strCategory.Replace("(", "").Replace(")", "") + "' THEN '" + strBitPattern + "'";
                            amountOfIncomparable++;

                            //if (iIndexRow > 0)
                            //{
                            strAddOperators += "INCOMPARABLE;";
                            //}
                            iIndexRow++;
                        }

                    }
                    strAddOperators = strAddOperators.TrimEnd(';');

                    string strBitPatternFull = new String('x', amountOfIncomparable); // string of 20 spaces;
                    strHexagonIncomparable += " ELSE '" + strBitPatternFull + "' END AS HexagonIncomparable" + strHexagonFieldName.Replace(".", "");


                    string strAddSQL = "";
                    iIndexRow = 0;
                    foreach (DataRow row in dt.Rows)
                    {
                        string strCategory = (string)row[0];
                        if (!strCategory.Equals(""))
                        {
                            //if (iIndexRow > 0)
                            //{
                            strAddSQL += strHexagonIncomparable + iIndexRow + ",";
                            //}
                            iIndexRow++;
                        }

                    }
                    strAddSQL = strAddSQL.TrimEnd(',');


                    //Manipulate construction sql
                    strSQL = strSQL.Replace("CALCULATEINCOMPARABLE", strAddSQL);
                    strOperators = strOperators.Replace("CALCULATEINCOMPARABLE", strAddOperators);
                }

                if (connection != null)
                    connection.Close();

            }
        }

        protected override void Add(object[] dataReader, int amountOfPreferences, string[] operators, ref ArrayList[] btg, ref int[] weight, ref long maxID, int weightHexagonIncomparable) //add tuple
        {
            ArrayList al = new ArrayList();

            //create int array from dataTableReader
            long[] tuple = new long[operators.GetUpperBound(0) + 1];
            for (int iCol = 0; iCol <= dataReader.GetUpperBound(0); iCol++)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol <= operators.GetUpperBound(0))
                {
                    //LOW und HIGH Spalte in record abfüllen
                    if (operators[iCol].Equals("LOW"))
                    {
                        tuple[iCol] = (long)dataReader[iCol];

                        //Check if long value is incomparable
                        if (iCol + 1 <= tuple.GetUpperBound(0) && operators[iCol + 1].Equals("INCOMPARABLE"))
                        {
                            //Incomparable field is always the next one
                            String strValue = (string)dataReader[iCol + 1];
                            if (strValue.Substring(0, 1).Equals("x"))
                            {
                                //current level is ok, but add zeros if before incomparables, otherwise fill with ones
                                for (int iValue = 0; iValue < strValue.Length; iValue++)
                                {
                                    if (tuple[iCol] <= weightHexagonIncomparable)
                                    {
                                        tuple[iCol + 1 + iValue] = 0; //diese sind besser als die unvergleichbaren
                                    }
                                    else
                                    {
                                        tuple[iCol + 1 + iValue] = 1; //diese sind schlechter als die unvergleichbaren
                                    }

                                }

                            }
                            else
                            {
                                //Overwrite current level value with new one
                                for (int iValue = 0; iValue < strValue.Length; iValue++)
                                {
                                    tuple[iCol + 1 + iValue] = long.Parse(strValue.Substring(iValue, 1));
                                }
                            }
                        }

                    }

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
