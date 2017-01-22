using System;
using System.Collections;
using System.Data;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

//Caution: Attention small changes in this code can lead to performance issues, i.e. using a startswith instead of an equal can increase by 10 times
//Important: Only use equal for comparing text (otherwise performance issues)
namespace prefSQL.SQLSkyline
{

    public class SPSkylineHexagon : TemplateHexagon
    {
        [SqlProcedure(Name = "prefSQL_SkylineHexagon")]
        public static void GetSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords, SqlInt32 sortType, SqlString strSelectIncomparable, int weightHexagonIncomparable)
        {
            SPSkylineHexagon skyline = new SPSkylineHexagon();
            string[] additionalParameters = new string[6];
            additionalParameters[4] = strSelectIncomparable.ToString();
            additionalParameters[5] = weightHexagonIncomparable.ToString();
            string preferenceOperators = strOperators.Value;
            string querySQL = strQuery.Value;

            //Change operators array and SQL query (replace INCOMPARABLES values)
            skyline.CalculateOperators(ref preferenceOperators, additionalParameters, Helper.CnnStringSqlclr, Helper.ProviderClr, ref querySQL);

            skyline.GetSkylineTable(querySQL, preferenceOperators, numberOfRecords.Value, false, Helper.CnnStringSqlclr, Helper.ProviderClr, additionalParameters, sortType.Value);
        }

        /// <summary>
        /// Change operators array and SQL query (replace INCOMPARABLES values)
        /// </summary>
        /// <param name="strOperators"></param>
        /// <param name="additionalParameters"></param>
        /// <param name="connectionString"></param>
        /// <param name="factory"></param>
        /// <param name="strSQL"></param>
        public void CalculateOperators(ref string strOperators, string[] additionalParameters, string connectionString, string factory, ref string strSQL)
        {
            string[] strSelectIncomparableAll = additionalParameters[4].Split(';');

            foreach (string strSelectIncomparable in strSelectIncomparableAll)
            {
                if (!strSelectIncomparable.Equals(""))
                {
                    //Check amount of incomparables
                    int posOfFrom = strSQL.IndexOf(" FROM ", StringComparison.OrdinalIgnoreCase)+1;
                    string strSQLIncomparable = "SELECT DISTINCT " + strSelectIncomparable + " " + strSQL.Substring(posOfFrom);

                    DataTable dt = Helper.GetDataTableFromSQL(strSQLIncomparable, connectionString, factory);

                    //Create hexagon single value statement for incomparable tuples
                    string strHexagonIncomparable = "CASE ";
                    int iPosCaseWhen = strSelectIncomparable.IndexOf("CASE WHEN") + 10;
                    int iPosEnd = strSelectIncomparable.Substring(iPosCaseWhen).IndexOf(" ");
                    string strHexagonFieldName = strSelectIncomparable.Substring(iPosCaseWhen, iPosEnd); // "colors.name";
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
                    strSQL = ReplaceFirst(strSQL, "CALCULATEINCOMPARABLE", strAddSQL);
                    //strSQL = strSQL.Replace("CALCULATEINCOMPARABLE", strAddSQL);
                    //strOperators = strOperators.Replace("CALCULATEINCOMPARABLE", strAddOperators);
                    strOperators = ReplaceFirst(strOperators, "CALCULATEINCOMPARABLE", strAddOperators);



                }
            }
        }


        private string ReplaceFirst(string text, string search, string replace)
        {
            int pos = text.IndexOf(search);
            if (pos < 0)
            {
                return text;
            }
            return text.Substring(0, pos) + replace + text.Substring(pos + search.Length);
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
