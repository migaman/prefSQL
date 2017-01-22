using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using prefSQL.SQLParser.Models;

namespace prefSQL.SQLParser
{
    //internal class
    class SQLCriterion
    {
        /// <summary>
        /// Create the WHERE-Clause according to the preference model
        /// </summary>
        /// <param name="model"></param>
        /// <param name="strPreSQL"></param>
        /// <returns></returns>
        public string GetCriterionClause(PrefSQLModel model, string strPreSQL)
        {
            //Build Skyline only if more than one attribute
            string strSQL = GetCriterionSkylineClause(model, strPreSQL);

            //Check if a WHERE-Clause was built
            if (strSQL.Length > 0)
            {
                //Only add WHERE if there is not already a where clause
                bool isWherePresent = strPreSQL.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase) > 0;
                if (isWherePresent)
                {
                    strSQL = " AND " + strSQL;
                }
                else
                {
                    strSQL = " WHERE " + strSQL;
                }
            }

            return strSQL;
        }



        /// <summary>
        /// Build the WHERE Clause to implement a Skyline
        /// </summary>
        /// <param name="model"></param>
        /// <param name="strPreSQL"></param>
        /// <returns></returns>
        private string GetCriterionSkylineClause(PrefSQLModel model, string strPreSQL)
        {
            string strWhereEqual;
            string strWhereBetter = " AND ( ";
            string strSQL = "";

            //Only add WHERE if there is not already a where clause
            bool isWherePresent = strPreSQL.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase) > 0;
            if (isWherePresent)
            {
                strWhereEqual = " AND ";
            }
            else
            {
                strWhereEqual = "WHERE ";
            }


            //Build the where clause with each column in the skyline
            for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
            {
                //Competition
                bool needsTextOrClause = model.Skyline[iChild].IsCategorical;

                //First child doesn't need an OR/AND
                if (iChild > 0)
                {
                    strWhereEqual += " AND ";
                    strWhereBetter += " OR ";
                }

                //Falls Text-Spalte ein zusätzliches OR einbauen für den Vergleich Farbe = Farbe
                if (needsTextOrClause)
                {
                    strWhereEqual += "(";
                }

                strWhereEqual += "{INNERcolumn} <= {column}";
                strWhereBetter += "{INNERcolumn} < {column}";

                strWhereEqual = strWhereEqual.Replace("{INNERcolumn}", model.Skyline[iChild].InnerExpression);
                strWhereBetter = strWhereBetter.Replace("{INNERcolumn}", model.Skyline[iChild].InnerExpression);
                strWhereEqual = strWhereEqual.Replace("{column}", model.Skyline[iChild].Expression);
                strWhereBetter = strWhereBetter.Replace("{column}", model.Skyline[iChild].Expression);

                //Falls Text-Spalte ein zusätzliches OR einbauen für den Vergleich Farbe = Farbe
                if (needsTextOrClause)
                {
                    strWhereEqual += " OR " + model.Skyline[iChild].InnerFullColumnName + " = " + model.Skyline[iChild].FullColumnName;
                    strWhereEqual += ")";
                }


            }
            //closing bracket for 2nd condition
            strWhereBetter += ") ";

            //Format strPreSQL
            foreach (KeyValuePair<string, string> table in model.Tables)
            {
                //Add ALIAS to tablename (Only if not already an ALIAS was set)
                if (table.Value.Equals(""))
                {
                    //Replace tablename (for fields)
                    strPreSQL = strPreSQL.Replace(table.Key + ".", table.Key + "_INNER.");
                    string pattern = @"\b" + table.Key + @"\b";
                    string replace = table.Key + " " + table.Key + "_INNER";
                    strPreSQL = Regex.Replace(strPreSQL, pattern, replace, RegexOptions.IgnoreCase);
                }
                else
                {
                    //Replace tablename (for fields)
                    strPreSQL = Regex.Replace(strPreSQL, @"\b" + table.Value + @"\.", table.Value + "_INNER.", RegexOptions.IgnoreCase);
                    //Replace ALIAS
                    string pattern = @"\b" + table.Value + @"\b";
                    string replace = table.Value + "_INNER";
                    strPreSQL = Regex.Replace(strPreSQL, pattern, replace, RegexOptions.IgnoreCase);
                }
            }

            //Check if SQL contains TOP Keywords
            if (model.NumberOfRecords != 0)
            {
                //Remove Top Keyword in inner clause
                int iPosTop = strPreSQL.IndexOf(" TOP ", StringComparison.OrdinalIgnoreCase)+1;
                int iPosTopEnd = strPreSQL.Substring(iPosTop + 3).TrimStart().IndexOf(" ", StringComparison.Ordinal);
                string strSQLAfterTop = strPreSQL.Substring(iPosTop + 3).TrimStart();
                strPreSQL = strPreSQL.Substring(0, iPosTop) + strSQLAfterTop.Substring(iPosTopEnd + 1);
            }


            strSQL += "NOT EXISTS(" + strPreSQL + " " + strWhereEqual + strWhereBetter + ")";
            return strSQL;
        }



    }
}
