using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using prefSQL.SQLParser.Models;
using System.Text.RegularExpressions;

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
        public string getCriterionClause(PrefSQLModel model, string strPreSQL)
        {
            string strSQL = "";
            bool isWHEREPresent = false;

            //Build Skyline only if more than one attribute
            strSQL = getCriterionSkylineClause(model, strPreSQL);

            //Check if a WHERE-Clause was built
            if (strSQL.Length > 0)
            {
                //Only add WHERE if there is not already a where clause
                isWHEREPresent = strPreSQL.IndexOf("WHERE") > 0;
                if (isWHEREPresent == true)
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
        private string getCriterionSkylineClause(PrefSQLModel model, string strPreSQL)
        {
            string strWhereEqual = "";
            string strWhereBetter = " AND ( ";
            string strSQL = "";
            bool isWHEREPresent = false;

            //Only add WHERE if there is not already a where clause
            isWHEREPresent = strPreSQL.IndexOf("WHERE") > 0;
            if (isWHEREPresent == true)
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
                bool needsTextORClause = false;

                //Competition
                needsTextORClause = model.Skyline[iChild].IsCategory;

                //First child doesn't need an OR/AND
                if (iChild > 0)
                {
                    strWhereEqual += " AND ";
                    strWhereBetter += " OR ";
                }

                //Falls Text-Spalte ein zusätzliches OR einbauen für den Vergleich Farbe = Farbe
                if (needsTextORClause == true)
                {
                    strWhereEqual += "(";
                }

                strWhereEqual += "{INNERcolumn} " + model.Skyline[iChild].Op + "= {column}";
                strWhereBetter += "{INNERcolumn} " + model.Skyline[iChild].Op + " {column}";

                strWhereEqual = strWhereEqual.Replace("{INNERcolumn}", model.Skyline[iChild].InnerColumnExpression);
                strWhereBetter = strWhereBetter.Replace("{INNERcolumn}", model.Skyline[iChild].InnerColumnExpression);
                strWhereEqual = strWhereEqual.Replace("{column}", model.Skyline[iChild].ColumnExpression);
                strWhereBetter = strWhereBetter.Replace("{column}", model.Skyline[iChild].ColumnExpression);

                //Falls Text-Spalte ein zusätzliches OR einbauen für den Vergleich Farbe = Farbe
                if (needsTextORClause == true)
                {
                    strWhereEqual += " OR " + model.Skyline[iChild].InnerColumnName + " = " + model.Skyline[iChild].FullColumnName;
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
                    strPreSQL = strPreSQL.Replace(table.Value + ".", table.Value + "_INNER.");
                    //Replace ALIAS
                    string pattern = @"\b" + table.Value + @"\b";
                    string replace = table.Value + "_INNER";
                    strPreSQL = Regex.Replace(strPreSQL, pattern, replace, RegexOptions.IgnoreCase);
                }
            }

            //Check if SQL contains TOP Keywords
            if (model.HasTop == true)
            {
                //Remove Top Keyword in inner clause
                int iPosTop = strPreSQL.IndexOf("TOP");
                int iPosTopEnd = strPreSQL.Substring(iPosTop + 3).TrimStart().IndexOf(" ");
                string strSQLAfterTOP = strPreSQL.Substring(iPosTop + 3).TrimStart();
                strPreSQL = strPreSQL.Substring(0, iPosTop) + strSQLAfterTOP.Substring(iPosTopEnd + 1);
            }


            strSQL += "NOT EXISTS(" + strPreSQL + " " + strWhereEqual + strWhereBetter + ") ";
            return strSQL;
        }



    }
}
