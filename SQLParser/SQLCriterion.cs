using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using prefSQL.SQLParser.Models;
using System.Text.RegularExpressions;

namespace prefSQL.SQLParser
{
    class SQLCriterion
    {

        //Create the WHERE-Clause from the preferene model
        public string getCriterionClause(PrefSQLModel model, string strPreSQL)
        {
            string strSQL = "";
            bool isWHEREPresent = false;

            //Build Skyline only if more than one attribute
            if (model.Skyline.Count > 1)
            {
                strSQL = getCriterionSkylineClause(model, strPreSQL);
            }
            else if (model.Rank.Count > 1)
            {
                strSQL = getCriterionRankClause(model);

            }

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




        /**
         * Build the WHERE Clause for a PRIORITIZE Preference SQL statement
         * 
         * */
        private string getCriterionRankClause(PrefSQLModel model)
        {
            string strSQL = "";

            //Build the where clause with each column in the skyline
            for (int iChild = 0; iChild < model.Rank.Count; iChild++)
            {
                //First child doesn't need an OR
                if (iChild > 0)
                {
                    strSQL += " OR ";
                }
                strSQL += "Rank" + model.Rank[iChild].ColumnName + " = 1";
            }

            return strSQL;
        }

        /**
         *  Build the WHERE Clause to implement a Skyline
         * 
         * 
         * */
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
                needsTextORClause = !model.Skyline[iChild].ColumnName.Equals("");

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
                    strWhereEqual += " OR " + model.Skyline[iChild].InnerColumnName + " = " + model.Skyline[iChild].ColumnName;
                    strWhereEqual += ")";
                }


            }
            //closing bracket for 2nd condition
            strWhereBetter += ") ";

            //Format strPreSQL
            foreach (string strTable in model.Tables)
            {
                //Replace tablename 
                strPreSQL = strPreSQL.Replace(strTable + ".", strTable + "_INNER.");

                //Add ALIAS to tablename (Only if not already an ALIAS was set)
                if (model.TableAliasName.Equals(""))
                {
                    string pattern = @"\b" + strTable + @"\b";
                    string replace = strTable + " " + strTable + "_INNER";
                    strPreSQL = Regex.Replace(strPreSQL, pattern, replace, RegexOptions.IgnoreCase);
                }
                else
                {
                    //Replace ALIAS
                    string pattern = @"\b" + strTable + @"\b";
                    string replace = strTable + "_INNER";
                    strPreSQL = Regex.Replace(strPreSQL, pattern, replace, RegexOptions.IgnoreCase);
                }
            }

            //Check if SQL contains TOP Keywords
            if (model.IncludesTOP == true)
            {
                //Remove Top Keyword
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
