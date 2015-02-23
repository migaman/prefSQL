using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Text;
using System.Threading.Tasks;
using Antlr4.Runtime.Tree;
using prefSQL.SQLParser.Models;


namespace prefSQL.SQLParser
{
    //internal class
    class SQLVisitor : SQLBaseVisitor<PrefSQLModel>
    {
        private Dictionary<string, string> tables = new Dictionary<string, string>();
        private bool includesTOP = false;                       //If SQL statement contains the "TOP" keyword it will be set to true
        private const string InnerTableSuffix = "_INNER";       //Table suffix for the inner query
        private const string RankingFunction = "ROW_NUMBER()";  //Default Ranking function
        private PrefSQLModel model;                             //Preference SQL Model, contains i.e. the skyline attributes
        private bool isNative;                                  //True if the skyline algorithm is native                 
        private bool withIncomparable = false;                  //True if the query must check for incomparable tuples


        public bool IsNative
        {
            get { return isNative; }
            set { isNative = value; }
        }
        

        public PrefSQLModel Model
        {
            get { return model; }
            set { model = value; }
        }

        /// <summary>
        /// Normal ORDER BY clause
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitOrderByDefault(SQLParser.OrderByDefaultContext context)
        {
            model.Ordering = SQLCommon.Ordering.AsIs;
            return base.VisitOrderByDefault(context);
        }

        /// <summary>
        /// Order BY clause with prefSQL keywords (SUMRANK or BESTRANK)
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitOrderBySpecial(SQLParser.OrderBySpecialContext context)
        {
            SQLCommon.Ordering ordering = SQLCommon.Ordering.AsIs;
            if (context.op.Type == SQLParser.K_SUMRANK)
            {
                ordering = SQLCommon.Ordering.RankingSummarize;
            }
            else if (context.op.Type == SQLParser.K_BESTRANK)
            {
                ordering = SQLCommon.Ordering.RankingBestOf;
            }
            model.Ordering = ordering;

            return base.VisitOrderBySpecial(context);
        }


        /// <summary>
        /// ORDER BY clause with syntactic sugar of prefSQL
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitOrderbyCategory(SQLParser.OrderbyCategoryContext context)
        {
            string strSQL = "";
            string strColumnName = "";
            string strTable = "";

            //Build CASE ORDER with arguments
            string strExpr = context.exprCategory().GetText();
            strColumnName = getColumnName(context.GetChild(0));
            strTable = getTableName(context.GetChild(0));
            string[] strTemp = Regex.Split(strExpr, @"(==|>>)"); //Split signs are == and >>
            string strSQLOrderBy = "";
            string strSQLELSE = "";



            //Define sort order value for each attribute
            int iWeight = 0;
            for (int i = 0; i < strTemp.GetLength(0); i++)
            {
                switch (strTemp[i])
                {
                    case ">>":
                        iWeight += 100; //Gewicht erhöhen, da >> Operator
                        break;
                    case "==":
                        break;  //Gewicht bleibt gleich da == Operator
                    case "OTHERSINCOMPARABLE":
                        ////Add one, so that equal-clause cannot be true with same level-values, but other names
                        strSQLELSE = " ELSE " + (iWeight);
                        break;
                    case "OTHERSEQUAL":
                        //Special word OTHERS EQUAL = all other attributes are defined with this order by value
                        strSQLELSE = " ELSE " + iWeight;
                        break;
                    default:
                        //Check if it contains multiple values
                        if (strTemp[i].StartsWith("{"))
                        {
                            //Multiple values --> construct IN statement
                            strTemp[i] = strTemp[i].Replace("{", "(").Replace("}", ")");
                            strSQLOrderBy += " WHEN " + strTable + "." + strColumnName + " IN " + strTemp[i] + " THEN " + iWeight.ToString();
                        }
                        else
                        {
                            //Single value --> construct = statement
                            strSQLOrderBy += " WHEN " + strTable + "." + strColumnName + " = " + strTemp[i] + " THEN " + iWeight.ToString();
                        }
                        break;
                }

                //Always Sort ASCENDING
                strSQL = "CASE" + strSQLOrderBy + strSQLELSE + " END ASC";
            }

            OrderByModel orderByModel = new OrderByModel();
            orderByModel.start = context.start.StartIndex;
            orderByModel.stop = context.stop.StopIndex+1;
            orderByModel.text = strSQL;
            model.OrderBy.Add(orderByModel);
            return base.VisitOrderbyCategory(context);


        }


        /// <summary>
        /// Adds the table name and alias for each table in the query
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitTable_or_subquery(SQLParser.Table_or_subqueryContext context)
        {
            string strTable = context.GetChild(0).GetText();
            string strTableAlias = "";
            if (context.ChildCount == 2)
            {
                strTableAlias = context.GetChild(1).GetText();
            }
            else if (context.ChildCount == 3) //ALIAS introduced with "AS"-Keyword
            {
                strTableAlias = context.GetChild(2).GetText();
            }
            tables.Add(strTable, strTableAlias);

            return base.VisitTable_or_subquery(context);
        }

        /// <summary>
        /// Set a boolean variable if query contains TOP keyword
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitTop_keyword(SQLParser.Top_keywordContext context)
        {
            includesTOP = true;
            return base.VisitTop_keyword(context);
        }

        
        /// <summary>
        /// Handles a categorical preference
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitPreferenceCategory(SQLParser.PreferenceCategoryContext context)
        {
            //It is a text --> Text text must be converted in a given sortorder
            PrefSQLModel pref = new PrefSQLModel();
            string strSQL = "";
            string strColumnName = "";
            string strTable = "";
            string strOperator = "";
            string strRankColumn = "";
            string strRankHexagon = "";
            string strHexagonIncomparable = "";
            int amountOfIncomparable = 0;

            //Build CASE ORDER with arguments
            string strExpr = context.exprCategory().GetText();
            string strColumnExpression = "";
            strColumnName = getColumnName(context.GetChild(0));
            strTable = getTableName(context.GetChild(0));
            string[] strTemp = Regex.Split(strExpr, @"(==|>>)"); //Split signs are == and >>
            string strSQLOrderBy = "";
            string strSQLELSE = "";
            string strSQLInnerELSE = "";
            string strSQLInnerOrderBy = "";
            string strInnerColumn = "";
            string strSingleColumn = strTable + "." + getColumnName(context.GetChild(0));
            string strInnerSingleColumn = strTable + InnerTableSuffix + "." + getColumnName(context.GetChild(0));
            string strSQLIncomparableAttribute = "";
            string strIncomporableAttribute = "";
            string strIncomporableAttributeELSE = "";
            bool bComparable = true;
            int weightHexagonIncomparable = 0;

            //Define sort order value for each attribute
            int iWeight = 0;
            for (int i = 0; i < strTemp.GetLength(0); i++)
            {
                switch (strTemp[i])
                {
                    case ">>":
                        iWeight += 100; //Gewicht erhöhen, da >> Operator
                        break;
                    case "==":
                        break;  //Gewicht bleibt gleich da == Operator
                    case "OTHERSINCOMPARABLE":
                        ////Add one, so that equal-clause cannot be true with same level-values, but other names
                        strSQLELSE = " ELSE " + (iWeight);
                        strSQLInnerELSE = " ELSE " + (iWeight + 1);
                        strIncomporableAttributeELSE = " ELSE " + strTable + "." + strColumnName; //Not comparable --> give string value of field
                        bComparable = false;
                        amountOfIncomparable = 99; //set a certain amount
                        strHexagonIncomparable = "CALCULATEINCOMPARABLE";
                        weightHexagonIncomparable = iWeight / 100;
                        withIncomparable = true;
                        break;
                    case "OTHERSEQUAL":
                        //Special word OTHERS EQUAL = all other attributes are defined with this order by value
                        strSQLELSE = " ELSE " + iWeight;
                        strSQLInnerELSE = " ELSE " + iWeight;
                        strIncomporableAttributeELSE = " ELSE ''"; //Comparable give empty column
                        break;
                    default:
                        //Check if it contains multiple values
                        if (strTemp[i].StartsWith("{"))
                        {
                            //Multiple values --> construct IN statement
                            strTemp[i] = strTemp[i].Replace("{", "(").Replace("}", ")");
                            strSQLOrderBy += " WHEN " + strTable + "." + strColumnName + " IN " + strTemp[i] + " THEN " + iWeight.ToString();
                            //This values are always incomparable (otherwise the = should be used)
                            strSQLInnerOrderBy += " WHEN " + strTable + InnerTableSuffix + "." + strColumnName + " IN " + strTemp[i] + " THEN " + (iWeight + 1);
                            //Not comparable --> give string value of field
                            strSQLIncomparableAttribute += " WHEN " + strTable + "." + strColumnName + " IN " + strTemp[i] + " THEN " + strTable + "." + strColumnName;

                            //Create hexagon single value statement for incomparable tuples
                            strHexagonIncomparable = "CASE ";
                            amountOfIncomparable = 0;
                            foreach (String strCategory in strTemp[i].Split(','))
                            {
                                string strBitPattern = new String('0', strTemp[i].Split(',').GetUpperBound(0)+1);
                                strBitPattern = strBitPattern.Substring(0, amountOfIncomparable) + "1" + strBitPattern.Substring(amountOfIncomparable + 1);
                                strHexagonIncomparable += " WHEN " + strTable + "." + strColumnName + " = " + strCategory.Replace("(", "").Replace(")", "") + " THEN '" + strBitPattern + "'";
                                amountOfIncomparable++;
                            }
                            string strBitPatternFull = new String('x', amountOfIncomparable); // string of 20 spaces;
                            strHexagonIncomparable += " ELSE '" + strBitPatternFull + "' END AS HexagonIncomparable" + strSingleColumn.Replace(".", "");
                            withIncomparable = true; //the values inside the bracket are incomparable
                        }
                        else
                        {
                            //Single value --> construct = statement
                            strSQLOrderBy += " WHEN " + strTable + "." + strColumnName + " = " + strTemp[i] + " THEN " + iWeight.ToString();
                            //This values are always comparable (otherwise the {x, y} should be used)
                            strSQLInnerOrderBy += " WHEN " + strTable + InnerTableSuffix + "." + strColumnName + " = " + strTemp[i] + " THEN " + iWeight.ToString();
                            strSQLIncomparableAttribute += " WHEN " + strTable + "." + strColumnName + " = " + strTemp[i] + " THEN ''"; //comparable
                        }
                        break;
                }

            }

            //Add others incomparable clause at the top-level if not OTHERS was specified
            if (strSQLELSE.Equals("") && IsNative == false)
            {
                strIncomporableAttributeELSE = " ELSE " + strTable + "." + strColumnName; //Not comparable --> give string value of field
                bComparable = false;
            }
            /*if (strSQLELSE.Equals("") && IsNative == false)
            {
                //No OTHERS-clause available -- This means all other elements are incomparable --> Add it to the beginning
                iWeight = 0;
                strSQLELSE = " ELSE " + (iWeight);
                strSQLInnerELSE = " ELSE " + (iWeight + 1);
                strIncomporableAttributeELSE = "ELSE " + strTable + "." + strColumnName; //Not comparable --> give string value of field
                bComparable = false;

            }*/
            strSQL = "CASE" + strSQLOrderBy + strSQLELSE + " END";
            strInnerColumn = "CASE" + strSQLInnerOrderBy + strSQLInnerELSE + " END";
            strIncomporableAttribute = "CASE" + strSQLIncomparableAttribute + strIncomporableAttributeELSE + " END";
            //strSelectDistinctIncomparable = "CASE" + strSelectDistinctIncomparable + strSelectDistinctElse + " END";
            strColumnExpression = strSQL;
            //Categories are always sorted ASCENDING
            strSQL += " ASC";
            strOperator = "<";

            strRankColumn = RankingFunction + " over (ORDER BY " + strSQL + ")";
            strRankHexagon = "DENSE_RANK()" + " over (ORDER BY " + strSQL + ")-1 AS Rank" + strSingleColumn.Replace(".", "");
            //strHexagonIncomparable = "CASE WHEN  colors.name IN ('blau') THEN '001' WHEN colors.name IN ('silber') THEN '100' WHEN colors.name IN ('rot') THEN '010' ELSE '111' END AS RankColorNew";
            //Add the preference to the list               
            pref.Skyline.Add(new AttributeModel(strColumnExpression, strOperator, strInnerColumn, strSingleColumn, strInnerSingleColumn, bComparable, strIncomporableAttribute, strSingleColumn.Replace(".", ""), strRankColumn, strRankHexagon, strSQL, true, strColumnName, strHexagonIncomparable, amountOfIncomparable, weightHexagonIncomparable));



            //pref.OrderBySkyline.Add(strSQL);
            pref.Tables = tables;
            pref.HasSkyline = true;
            pref.WithIncomparable = withIncomparable;
            model = pref;
            return pref;
        }

        /// <summary>
        /// Handles a numerical/date HIGH/LOW preference
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitPreferenceLOWHIGH(SQLParser.PreferenceLOWHIGHContext context)
        {
            string strSQL = "";
            PrefSQLModel pref = new PrefSQLModel();
            bool isLevelStepEqual = true;
            string strColumnName = "";
            string strColumnExpression = "";
            string strInnerColumnExpression = "";
            string strFullColumnName = "";
            string strTable = "";
            string strOperator = "";
            string strRankColumn = "";
            string strRankHexagon = "";
            string strLevelStep = "";
            string strLevelAdd = "";
            string strLevelMinus = "";
            bool bComparable = true;
            string strIncomporableAttribute = "";
            
            //Separate Column and Table
            strColumnName = getColumnName(context.GetChild(0));
            strTable = getTableName(context.GetChild(0));
            strFullColumnName = strTable + "." + strColumnName;
            
            if (context.ChildCount == 4)
            {
                //If a third parameter is given, it is the Level Step  (i.e. LOW price 1000 means prices divided through 1000)
                //The user doesn't care about a price difference of 1000
                //This results in a smaller skyline
                strLevelStep = " / " + context.GetChild(2).GetText();
                strLevelAdd = " + " + context.GetChild(2).GetText();
                strLevelMinus = " - " + context.GetChild(2).GetText();
                if (context.GetChild(3).GetText().Equals("EQUAL"))
                {
                    isLevelStepEqual = true;
                    bComparable = true;
                }
                else
                {
                    isLevelStepEqual = false;
                    bComparable = false;
                    withIncomparable = true;
                }
                
            }
            //Keyword LOW or HIGH, build ORDER BY
            if (context.op.Type == SQLParser.K_LOW)
            {
                strSQL = strColumnName + strLevelStep + " ASC";
                strOperator = "<";
                strRankHexagon = "DENSE_RANK()" + " over (ORDER BY " + strFullColumnName + strLevelStep + " ASC)-1 AS Rank" + strColumnName;
                strRankColumn = RankingFunction + " over (ORDER BY " + strFullColumnName + " ASC)";
                strColumnExpression = strTable + "." + strColumnName + strLevelStep;
                if (isLevelStepEqual == true)
                {
                    strInnerColumnExpression = strTable + InnerTableSuffix + "." + strColumnName + strLevelStep;
                }
                else
                {   
                    //Values from the same step are Incomparable
                    strInnerColumnExpression = "(" + strTable + InnerTableSuffix + "." + strColumnName + strLevelAdd + ")" + strLevelStep;
                    strIncomporableAttribute = "'INCOMPARABLE'";
                }
                
            }
            else if (context.op.Type == SQLParser.K_HIGH)
            {
                strSQL = strColumnName + strLevelStep + " DESC";
                strOperator = ">";
                strRankHexagon = "DENSE_RANK()" + " over (ORDER BY " + strFullColumnName + strLevelStep + " DESC)-1 AS Rank" + strColumnName;
                strRankColumn = RankingFunction + " over (ORDER BY " + strFullColumnName + " DESC)";
                strColumnExpression = strTable + "." + strColumnName + strLevelStep;
                if (isLevelStepEqual == true)
                {
                    strInnerColumnExpression = strTable + InnerTableSuffix + "." + strColumnName + strLevelStep;
                }
                else
                {
                    //Values from the same step are Incomparable
                    strInnerColumnExpression = "(" + strTable + InnerTableSuffix + "." + strColumnName + strLevelMinus + ")" + strLevelStep;
                }
            }
            else if (context.op.Type == SQLParser.K_LOWDATE)
            {
                strSQL = strColumnName + " ASC";
                strOperator = ">";
                strRankHexagon = "DENSE_RANK()" + " over (ORDER BY DATEDIFF(minute, '1900-01-01', " + strFullColumnName + ") " + strLevelStep + " ASC)-1 AS Rank" + strColumnName;
                strRankColumn = RankingFunction + " over (ORDER BY " + strFullColumnName + " DESC)";
                strColumnExpression = "DATEDIFF(minute, '1900-01-01', " + strTable + "." + strColumnName + ")";
                strInnerColumnExpression = "DATEDIFF(minute, '1900-01-01', " + strTable + InnerTableSuffix + "." + strColumnName + ")"; 
            }
            else if (context.op.Type == SQLParser.K_HIGHDATE)
            {
                strSQL = strColumnName + " DESC";
                strOperator = ">";
                strRankHexagon = "DENSE_RANK()" + " over (ORDER BY DATEDIFF(minute, '1900-01-01', " + strFullColumnName + ") " + strLevelStep + " DESC)-1 AS Rank" + strColumnName;
                strRankColumn = RankingFunction + " over (ORDER BY " + strFullColumnName + " DESC)";
                strColumnExpression = "DATEDIFF(minute, '1900-01-01', " + strTable + "." + strColumnName + ") " + strLevelStep;
                strInnerColumnExpression = "DATEDIFF(minute, '1900-01-01', " + strTable + InnerTableSuffix + "." + strColumnName + ") " + strLevelStep; 
            }


            //Add the preference to the list               
            pref.Skyline.Add(new AttributeModel(strColumnExpression, strOperator, strInnerColumnExpression, strFullColumnName, "", bComparable, strIncomporableAttribute, strColumnName, strRankColumn, strRankHexagon, strSQL, false, strColumnName, "", 0));
            pref.HasSkyline = true;
            pref.Tables = tables;
            pref.WithIncomparable = withIncomparable;
            model = pref;
            return pref;


        }

        /// <summary>
        /// Combines multiple pareto preferences (each preference is equivalent)
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitExprAnd(SQLParser.ExprAndContext context)
        {
            //And was used --> visit left and right node
            PrefSQLModel left = Visit(context.exprSkyline(0));
            PrefSQLModel right = Visit(context.exprSkyline(1));
            
            //Add the columns to the preference model
            PrefSQLModel pref = new PrefSQLModel();
            pref.Skyline.AddRange(left.Skyline);
            pref.Skyline.AddRange(right.Skyline);
            pref.Tables = tables;
            pref.HasTop = includesTOP;
            pref.HasSkyline = true;
            pref.WithIncomparable = withIncomparable;
            model = pref;
            return pref;


            //return base.VisitExprAnd(context);
        }

        
        /// <summary>
        /// Handles around preferences
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitPreferenceAROUND(SQLParser.PreferenceAROUNDContext context)
        {
            string strSQL = "";
            PrefSQLModel pref = new PrefSQLModel();

            string strColumn = "";
            string strFullColumnName = "";
            string strColumnExpression = "";
            string strTable = "";
            string strOperator = "";
            string strInnerColumnExpression = "";

            //Query Keywords AROUND, FAVOUR and DISFAVOUR, after that create an ORDER BY of it

            strColumn = getColumnName(context.GetChild(0));
            strTable = getTableName(context.GetChild(0));
            strFullColumnName = strTable + "." + strColumn;

            switch (context.op.Type)
            {
                case SQLParser.K_AROUND:
                    
                    //Value should be as close as possible to a given numeric value
                    //Check if its a geocoordinate
                    if (context.GetChild(2).GetType().ToString() == "prefSQL.SQLParser.SQLParser+GeocoordinateContext")
                    {
                        strSQL = "ABS(DISTANCE(" + context.GetChild(0).GetText() + ", \"" + context .GetChild(2).GetChild(1).GetText() + "," + context.GetChild(2).GetChild(3).GetText() + "\")) ASC";
                        strColumnExpression = "ABS(DISTANCE(" + context.GetChild(0).GetText() + ", \"" + context.GetChild(2).GetChild(1).GetText() + "," + context.GetChild(2).GetChild(3).GetText() + "\"))";
                        strInnerColumnExpression = strColumnExpression.Replace(strFullColumnName, strTable + InnerTableSuffix + "." + strColumn);
                    }
                    else
                    {
                        strSQL = "ABS(" + context.GetChild(0).GetText() + " - " + context.GetChild(2).GetText() + ") ASC";
                        strColumnExpression = "ABS(" + context.GetChild(0).GetText() + " - " + context.GetChild(2).GetText() + ")";
                        //Ganzer Spaltenname ersetzen, ansonsten gibt es durcheinander (z.B. ALIAS für Tabelle ist c, und Attributname ist price)
                        strInnerColumnExpression = strColumnExpression.Replace(strFullColumnName, strTable + InnerTableSuffix + "." + strColumn);
                    }
                    strOperator = "<";

                    pref.Skyline.Add(new AttributeModel(strColumnExpression, strOperator, strInnerColumnExpression, "", "", true, "", "", "", "", strSQL, false, strColumn, "", 0));

                    break;

                case SQLParser.K_FAVOUR:
                    //Value should be as close as possible to a given string value
                    strSQL = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END ASC";
                    strColumnExpression = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END";
                    strInnerColumnExpression = strColumnExpression.Replace(strFullColumnName, strTable + InnerTableSuffix + "." + strColumn);
                    strOperator = "<";

                    pref.Skyline.Add(new AttributeModel(strColumnExpression, strOperator, strInnerColumnExpression, "", "", true, "", "", "", "", strSQL, false, strColumn, "", 0));

                    break;

                case SQLParser.K_DISFAVOUR:
                    //Value should be as far away as possible to a given string value
                    strSQL = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END DESC";
                    strColumnExpression = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END";
                    strInnerColumnExpression = strColumnExpression.Replace(strFullColumnName, strTable + InnerTableSuffix + "." + strColumn);
                    strOperator = ">";


                    pref.Skyline.Add(new AttributeModel(strColumnExpression, strOperator, strInnerColumnExpression, strFullColumnName, "", true, "", "", "", "", strSQL, false, strColumn, "", 0));

                    break;

            }



            //Add the preference to the list               
            pref.Tables = tables;
            pref.HasSkyline = true;
            pref.WithIncomparable = withIncomparable;
            model = pref;

            return pref;

        }



        /// <summary>
        /// Returns the column name from the parse tree object
        /// </summary>
        /// <param name="tree"></param>
        /// <returns></returns>
        private string getColumnName(IParseTree tree)
        {
            if (tree.ChildCount == 1)
            {
                //Syntax column only (column)
                return tree.GetText();
            }
            else
            {
                //Syntax Table with column (table.column)
                return tree.GetChild(2).GetText();
            }
        }

        /// <summary>
        /// Returns the table name from the parse tree object
        /// </summary>
        /// <param name="tree"></param>
        /// <returns></returns>
        private string getTableName(IParseTree tree)
        {
            if (tree.ChildCount == 1)
            {
                //Syntax column only (column)
                return "";
            }
            else
            {
                //Syntax Table with column (table.column)
                return tree.GetChild(0).GetText();
            }
        }

    }
}
