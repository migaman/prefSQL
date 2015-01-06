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
        private bool includesTOP = false;
        private const string InnerTableSuffix = "_INNER"; //Table suffix for the inner query
        private const string RankingFunction = "ROW_NUMBER()";
        private PrefSQLModel model;
        

        public PrefSQLModel Model
        {
            get { return model; }
            set { model = value; }
        }

        
        public override PrefSQLModel VisitOrderByDefault(SQLParser.OrderByDefaultContext context)
        {
            model.Ordering = SQLCommon.Ordering.AsIs;
            return base.VisitOrderByDefault(context);
        }

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


        public override PrefSQLModel VisitOrderbyCategory(SQLParser.OrderbyCategoryContext context)
        {
            string strSQL = "";
            string strColumnName = "";
            string strTable = "";

            //Build CASE ORDER with arguments
            string strExpr = context.exprSkyline().GetText();
            strColumnName = getColumn(context.GetChild(0));
            strTable = getTable(context.GetChild(0));
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
                        //bComparable = false;
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



        public override PrefSQLModel VisitTable_or_subquery(SQLParser.Table_or_subqueryContext context)
        {
            string strTable = context.GetChild(0).GetText();
            string strTableAlias = "";
            if (context.ChildCount == 2)
            {
                strTableAlias = context.GetChild(1).GetText();
            }
            tables.Add(strTable, strTableAlias);

            return base.VisitTable_or_subquery(context);
        }

        public override PrefSQLModel VisitTop_keyword(SQLParser.Top_keywordContext context)
        {
            includesTOP = true;
            return base.VisitTop_keyword(context);
        }

        

        public override PrefSQLModel VisitPreferenceCategory(SQLParser.PreferenceCategoryContext context)
        {
            string strSQL = "";
            PrefSQLModel pref = new PrefSQLModel();
            string strColumnName = "";
            string strTable = "";
            string strOperator = "";
            string strRankColumn = "";
            string strRankHexagon = "";

            //It is a text --> Text text must be converted in a given sortorder

            //Build CASE ORDER with arguments
            string strExpr = context.exprSkyline().GetText();
            strColumnName = getColumn(context.GetChild(0));
            strTable = getTable(context.GetChild(0));
            string[] strTemp = Regex.Split(strExpr, @"(==|>>)"); //Split signs are == and >>
            string strSQLOrderBy = "";
            string strSQLELSE = "";
            string strSQLInnerELSE = "";
            string strSQLInnerOrderBy = "";
            string strInnerColumn = "";
            string strSingleColumn = strTable + "." + getColumn(context.GetChild(0));
            string strInnerSingleColumn = strTable + InnerTableSuffix + "." + getColumn(context.GetChild(0));
            string strSQLIncomparableAttribute = "";
            string strIncomporableAttribute = "";
            string strIncomporableAttributeELSE = "";
            bool bComparable = true;

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
                        strIncomporableAttributeELSE = "ELSE " + strTable + "." + strColumnName; //Not comparable --> give string value of field
                        bComparable = false;
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
            strSQL = "CASE" + strSQLOrderBy + strSQLELSE + " END";
            strInnerColumn = "CASE" + strSQLInnerOrderBy + strSQLInnerELSE + " END";
            strIncomporableAttribute = "CASE" + strSQLIncomparableAttribute + strIncomporableAttributeELSE + " END";
            strColumnName = strSQL;

            //Categories are always sorted ASCENDING
            strSQL += " ASC";
            strOperator = "<";

            strRankColumn = RankingFunction + " over (ORDER BY " + strSQL + ")";
            strRankHexagon = "DENSE_RANK()" + " over (ORDER BY " + strSQL + ")-1 AS Rank" + strSingleColumn.Replace(".", "");
            //Add the preference to the list               
            pref.Skyline.Add(new AttributeModel(strColumnName, strOperator, strInnerColumn, strSingleColumn, strInnerSingleColumn, bComparable, strIncomporableAttribute, strSingleColumn.Replace(".", ""), strRankColumn, strRankHexagon, strSQL));



            //pref.OrderBySkyline.Add(strSQL);
            pref.Tables = tables;
            pref.HasSkyline = true;
            model = pref;
            return pref;
        }

        public override PrefSQLModel VisitPreferenceLOWHIGH(SQLParser.PreferenceLOWHIGHContext context)
        {
            string strSQL = "";
            PrefSQLModel pref = new PrefSQLModel();
            string strColumnName = "";
            string strColumnExpression = "";
            string strInnerColumnExpression = "";
            string strFullColumnName = "";
            string strTable = "";
            string strOperator = "";
            string strRankColumn = "";
            string strRankHexagon = "";
            string strLevelStep = "";
            
            //Separate Column and Table
            strColumnName = getColumn(context.GetChild(0));
            strTable = getTable(context.GetChild(0));
            strFullColumnName = strTable + "." + strColumnName;
            
            if (context.ChildCount == 3)
            {
                //If a third parameter is given, it is the Level Step  (i.e. LOW price 1000 means prices divided through 1000)
                //The user doesn't care about a price difference of 1000
                //This results in a smaller skyline
                strLevelStep = " / " + context.GetChild(2).GetText();
            }
            //Keyword LOW or HIGH, build ORDER BY
            if (context.op.Type == SQLParser.K_LOW)
            {
                strSQL = strColumnName + strLevelStep + " ASC";
                strOperator = "<";
                strRankHexagon = "DENSE_RANK()" + " over (ORDER BY " + strFullColumnName + strLevelStep + " ASC)-1 AS Rank" + strColumnName;
                strRankColumn = RankingFunction + " over (ORDER BY " + strFullColumnName + " ASC)";


                strColumnExpression = strTable + "." + strColumnName + strLevelStep;
                strInnerColumnExpression = strTable + InnerTableSuffix + "." + strColumnName + strLevelStep;
            }
            else if (context.op.Type == SQLParser.K_HIGH)
            {
                strSQL = strColumnName + strLevelStep + " DESC";
                strOperator = ">";
                strRankHexagon = "DENSE_RANK()" + " over (ORDER BY " + strFullColumnName + strLevelStep + " DESC)-1 AS Rank" + strColumnName;
                strRankColumn = RankingFunction + " over (ORDER BY " + strFullColumnName + " DESC)";
                strColumnExpression = strTable + "." + strColumnName + strLevelStep; ;
                strInnerColumnExpression = strTable + InnerTableSuffix + "." + strColumnName + strLevelStep; ;
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
            pref.Skyline.Add(new AttributeModel(strColumnExpression, strOperator, strInnerColumnExpression, "", "", true, "", strColumnName, strRankColumn, strRankHexagon, strSQL));
            pref.HasSkyline = true;
            pref.Tables = tables;
            model = pref;
            return pref;


        }


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
            model = pref;
            return pref;


            //return base.VisitExprAnd(context);
        }

        

        public override PrefSQLModel VisitPreferenceAROUND(SQLParser.PreferenceAROUNDContext context)
        {
            string strSQL = "";
            PrefSQLModel pref = new PrefSQLModel();
            string strColumn = "";
            string strTable = "";
            string strOperator = "";
            string strInnerColumnExpression = "";

            //Query Keywords AROUND, FAVOUR and DISFAVOUR, after that create an ORDER BY of it

            strColumn = getColumn(context.GetChild(0));
            strTable = getTable(context.GetChild(0));

            switch (context.op.Type)
            {
                case SQLParser.K_AROUND:
                    
                    //Value should be as close as possible to a given numeric value
                    //Check if its a geocoordinate
                    if (context.GetChild(2).GetType().ToString() == "prefSQL.SQLParser.SQLParser+GeocoordinateContext")
                    {
                        strSQL = "ABS(DISTANCE(" + context.GetChild(0).GetText() + ", \"" + context .GetChild(2).GetChild(1).GetText() + "," + context.GetChild(2).GetChild(3).GetText() + "\")) ASC";
                        strColumn = "ABS(DISTANCE(" + context.GetChild(0).GetText() + ", \"" + context.GetChild(2).GetChild(1).GetText() + "," + context.GetChild(2).GetChild(3).GetText() + "\"))";
                        strInnerColumnExpression = strColumn.Replace(strTable, strTable + InnerTableSuffix);
                    }
                    else
                    {
                        strSQL = "ABS(" + context.GetChild(0).GetText() + " - " + context.GetChild(2).GetText() + ") ASC";
                        strColumn = "ABS(" + context.GetChild(0).GetText() + " - " + context.GetChild(2).GetText() + ")";
                        strInnerColumnExpression = strColumn.Replace(strTable, strTable + InnerTableSuffix);
                    }
                    strOperator = "<";

                    pref.Skyline.Add(new AttributeModel(strColumn, strOperator, strInnerColumnExpression, "", "", true, "", "", "", "", strSQL));

                    break;

                case SQLParser.K_FAVOUR:
                    //Value should be as close as possible to a given string value
                    strSQL = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END ASC";
                    strColumn = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END";
                    strInnerColumnExpression = strColumn.Replace(strTable, strTable + InnerTableSuffix);
                    strOperator = "<";

                    pref.Skyline.Add(new AttributeModel(strColumn, strOperator, strInnerColumnExpression, "", "", true, "", "", "", "", strSQL));

                    break;

                case SQLParser.K_DISFAVOUR:
                    //Value should be as far away as possible to a given string value
                    strSQL = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END DESC";
                    strColumn = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END";
                    strInnerColumnExpression = strColumn.Replace(strTable, strTable + InnerTableSuffix);
                    strOperator = ">";


                    pref.Skyline.Add(new AttributeModel(strColumn, strOperator, strInnerColumnExpression, "", "", true, "", "", "", "", strSQL));

                    break;

            }



            //Add the preference to the list               
            pref.Tables = tables;
            pref.HasSkyline = true;
            model = pref;

            return pref;

        }




        private string getColumn(IParseTree tree)
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


        private string getTable(IParseTree tree)
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
