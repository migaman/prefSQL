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
        private Dictionary<string, string> tables = new Dictionary<string, string>();   //contains all tables from the query
        private int numberOfRecords = 0;                        //Specifies the number of records to return (TOP Clause), 0 = all records
        private const string InnerTableSuffix = "_INNER";       //Table suffix for the inner query
        private const string RankingFunction = "ROW_NUMBER()";  //Default Ranking function
        private PrefSQLModel model;                             //Preference SQL Model, contains i.e. the skyline attributes
        private bool isNative;                                  //True if the skyline algorithm is native                 
        private bool hasIncomparableTuples = false;             //True if the skyline must be checked for incomparable tuples
        private bool containsOpenPreference = false;            //True if the skyline contains a categorical preference without an explicit OTHERS statement


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
        /// Set a boolean variable if query contains TOP keyword
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitTop_keyword(SQLParser.Top_keywordContext context)
        {
            numberOfRecords = int.Parse(context.GetChild(1).GetText());
            return base.VisitTop_keyword(context);
        }


        /// <summary>
        /// Adds each used table name and its alias in the query to a list
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitTable_List_Item(SQLParser.Table_List_ItemContext context)
        {
            string strTable = context.GetChild(0).GetText();
            string strTableAlias = "";
            if (context.ChildCount == 2)
            {
                //ALIAS introduced without "AS"-Keyword
                strTableAlias = context.GetChild(1).GetText();
            }
            else if (context.ChildCount == 3) 
            {
                //ALIAS introduced with "AS"-Keyword
                strTableAlias = context.GetChild(2).GetText();
            }
            tables.Add(strTable, strTableAlias);

            return base.VisitTable_List_Item(context);
        }


        /// <summary>
        /// Combines multiple ranking preferences (each preference has its own weight)
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitWeightedsumAnd(SQLParser.WeightedsumAndContext context)
        {
            //And was used --> visit left and right node
            PrefSQLModel left = Visit(context.exprRanking(0));
            PrefSQLModel right = Visit(context.exprRanking(1));

            //Add the columns to the preference model
            PrefSQLModel pref = new PrefSQLModel();
            pref.Ranking.AddRange(left.Ranking);
            pref.Ranking.AddRange(right.Ranking);
            pref.Tables = tables;
            model = pref;
            return pref;
        }

        /// <summary>
        /// Base function to handle ranking preferences
        /// </summary>
        /// <param name="strColumnName"></param>
        /// <param name="strTable"></param>
        /// <param name="strExpression"></param>
        /// <param name="weight"></param>
        /// <returns></returns>
        private PrefSQLModel addWeightedSum(string strColumnName, string strTable, string strExpression, double weight)
        {
            PrefSQLModel pref = new PrefSQLModel();
            string strTableAlias = ""; ;
            string strSelectExtrema = "";
            string strFullColumnName = "";

            //Separate Column and Table
            strFullColumnName = strTable + "." + strColumnName;           

            //Search if table name is just an alias
            var myValue = tables.FirstOrDefault(x => x.Value == strTable).Key;
            if (myValue != null)
            {
                //Its an alias, replace it with the real table name
                strTableAlias = strTable;
                strTable = myValue;
            }

            //Select Statement to read the extrem values of the preference
            strSelectExtrema = "SELECT MIN(" + strExpression + "), MAX(" + strExpression + ") FROM " + strTable + " " + strTableAlias;
            

            //Add the preference to the list               
            pref.Ranking.Add(new RankingModel(strFullColumnName, strColumnName, strExpression, weight, strSelectExtrema));
            pref.Tables = tables;
            model = pref;
            return pref;
        }

        /// <summary>
        /// Handles LOW/HIGH ranking preferences
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitWeightedsumLowHigh(SQLParser.WeightedsumLowHighContext context)
        {
            //Keyword LOW or HIGH
            string strColumnName = "";
            string strTable = "";
            string strExpression = "";
            double weight = 0.0;
            
            //Separate Column and Table
            strColumnName = getColumnName(context.GetChild(0));
            strTable = getTableName(context.GetChild(0));
            strExpression = strTable + "." + strColumnName;
            if (context.op.Type == SQLParser.K_HIGH)
            {
                //Multiply with -1 (result: every value can be minimized!)
                strExpression += " * -1";
            }
            weight = double.Parse(context.GetChild(2).GetText());

            //Add the preference to the list               
            return addWeightedSum(strColumnName, strTable, strExpression, weight);
        }

        /// <summary>
        /// Handles around ranking preferences
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitWeightedsumAround(SQLParser.WeightedsumAroundContext context)
        {
            //Keyword AROUND, FAVOUR, DISFAVOUR
            string strColumnName = "";
            string strTable = "";
            string strExpression = "";
            double weight = 0.0;

            //Separate Column and Table
            strColumnName = getColumnName(context.GetChild(0));
            strTable = getTableName(context.GetChild(0));
            
            switch (context.op.Type)
            {
                case SQLParser.K_AROUND:

                    //Value should be as close as possible to a given numeric value
                    //Check if its a geocoordinate
                    strExpression = "ABS(" + context.GetChild(0).GetText() + " - " + context.GetChild(2).GetText() + ")";
                    break;
                case SQLParser.K_FAVOUR:
                    //Value should be as close as possible to a given string value
                    strExpression = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END";
                    break;

                case SQLParser.K_DISFAVOUR:
                    //Value should be as far away as possible to a given string value
                    strExpression = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END";
                    break;
            }
            weight = double.Parse(context.GetChild(3).GetText());

            //Add the preference to the list               
            return addWeightedSum(strColumnName, strTable, strExpression, weight);
        }



        /// <summary>
        /// Handles a categorical ranking preference
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitWeightedsumCategory(SQLParser.WeightedsumCategoryContext context)
        {
            //It is a text --> Text text must be converted in a given sortorder
            string strColumnName = "";
            string strTable = "";
            string strExpression = "";
            double weight = 0.0;

            //Build CASE ORDER with arguments
            string strCaseWhen = "";
            string strCaseElse = "";

            //Separate Column and Table
            strColumnName = getColumnName(context.GetChild(0));
            strTable = getTableName(context.GetChild(0));

            //Define sort order value for each attribute
            int iWeight = 0;
            for (int i = 0; i < context.exprCategory().ChildCount; i++)
            {
                switch (context.exprCategory().GetChild(i).GetText())
                {
                    case ">>":
                        iWeight += 1; //Gewicht erhöhen, da >> Operator
                        break;
                    case "==":
                        break;  //Gewicht bleibt gleich da == Operator
                    case "OTHERSEQUAL":
                        //Special word OTHERS EQUAL = all other attributes are defined with this order by value
                        strCaseElse = " ELSE " + iWeight;
                        break;
                    default:
                        //Check if it contains multiple values
                        if (context.exprCategory().GetChild(i).ChildCount > 1)
                        {
                            //Multiple values --> construct IN statement
                            string strValues = "(" + context.exprCategory().GetChild(i).GetChild(1).GetText() + ")";
                            strCaseWhen += " WHEN " + strTable + "." + strColumnName + " IN " + strValues + " THEN " + iWeight.ToString();

                        }
                        else
                        {
                            //Single value --> construct = statement
                            strCaseWhen += " WHEN " + strTable + "." + strColumnName + " = " + context.exprCategory().GetChild(i).GetText() + " THEN " + iWeight.ToString();
                        }
                        break;
                }

            }

            weight = double.Parse(context.GetChild(4).GetText());
            strExpression = "CASE" + strCaseWhen + strCaseElse + " END";
            

            //Add the ranking to the list               
            return addWeightedSum(strColumnName, strTable, strExpression, weight);
        }





        /// <summary>
        /// Combines multiple pareto preferences (each preference is equivalent)
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitSkylineAnd(SQLParser.SkylineAndContext context)
        {
            //And was used --> visit left and right node
            PrefSQLModel left = Visit(context.exprSkyline(0));
            PrefSQLModel right = Visit(context.exprSkyline(1));

            //Add the columns to the preference model
            PrefSQLModel pref = new PrefSQLModel();
            pref.Skyline.AddRange(left.Skyline);
            pref.Skyline.AddRange(right.Skyline);
            pref.Tables = tables;
            pref.NumberOfRecords = numberOfRecords;
            pref.WithIncomparable = hasIncomparableTuples;
            pref.ContainsOpenPreference = containsOpenPreference;
            model = pref;
            return pref;
        }



        /// <summary>
        /// Base function to handle skyline preferences
        /// </summary>
        /// <param name="strColumnName"></param>
        /// <param name="strTable"></param>
        /// <param name="strExpression"></param>
        /// <param name="weight"></param>
        /// <returns></returns>
        private PrefSQLModel addSkyline(AttributeModel attributeModel)
        {
            //Add the preference to the list               
            PrefSQLModel pref = new PrefSQLModel();
            pref.Skyline.Add(attributeModel);
            pref.NumberOfRecords = numberOfRecords;
            pref.Tables = tables;
            pref.WithIncomparable = hasIncomparableTuples;
            pref.ContainsOpenPreference = containsOpenPreference;
            model = pref;
            return pref;
        }


        /// <summary>
        /// Handles a numerical HIGH/LOW preference
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitSkylinePreferenceLowHigh(SQLParser.SkylinePreferenceLowHighContext context)
        {
            bool isLevelStepEqual = true;
            string strColumnName = "";
            string strColumnExpression = "";
            string strInnerColumnExpression = "";
            string strFullColumnName = "";
            string strTable = "";
            string strRankHexagon = "";
            string strLevelStep = "";
            string strLevelAdd = "";
            string strLevelMinus = "";
            string strExpression = "";
            bool bComparable = true;
            string strIncomporableAttribute = "";
            string strOpposite = "";
            
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
                    hasIncomparableTuples = true;
                    //Some algorithms cannot handle this incomparable preference --> It is like a categorical preference without explicit OTHERS
                    containsOpenPreference = true;
                }
                
            }
            //Keyword LOW or HIGH, build ORDER BY
            if (context.op.Type == SQLParser.K_LOW || context.op.Type == SQLParser.K_HIGH)
            {
                string strSortOrder = "ASC";
                string strLevelAdditionaly = strLevelAdd;
                if (context.op.Type == SQLParser.K_HIGH)
                {
                    strSortOrder = "DESC";
                    strLevelAdditionaly = strLevelMinus;
                    //Multiply with -1 (result: every value can be minimized!)
                    strOpposite = " * -1";
                }
                strRankHexagon = "DENSE_RANK()" + " OVER (ORDER BY " + strFullColumnName + strLevelStep + " " + strSortOrder + ")-1 AS Rank" + strFullColumnName.Replace(".", "");
                strColumnExpression = "DENSE_RANK() OVER (ORDER BY " + strFullColumnName + strLevelStep + strOpposite + ")";
                strExpression = strFullColumnName + strLevelStep + strOpposite;
                if (isLevelStepEqual == true)
                {
                    strInnerColumnExpression = strTable + InnerTableSuffix + "." + strColumnName + strLevelStep + strOpposite;
                }
                else
                {   
                    //Values from the same step are Incomparable
                    strInnerColumnExpression = "(" + strTable + InnerTableSuffix + "." + strColumnName + strLevelAdditionaly + ")" + strLevelStep + strOpposite;
                    strIncomporableAttribute = "'INCOMPARABLE'";
                }
                
            }
            


            //Add the preference to the list      
            return addSkyline(new AttributeModel(strColumnExpression, strInnerColumnExpression, strFullColumnName, "", bComparable, strIncomporableAttribute, strRankHexagon, false, "", 0, strExpression));
        }



        /// <summary>
        /// Handles a categorical preference
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitSkylinePreferenceCategory(SQLParser.SkylinePreferenceCategoryContext context)
        {
            //It is a text --> Text text must be converted in a given sortordez
            string strColumnName = "";
            string strTable = "";
            string strRankHexagon = "";
            string strHexagonIncomparable = "";
            int amountOfIncomparable = 0;
            string strFullColumnName = "";
            //Build CASE ORDER with arguments
            string strExpr = context.exprCategory().GetText();
            string strColumnExpression = "";

            //Separate Column and Table
            strColumnName = getColumnName(context.GetChild(0));
            strTable = getTableName(context.GetChild(0));
            strFullColumnName = strTable +  "." + strColumnName;


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
                        hasIncomparableTuples = true;
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
                                string strBitPattern = new String('0', strTemp[i].Split(',').GetUpperBound(0) + 1);
                                strBitPattern = strBitPattern.Substring(0, amountOfIncomparable) + "1" + strBitPattern.Substring(amountOfIncomparable + 1);
                                strHexagonIncomparable += " WHEN " + strTable + "." + strColumnName + " = " + strCategory.Replace("(", "").Replace(")", "") + " THEN '" + strBitPattern + "'";
                                amountOfIncomparable++;
                            }
                            string strBitPatternFull = new String('x', amountOfIncomparable); // string of 20 spaces;
                            strHexagonIncomparable += " ELSE '" + strBitPatternFull + "' END AS HexagonIncomparable" + strSingleColumn.Replace(".", "");
                            hasIncomparableTuples = true; //the values inside the bracket are incomparable
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

            if (strSQLELSE.Equals(""))
            {
                //There is a categorical preference without an OTHER statement!! (Not all algorithms can handle that)
                containsOpenPreference = true;
            }

            //Add others incomparable clause at the top-level if not OTHERS was specified
            if (strSQLELSE.Equals("") && IsNative == false)
            {
                strIncomporableAttributeELSE = " ELSE " + strTable + "." + strColumnName; //Not comparable --> give string value of field
                strSQLELSE = " ELSE NULL"; //if no OTHERS is present all other values are on the top level
                bComparable = false;
                hasIncomparableTuples = true;
            }

            string strExpression = "CASE" + strSQLOrderBy + strSQLELSE + " END";
            strInnerColumn = "CASE" + strSQLInnerOrderBy + strSQLInnerELSE + " END";
            strIncomporableAttribute = "CASE" + strSQLIncomparableAttribute + strIncomporableAttributeELSE + " END";
            strColumnExpression = "DENSE_RANK() OVER (ORDER BY " + strExpression + ")";


            strRankHexagon = "DENSE_RANK()" + " OVER (ORDER BY " + strExpression + ")-1 AS Rank" + strFullColumnName.Replace(".", "");


            //Add the preference to the list               
            return addSkyline(new AttributeModel(strColumnExpression, strInnerColumn, strSingleColumn, strInnerSingleColumn, bComparable, strIncomporableAttribute, strRankHexagon, true, strHexagonIncomparable, amountOfIncomparable, weightHexagonIncomparable, strExpression));
        }

        /// <summary>
        /// Handles around preferences
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitSkylinePreferenceAround(SQLParser.SkylinePreferenceAroundContext context)
        {
            string strColumnName = "";
            string strFullColumnName = "";
            string strColumnExpression = "";
            string strTable = "";
            string strInnerColumnExpression = "";
            string strRankHexagon = "";
            string strExpression = "";

            //Separate Column and Table
            strColumnName = getColumnName(context.GetChild(0));
            strTable = getTableName(context.GetChild(0));
            strFullColumnName = strTable + "." + strColumnName;

            switch (context.op.Type)
            {
                case SQLParser.K_AROUND:
                    
                    //Value should be as close as possible to a given numeric value
                    strColumnExpression = "DENSE_RANK() OVER (ORDER BY ABS(" + context.GetChild(0).GetText() + " - " + context.GetChild(2).GetText() + "))";
                    strInnerColumnExpression = "ABS(" + getTableName(context.GetChild(0)) + InnerTableSuffix + "." + getColumnName(context.GetChild(0))  + " - " + context.GetChild(2).GetText() + ")";
                    strExpression = "ABS(" + context.GetChild(0).GetText() + " - " + context.GetChild(2).GetText() + ")";
                    break;

                case SQLParser.K_FAVOUR:
                    //Value should be as close as possible to a given string value
                    strColumnExpression = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END";
                    strInnerColumnExpression = "CASE WHEN " + getTableName(context.GetChild(0)) + InnerTableSuffix + "." + getColumnName(context.GetChild(0)) + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END";
                    strExpression = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END";
                    break;

                case SQLParser.K_DISFAVOUR:
                    //Value should be as far away as possible to a given string value
                    strColumnExpression = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END";
                    strInnerColumnExpression = "CASE WHEN " + getTableName(context.GetChild(0)) + InnerTableSuffix + "." + getColumnName(context.GetChild(0)) + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END * -1";
                    strExpression = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END * -1";
                    break;
            }


            strRankHexagon = "DENSE_RANK()" + " OVER (ORDER BY " + strExpression + ")-1 AS Rank" + strFullColumnName.Replace(".", "");
            

            //Add the preference to the list     
            return addSkyline(new AttributeModel(strColumnExpression, strInnerColumnExpression, strFullColumnName, "", true, "", strRankHexagon, false, "", 0, strExpression));
        }





        /// <summary>
        /// Handle a prefence with the IS MORE IMPORTANT AS keyword
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitSkylineMoreImportant(SQLParser.SkylineMoreImportantContext context)
        {
            //Preference with Priorization --> visit left and right node
            PrefSQLModel left = Visit(context.GetChild(0));
            PrefSQLModel right = Visit(context.GetChild(5));

            //
            string strSortOrder = left.Skyline[0].Expression + "," + right.Skyline[0].Expression;
            string strColumnALIAS = left.Skyline[0].FullColumnName.Replace(".", "") + right.Skyline[0].FullColumnName.Replace(".", "");
            string strSkyline = "DENSE_RANK()" + " OVER (ORDER BY " + strSortOrder + ")";

            //left.Skyline[0].ColumnExpression = strSkyline;
            //left.Skyline[0].ColumnName = strColumnALIAS;
            left.Skyline[0].Comparable = left.Skyline[0].Comparable && right.Skyline[0].Comparable;

            //Add the columns to the preference model
            PrefSQLModel pref = new PrefSQLModel();
            pref.Skyline.AddRange(left.Skyline);
            pref.Tables = tables;
            pref.NumberOfRecords = numberOfRecords;
            pref.WithIncomparable = hasIncomparableTuples;
            pref.ContainsOpenPreference = containsOpenPreference;
            model = pref;
            return pref;
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
            orderByModel.stop = context.stop.StopIndex + 1;
            orderByModel.text = strSQL;
            model.OrderBy.Add(orderByModel);
            return base.VisitOrderbyCategory(context);
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
