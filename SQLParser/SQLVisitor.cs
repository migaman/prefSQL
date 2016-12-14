using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Antlr4.Runtime.Tree;
using prefSQL.SQLParser.Models;
using prefSQL.Grammar;
using System.Globalization;
using prefSQL.SQLParser.Udf;

namespace prefSQL.SQLParser
{
    //internal class
    class SQLVisitor : PrefSQLBaseVisitor<PrefSQLModel>
    {
        private Dictionary<string, string> _tables = new Dictionary<string, string>();   //contains all tables from the query
        private int _numberOfRecords;                        //Specifies the number of records to return (TOP Clause), 0 = all records
        private const string InnerTableSuffix = "_INNER";       //Table suffix for the inner query
        private bool _hasIncomparableTuples;             //True if the skyline must be checked for incomparable tuples
        private bool _containsOpenPreference;            //True if the skyline contains a categorical preference without an explicit OTHERS statement


        public bool IsNative { get; set; }


        public PrefSQLModel Model { get; set; }


        /// <summary>
        /// Set a boolean variable if query contains TOP keyword
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitTop_keyword(PrefSQLParser.Top_keywordContext context)
        {
            _numberOfRecords = int.Parse(context.GetChild(1).GetText());
            return base.VisitTop_keyword(context);
        }


        /// <summary>
        /// Adds each used table name and its alias in the query to a list
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitTable_List_Item(PrefSQLParser.Table_List_ItemContext context)
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
            _tables.Add(strTable, strTableAlias);

            return base.VisitTable_List_Item(context);
        }


        /// <summary>
        /// Combines multiple ranking preferences (each preference has its own weight)
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitWeightedsumAnd(PrefSQLParser.WeightedsumAndContext context)
        {
            //And was used --> visit left and right node
            PrefSQLModel left = Visit(context.exprRanking(0));
            PrefSQLModel right = Visit(context.exprRanking(1));

            //Add the columns to the preference model
            PrefSQLModel pref = new PrefSQLModel();
            pref.Ranking.AddRange(left.Ranking);
            pref.Ranking.AddRange(right.Ranking);
            pref.Tables = _tables;
            pref.ContainsOpenPreference = _containsOpenPreference;
            Model = pref;
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
        private PrefSQLModel AddWeightedSum(string strColumnName, string strTable, string strExpression, double weight)
        {
            PrefSQLModel pref = new PrefSQLModel();
            string strTableAlias = "";

            //Separate Column and Table
            string strFullColumnName = strTable + "." + strColumnName;

            //Search if table name is just an alias
            string myValue = _tables.FirstOrDefault(x => x.Value == strTable).Key;
            if (myValue != null)
            {
                //Its an alias, replace it with the real table name
                strTableAlias = strTable;
                strTable = myValue;
            }

            //Select Statement to read the extrem values of the preference
            string strSelectExtrema = "SELECT MIN(" + strExpression + "), MAX(" + strExpression + ") FROM " + strTable + " " + strTableAlias;


            //Add the preference to the list               
            pref.Ranking.Add(new RankingModel(strFullColumnName, strColumnName, strExpression, weight, strSelectExtrema));
            pref.Tables = _tables;
            pref.ContainsOpenPreference = _containsOpenPreference;
            Model = pref;
            return pref;
        }

        /// <summary>
        /// Handles LOW/HIGH ranking preferences
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitWeightedsumLowHigh(PrefSQLParser.WeightedsumLowHighContext context)
        {
            //Keyword LOW or HIGH

            //Separate Column and Table
            string strColumnName = GetColumnName(context.GetChild(0));
            string strTable = GetTableName(context.GetChild(0));
            string strExpression = strTable + "." + strColumnName;
            if (context.op.Type == PrefSQLParser.K_HIGH)
            {
                //Multiply with -1 (result: every value can be minimized!)
                strExpression += " * -1";
            }
            // Set the decimal seperator, because prefSQL double values are always with decimal separator "."
            NumberFormatInfo format = new NumberFormatInfo();
            format.NumberDecimalSeparator = ".";
            double weight = double.Parse(context.GetChild(2).GetText(), format);


            //Add the preference to the list               
            return AddWeightedSum(strColumnName, strTable, strExpression, weight);
        }

        /// <summary>
        /// Handles around ranking preferences
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitWeightedsumAround(PrefSQLParser.WeightedsumAroundContext context)
        {
            //Keyword AROUND, FAVOUR, DISFAVOUR
            string strExpression = "";

            //Separate Column and Table
            string strColumnName = GetColumnName(context.GetChild(0));
            string strTable = GetTableName(context.GetChild(0));

            switch (context.op.Type)
            {
                case PrefSQLParser.K_AROUND:

                    //Value should be as close as possible to a given numeric value
                    //Check if its a geocoordinate
                    strExpression = "ABS(" + context.GetChild(0).GetText() + " - " + context.GetChild(2).GetText() + ")";
                    break;
                case PrefSQLParser.K_FAVOUR:
                    //Value should be as close as possible to a given string value
                    strExpression = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END";
                    break;

                case PrefSQLParser.K_DISFAVOUR:
                    //Value should be as far away as possible to a given string value
                    strExpression = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END";
                    break;
            }
            // Set the decimal seperator, because prefSQL double values are always with decimal separator "."
            NumberFormatInfo format = new NumberFormatInfo();
            format.NumberDecimalSeparator = ".";
            double weight = double.Parse(context.GetChild(3).GetText(), format);

            //Add the preference to the list               
            return AddWeightedSum(strColumnName, strTable, strExpression, weight);
        }



        /// <summary>
        /// Handles a categorical ranking preference
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitWeightedsumCategory(PrefSQLParser.WeightedsumCategoryContext context)
        {
            //It is a text --> Text text must be converted in a given sortorder

            //Build CASE ORDER with arguments
            string strCaseWhen = "";
            string strCaseElse = "";

            //Separate Column and Table
            string strColumnName = GetColumnName(context.GetChild(0));
            string strTable = GetTableName(context.GetChild(0));

            //Define sort order value for each attribute
            int iWeight = 0;
            for (int i = 0; i < context.exprCategoryNoIncomparable().ChildCount; i++)
            {
                switch (context.exprCategoryNoIncomparable().GetChild(i).GetText())
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
                        if (context.exprCategoryNoIncomparable().GetChild(i).ChildCount > 1)
                        {
                            //Multiple values --> construct IN statement
                            for (int ii = 0; ii < context.exprCategoryNoIncomparable().GetChild(i).ChildCount; ii++)
                            {
                                switch (context.exprCategoryNoIncomparable().GetChild(i).GetChild(ii).GetText())
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
                                        string strValues = "(" + context.exprCategoryNoIncomparable().GetChild(i).GetChild(ii).GetText() + ")";
                                        strCaseWhen += " WHEN " + strTable + "." + strColumnName + " IN " + strValues + " THEN " + iWeight.ToString();
                                        break;
                                }
                            }

                        }
                        else
                        {
                            //Single value --> construct = statement
                            strCaseWhen += " WHEN " + strTable + "." + strColumnName + " = " + context.exprCategoryNoIncomparable().GetChild(i).GetText() + " THEN " + iWeight.ToString();
                        }
                        break;
                }

            }

            // Set the decimal seperator, because prefSQL double values are always with decimal separator "."
            NumberFormatInfo format = new NumberFormatInfo();
            format.NumberDecimalSeparator = ".";
            double weight = double.Parse(context.GetChild(context.ChildCount-1).GetText(), format);

            if(strCaseElse.Equals(""))
            {
                _containsOpenPreference = true;
            }
            string strExpression = "CASE" + strCaseWhen + strCaseElse + " END";


            //Add the ranking to the list               
            return AddWeightedSum(strColumnName, strTable, strExpression, weight);
        }





        /// <summary>
        /// Combines multiple pareto preferences (each preference is equivalent)
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitSkylineAnd(PrefSQLParser.SkylineAndContext context)
        {
            //And was used --> visit left and right node
            PrefSQLModel left = Visit(context.exprSkyline(0));
            PrefSQLModel right = Visit(context.exprSkyline(1));

            //Add the columns to the preference model
            PrefSQLModel pref = new PrefSQLModel();
            pref.Skyline.AddRange(left.Skyline);
            pref.Skyline.AddRange(right.Skyline);
            pref.Tables = _tables;
            pref.NumberOfRecords = _numberOfRecords;
            pref.WithIncomparable = _hasIncomparableTuples;
            pref.ContainsOpenPreference = _containsOpenPreference;
            Model = pref;
            return pref;
        }


        /// <summary>
        /// Base function to handle skyline preferences
        /// </summary>
        /// <returns></returns>
        private PrefSQLModel AddSkyline(AttributeModel attributeModel)
        {
            //Add the preference to the list               
            PrefSQLModel pref = new PrefSQLModel();
            pref.Skyline.Add(attributeModel);
            pref.NumberOfRecords = _numberOfRecords;
            pref.Tables = _tables;
            pref.WithIncomparable = _hasIncomparableTuples;
            pref.ContainsOpenPreference = _containsOpenPreference;
            Model = pref;
            return pref;
        }


        /// <summary>
        /// Handles a numerical HIGH/LOW preference
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitSkylinePreferenceLowHigh(PrefSQLParser.SkylinePreferenceLowHighContext context)
        {
            bool isLevelStepEqual = true;
            string strColumnExpression = "";
            string strInnerColumnExpression = "";
            string strLevelStep = "";
            string strLevelAdd = "";
            string strLevelMinus = "";
            string strExpression = "";
            bool bComparable = true;
            string strIncomporableAttribute = "";
            string strOpposite = "";

            //Separate Column and Table
            string strColumnName = GetColumnName(context.GetChild(0));
            string strTable = GetTableName(context.GetChild(0));
            string strFullColumnName = strTable + "." + strColumnName;

            if (context.ChildCount == 4)
            {
                //If a third parameter is given, it is the Level Step  (i.e. LOW price 1000 means prices divided through 1000)
                //The user doesn't care about a price difference of 1000
                //This results in a smaller skyline
                strLevelStep = " / " + context.GetChild(2).GetText();
                strLevelAdd = " + " + context.GetChild(2).GetText();
                strLevelMinus = " - " + context.GetChild(2).GetText();
                if (!context.GetChild(3).GetText().Equals("EQUAL"))
                {
                    isLevelStepEqual = false;
                    bComparable = false;
                    _hasIncomparableTuples = true;
                    //Some algorithms cannot handle this incomparable preference --> It is like a categorical preference without explicit OTHERS
                    _containsOpenPreference = true;
                }

            }
            //Keyword LOW or HIGH, build ORDER BY
            if (context.op.Type == PrefSQLParser.K_LOW || context.op.Type == PrefSQLParser.K_HIGH)
            {
                string strLevelAdditionaly = strLevelAdd;
                if (context.op.Type == PrefSQLParser.K_HIGH)
                {
                    strLevelAdditionaly = strLevelMinus;
                    //Multiply with -1 (result: every value can be minimized!)
                    strOpposite = " * -1";
                }
                //Don't use Functions like DENSE_RANK() for the preferences --> slows down SQL performance!
                strColumnExpression = "CAST(" + strFullColumnName + strLevelStep + strOpposite + " AS bigint)";
                strExpression = strFullColumnName + strLevelStep + strOpposite;
                if (isLevelStepEqual)
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
            return AddSkyline(new AttributeModel(strColumnExpression, strInnerColumnExpression, strFullColumnName, "", bComparable, strIncomporableAttribute, false, "", 0, 0, strExpression));
        }

        public override PrefSQLModel VisitSkylinePreferenceUdf(PrefSQLParser.SkylinePreferenceUdfContext context)
        {
            // extract data
            var udfModel = new SqlUdfVisitor().VisitSkylinePreferenceUdf(context);

            // set global flags
            _hasIncomparableTuples = _hasIncomparableTuples || udfModel.HasIncomparableTuples;
            _containsOpenPreference = _containsOpenPreference || udfModel.ContainsOpenPreference;

            // create preference and add to list      
            var sub = new SqlUdfBuilder(udfModel, InnerTableSuffix);
            return AddSkyline(new AttributeModel(sub.CreateRankingExpr(), sub.CreateInnerExpr(), udfModel.FullFunctionName, "", udfModel.IsComparable, sub.CreateIncomporableAttribute(), false, "", 0, 0, sub.CreateExpression()));

        }


        /// <summary>
        /// Handles a categorical preference
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitSkylinePreferenceCategory(PrefSQLParser.SkylinePreferenceCategoryContext context)
        {
            //It is a text --> Text text must be converted in a given sortordez
            string strHexagonIncomparable = "";
            int amountOfIncomparable = 0;
            //Build CASE ORDER with arguments
            string strExpr = context.exprCategory().GetText();

            //Separate Column and Table
            string strColumnName = GetColumnName(context.GetChild(0));
            string strTable = GetTableName(context.GetChild(0));


            string[] strTemp = Regex.Split(strExpr, @"(==|>>)"); //Split signs are == and >>
            string strSQLOrderBy = "";
            string strSqlelse = "";
            string strSQLInnerElse = "";
            string strSQLInnerOrderBy = "";
            string strSingleColumn = strTable + "." + GetColumnName(context.GetChild(0));
            string strInnerSingleColumn = strTable + InnerTableSuffix + "." + GetColumnName(context.GetChild(0));
            string strSQLIncomparableAttribute = "";
            string strIncomporableAttributeElse = "";
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
                        strSqlelse = " ELSE " + (iWeight);
                        strSQLInnerElse = " ELSE " + (iWeight + 1);
                        strIncomporableAttributeElse = " ELSE " + strTable + "." + strColumnName; //Not comparable --> give string value of field
                        bComparable = false;
                        amountOfIncomparable = 99; //set a certain amount
                        strHexagonIncomparable = "CALCULATEINCOMPARABLE";
                        weightHexagonIncomparable = iWeight / 100;
                        _hasIncomparableTuples = true;
                        break;
                    case "OTHERSEQUAL":
                        //Special word OTHERS EQUAL = all other attributes are defined with this order by value
                        strSqlelse = " ELSE " + iWeight;
                        strSQLInnerElse = " ELSE " + iWeight;
                        strIncomporableAttributeElse = " ELSE ''"; //Comparable give empty column
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
                            _hasIncomparableTuples = true; //the values inside the bracket are incomparable
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

            if (strSqlelse.Equals(""))
            {
                //There is a categorical preference without an OTHER statement!! (Not all algorithms can handle that)
                _containsOpenPreference = true;
            }

            //Add others incomparable clause at the top-level if not OTHERS was specified
            if (strSqlelse.Equals("") && IsNative == false)
            {
                strIncomporableAttributeElse = " ELSE " + strTable + "." + strColumnName; //Not comparable --> give string value of field
                strSqlelse = " ELSE NULL"; //if no OTHERS is present all other values are on the top level
                bComparable = false;
                _hasIncomparableTuples = true;
            }

            string strExpression = "CASE" + strSQLOrderBy + strSqlelse + " END";
            string strInnerColumn = "CASE" + strSQLInnerOrderBy + strSQLInnerElse + " END";
            string strIncomporableAttribute = "CASE" + strSQLIncomparableAttribute + strIncomporableAttributeElse + " END";
            string strColumnExpression = "CAST(" + strExpression + " AS bigint)";




            //Add the preference to the list               
            return AddSkyline(new AttributeModel(strColumnExpression, strInnerColumn, strSingleColumn, strInnerSingleColumn, bComparable, strIncomporableAttribute, true, strHexagonIncomparable, amountOfIncomparable, weightHexagonIncomparable, strExpression));
        }

        /// <summary>
        /// Handles around preferences
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitSkylinePreferenceAround(PrefSQLParser.SkylinePreferenceAroundContext context)
        {
            string strColumnExpression = "";
            string strInnerColumnExpression = "";
            string strExpression = "";

            //Separate Column and Table
            string strColumnName = GetColumnName(context.GetChild(0));
            string strTable = GetTableName(context.GetChild(0));
            string strFullColumnName = strTable + "." + strColumnName;

            switch (context.op.Type)
            {
                case PrefSQLParser.K_AROUND:

                    //Value should be as close as possible to a given numeric value
                    strColumnExpression = "CAST(ABS(" + context.GetChild(0).GetText() + " - " + context.GetChild(2).GetText() + ") AS bigint)";
                    strInnerColumnExpression = "ABS(" + GetTableName(context.GetChild(0)) + InnerTableSuffix + "." + GetColumnName(context.GetChild(0)) + " - " + context.GetChild(2).GetText() + ")";
                    strExpression = "ABS(" + context.GetChild(0).GetText() + " - " + context.GetChild(2).GetText() + ")";
                    break;

                case PrefSQLParser.K_FAVOUR:
                    //Value should be as close as possible to a given string value
                    //Cast Column Expression, because coparison is built on long values
                    strColumnExpression = "CAST(CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END AS BIGINT)";
                    strInnerColumnExpression = "CASE WHEN " + GetTableName(context.GetChild(0)) + InnerTableSuffix + "." + GetColumnName(context.GetChild(0)) + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END";
                    strExpression = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END";
                    break;
                case PrefSQLParser.K_DISFAVOUR:
                    //Value should be as far away as possible to a given string value
                    //Add missing negative multiplication and Cast Column Expression, because coparison is built on long values. 
                    strColumnExpression = "CAST(CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END * -1 AS BIGINT)";
                    strInnerColumnExpression = "CASE WHEN " + GetTableName(context.GetChild(0)) + InnerTableSuffix + "." + GetColumnName(context.GetChild(0)) + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END * -1";
                    strExpression = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END * -1";
                    break;
            }


            //Add the preference to the list     
            return AddSkyline(new AttributeModel(strColumnExpression, strInnerColumnExpression, strFullColumnName, "", true, "", false, "", 0, 0, strExpression));
        }





        /// <summary>
        /// Handle a prefence with the IS MORE IMPORTANT AS keyword
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitSkylineMoreImportant(PrefSQLParser.SkylineMoreImportantContext context)
        {
            //Preference with Priorization --> visit left and right node
            PrefSQLModel left = Visit(context.GetChild(0));
            PrefSQLModel right = Visit(context.GetChild(5));

            //Combine left and right model (left is more important than right)
            string strSortOrder = left.Skyline[0].Expression + "," + right.Skyline[0].Expression;
            string strSkyline = "DENSE_RANK()" + " OVER (ORDER BY " + strSortOrder + ")";
            //Combine to new column name
            string strColumnALIAS = left.Skyline[0].FullColumnName.Replace(".", "_") + right.Skyline[0].FullColumnName.Replace(".", "_");

            left.Skyline[0].Expression = strSkyline;
            left.Skyline[0].RankExpression = strSkyline;
            left.Skyline[0].FullColumnName = strColumnALIAS;
            left.Skyline[0].Comparable = left.Skyline[0].Comparable && right.Skyline[0].Comparable;

            //Add the columns to the preference model
            PrefSQLModel pref = new PrefSQLModel();
            pref.Skyline.AddRange(left.Skyline);
            pref.Tables = _tables;
            pref.NumberOfRecords = _numberOfRecords;
            pref.WithIncomparable = _hasIncomparableTuples;
            pref.ContainsOpenPreference = _containsOpenPreference;
            Model = pref;
            return pref;
        }


        /// <summary>
        /// Normal ORDER BY clause
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitOrderByDefault(PrefSQLParser.OrderByDefaultContext context)
        {
            Model.Ordering = SQLCommon.Ordering.AsIs;
            return base.VisitOrderByDefault(context);
        }

        /// <summary>
        /// Order BY clause with prefSQL keywords (SUMRANK or BESTRANK)
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitOrderBySpecial(PrefSQLParser.OrderBySpecialContext context)
        {
            SQLCommon.Ordering ordering = SQLCommon.Ordering.AsIs;
            if (context.op.Type == PrefSQLParser.K_SUMRANK)
            {
                ordering = SQLCommon.Ordering.RankingSummarize;
            }
            else if (context.op.Type == PrefSQLParser.K_BESTRANK)
            {
                ordering = SQLCommon.Ordering.RankingBestOf;
            }
            Model.Ordering = ordering;

            return base.VisitOrderBySpecial(context);
        }




        /// <summary>
        /// ORDER BY clause with syntactic sugar of prefSQL
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override PrefSQLModel VisitOrderbyCategory(PrefSQLParser.OrderbyCategoryContext context)
        {
            string strSQL = "";

            //Build CASE ORDER with arguments
            string strExpr = context.exprCategory().GetText();
            string strColumnName = GetColumnName(context.GetChild(0));
            string strTable = GetTableName(context.GetChild(0));
            string[] strTemp = Regex.Split(strExpr, @"(==|>>)"); //Split signs are == and >>
            string strSQLOrderBy = "";
            string strSqlelse = "";



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
                        strSqlelse = " ELSE " + (iWeight);
                        break;
                    case "OTHERSEQUAL":
                        //Special word OTHERS EQUAL = all other attributes are defined with this order by value
                        strSqlelse = " ELSE " + iWeight;
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
                strSQL = "CASE" + strSQLOrderBy + strSqlelse + " END ASC";
            }

            OrderByModel orderByModel = new OrderByModel();
            orderByModel.Start = context.start.StartIndex;
            orderByModel.Stop = context.stop.StopIndex + 1;
            orderByModel.Text = strSQL;
            Model.OrderBy.Add(orderByModel);
            return base.VisitOrderbyCategory(context);
        }


        /// <summary>
        /// Returns the column name from the parse tree object
        /// </summary>
        /// <param name="tree"></param>
        /// <returns></returns>
        private string GetColumnName(IParseTree tree)
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
        private string GetTableName(IParseTree tree)
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

        public override PrefSQLModel VisitExprSampleSkyline(PrefSQLParser.ExprSampleSkylineContext context)
        {
            Model.SkylineSampleCount = int.Parse(context.GetChild(4).GetText());
            Model.SkylineSampleDimension = int.Parse(context.GetChild(6).GetText());
            Model.HasSkylineSample = true;
            return base.VisitExprSampleSkyline(context);
        }


    }
}
