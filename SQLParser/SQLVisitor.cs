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
    


    class SQLVisitor : SQLBaseVisitor<PrefSQLModel>
    {

        public const string strTableSuffix = "_INNER"; //Table suffix for the inner query

        public override PrefSQLModel VisitTable_or_subquery(SQLParser.Table_or_subqueryContext context)
        {
            return base.VisitTable_or_subquery(context);
        }

        public override PrefSQLModel VisitTable_name(SQLParser.Table_nameContext context)
        {
            return base.VisitTable_name(context);
        }


        public override PrefSQLModel VisitPreferenceLOWHIGH(SQLParser.PreferenceLOWHIGHContext context)
        {
            String strSQL = "";
            PrefSQLModel pref = new PrefSQLModel();
            String strColumn = "";
            String strTable = "";
            String strOperator = "";

            //With only 2 expressions it is a numeric LOW preference 
            if (context.ChildCount == 2)
            {
                //Separate Column and Table
                strColumn = getColumn(context.GetChild(1));
                strTable = getTable(context.GetChild(1));

                //Keyword LOW or HIGH, build ORDER BY
                if (context.op.Type == SQLParser.K_LOW)
                {
                    strSQL = strColumn + " ASC";
                    strOperator = "<";
                }
                else if (context.op.Type == SQLParser.K_HIGH)
                {
                    strSQL = strColumn + " DESC";
                    strOperator = ">";

                }


                //Add the preference to the list               
                pref.Skyline.Add(new AttributeModel(strTable + "." + strColumn, strOperator, strTable, strTable + strTableSuffix, strTable + strTableSuffix + "." + strColumn, "", "", false, strTable + strTableSuffix + "." + strColumn));
                pref.Tables.Add(strTable);

            }
            //Otherwise it is a text LOW/HIGH preference --> Text text must be converted in a given sortorder
            else
            {

                //Build CASE ORDER with arguments
                String strExpr = context.expr().GetText();
                strColumn = getColumn(context.GetChild(1));
                strTable = getTable(context.GetChild(1));
                string[] strTemp = Regex.Split(strExpr, @"(==|>>)"); //Split signs are == and >>
                string strSQLOrderBy = "";
                string strSQLELSE = "";
                string strSQLELSEAccumulation = "";
                string strSQLInnerOrderBy = "";
                //string strSQLInnerAccumulationOrderBy = "";
                string strInnerColumn = "";
                string strInnerColumnAccumulation = "";
                string strSingleColumn = strTable + "." + getColumn(context.GetChild(1));
                string strInnerSingleColumn = strTable + strTableSuffix + "." + getColumn(context.GetChild(1));
                Boolean includeOthers = false;

                //Define sort order value for each attribute
                int iWeight = 0;
                for (int i = 0; i < strTemp.GetLength(0); i++)
                {
                    switch (strTemp[i])
                    {
                        case ">>":
                            iWeight+=100; //Gewicht erhöhen, da >> Operator
                            break;
                        case "==":
                            break;  //Gewicht bleibt gleich da == Operator
                        case "OTHERS":
                            //Special word others = all other attributes are defined with this order by value
                            strSQLELSE = " ELSE " + iWeight;
                            //Speziell ist beim OTHERS, dass die Bedingung dann nicht zutreffen darf weil sonst z.B. grün mit rot verglichen wird!!
                            strSQLELSEAccumulation = " ELSE " + (iWeight + 1); //Add one, so that equal-clause cannot be true with same level-values, but other names
                            includeOthers = true;
                            break;
                        default:
                            //Check if it contains multiple values
                            if (strTemp[i].StartsWith("{"))
                            {
                                //Multiple values --> construct IN statement
                                strTemp[i] = strTemp[i].Replace("{", "(").Replace("}", ")");
                                strSQLOrderBy += " WHEN " + strTable + "." + strColumn + " IN " + strTemp[i] + " THEN " + iWeight.ToString();
                                //This values are always incomparable (otherwise the = should be used)
                                strSQLInnerOrderBy += " WHEN " + strTable + strTableSuffix + "." + strColumn + " IN " + strTemp[i] + " THEN " + (iWeight + 1);
                                //strSQLInnerAccumulationOrderBy += " WHEN " + strTable + strTableSuffix + "." + strColumn + " IN " + strTemp[i] + " THEN " + (iWeight + 1);
                            }
                            else
                            {
                                //Single value --> construct = statement
                                strSQLOrderBy += " WHEN " + strTable + "." + strColumn + " = " + strTemp[i] + " THEN " + iWeight.ToString();
                                //This values are always comparable (otherwise the {x, y} should be used)
                                strSQLInnerOrderBy += " WHEN " + strTable + strTableSuffix + "." + strColumn + " = " + strTemp[i] + " THEN " + iWeight.ToString();
                                //strSQLInnerAccumulationOrderBy += " WHEN " + strTable + strTableSuffix + "." + strColumn + " = " + strTemp[i] + " THEN " + (iWeight+1);
                            }
                            break;
                    }

                }
                strSQL = "CASE" + strSQLOrderBy + strSQLELSE + " END";
                strInnerColumn = "CASE" + strSQLInnerOrderBy + strSQLELSE + " END";
                strInnerColumnAccumulation = "CASE" + strSQLInnerOrderBy + strSQLELSEAccumulation + " END";
                //strInnerColumnAccumulation = "CASE" + strSQLInnerAccumulationOrderBy + strSQLELSEAccumulation + " END";
                strColumn = strSQL;

                //Depending on LOW or HIGH do an ASCENDING or DESCENDING sort
                if (context.op.Type == SQLParser.K_LOW)
                {

                    strSQL += " ASC";
                    strOperator = "<";
                }
                else if (context.op.Type == SQLParser.K_HIGH)
                {
                    strSQL += " DESC";
                    strOperator = ">";

                }
                //Add the preference to the list               
                pref.Skyline.Add(new AttributeModel(strColumn, strOperator, strTable, strTable + "_" + "INNER", strInnerColumn, strSingleColumn, strInnerSingleColumn, includeOthers, strInnerColumnAccumulation));
                pref.Tables.Add(strTable);
            }


            
            pref.OrderBy.Add(strSQL);
            return pref;

        }


        public override PrefSQLModel VisitExprand(SQLParser.ExprandContext context)
        {
            //And was used --> visit left and right node
            PrefSQLModel left = Visit(context.expr(0));
            PrefSQLModel right = Visit(context.expr(1));
            
            //Add the columns to the preference model
            PrefSQLModel pref = new PrefSQLModel();
            pref.Skyline.AddRange(left.Skyline);
            pref.Skyline.AddRange(right.Skyline);
            pref.OrderBy.AddRange(left.OrderBy);
            pref.OrderBy.AddRange(right.OrderBy);
            pref.Tables.UnionWith(left.Tables);
            pref.Tables.UnionWith(right.Tables);
            return pref;

        }

        public override PrefSQLModel VisitPreferenceAROUND(SQLParser.PreferenceAROUNDContext context)
        {
            String strSQL = "";
            //Abfrage auf Keyword AROUND, FAVOUR und DISFAVOUR, danach ein ORDER BY daraus machen

            switch (context.op.Type)
            {
                case SQLParser.K_AROUND:
                    //Value should be as close as possible to a given numeric value
                    //Check if its a geocoordinate
                    if (context.GetChild(2).GetType().ToString() == "prefSQL.SQLParser.SQLParser+GeocoordinateContext")
                    {
                        strSQL = "ABS(DISTANCE(" + context.GetChild(0).GetText() + ", \"" + context .GetChild(2).GetChild(1).GetText() + "," + context.GetChild(2).GetChild(3).GetText() + "\")) ASC";
                    }
                    else
                    {
                        strSQL = "ABS(" + context.GetChild(0).GetText() + " - " + context.GetChild(2).GetText() + ") ASC";
                    }
                    break;

                case SQLParser.K_FAVOUR:
                    //Value should be as close as possible to a given string value
                    strSQL = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END ASC";
                    break;

                case SQLParser.K_DISFAVOUR:
                    //Value should be as far away as possible to a given string value
                    strSQL = "CASE WHEN " + context.GetChild(0).GetText() + " = " + context.GetChild(2).GetText() + " THEN 1 ELSE 2 END DESC";
                    break;

            }

            PrefSQLModel pref = new PrefSQLModel();
            pref.OrderBy.Add(strSQL);
            return pref;

            //return strSQL + base.VisitPreferenceAROUND(context);


        }




        private String getColumn(IParseTree tree)
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


        private String getTable(IParseTree tree)
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
