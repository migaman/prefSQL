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
                pref.Skyline.Add(new AttributeModel(strTable + "." + strColumn, strOperator, strTable, strTable + "_" + "INNER", strTable + "_INNER." + strColumn, "", "", false));
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
                string strSQLInnerOrderBy = "";
                string strInnerColumn = "";
                string strSingleColumn = strTable + "." + getColumn(context.GetChild(1));
                string strInnerSingleColumn = strTable + "_INNER." + getColumn(context.GetChild(1));
                Boolean includeOthers = false;

                //Define sort order value for each attribute
                int iWeight = 0;
                for (int i = 0; i < strTemp.GetLength(0); i++)
                {
                    switch (strTemp[i])
                    {
                        case ">>":
                            iWeight++; //Gewicht erhöhen, da >> Operator
                            break;
                        case "==":
                            break;  //Gewicht bleibt gleich da == Operator
                        case "OTHERS":
                            //Special word others = all other attributes are defined with this order by value
                            strSQLELSE = " ELSE " + iWeight;
                            includeOthers = true;
                            break;
                        default:
                            strSQLOrderBy += " WHEN " + strTable + "." + strColumn + " = " + strTemp[i] + " THEN " + iWeight.ToString();
                            strSQLInnerOrderBy += " WHEN " + strTable + "_INNER." + strColumn + " = " + strTemp[i] + " THEN " + iWeight.ToString();
                            break;
                    }

                }
                strSQL = "CASE" + strSQLOrderBy + strSQLELSE + " END";
                strInnerColumn = "CASE" + strSQLInnerOrderBy + strSQLELSE + " END";
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
                pref.Skyline.Add(new AttributeModel(strColumn, strOperator, strTable, strTable + "_" + "INNER", strInnerColumn, strSingleColumn, strInnerSingleColumn, includeOthers));
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
