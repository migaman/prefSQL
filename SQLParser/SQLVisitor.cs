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
        public override PrefSQLModel VisitPreferenceLOWHIGH(SQLParser.PreferenceLOWHIGHContext context)
        {
            String strSQL = "";
            PrefSQLModel pref = new PrefSQLModel();
            String strColumn = "";
            String strOperator = "";

            //With only 2 expressions it is a numeric LOW preference 
            if (context.ChildCount == 2)
            {
                //Abfrage auf Keyword LOW oder HIGH, danach ein ORDER BY daraus machen
                strColumn = context.GetChild(1).GetText();
                
                
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
                pref.Skyline.Add(new AttributeModel(strColumn, strOperator));

            }
            //Otherwise it is a text LOW preference --> Text text must be converted in a given sortorder
            else
            {

                //Build CASE ORDER with arguments
                String strExpr = context.expr().GetText(); //context.expr(1).GetText();
                strColumn = context.GetChild(1).GetText(); // +context.GetChild(2).GetText() + context.GetChild(3).GetText(); //context.expr(0).GetText();
                string[] strTemp = Regex.Split(strExpr, @"(==|>>)"); //Split Zechen sind == und >>
                string strSQLOrderBy = "";
                string strSQLELSE = "";

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
                            break;
                        default:
                            strSQLOrderBy += " WHEN " + strColumn + " = " + strTemp[i] + " THEN " + iWeight.ToString();
                            break;
                    }

                }
                strSQL = "CASE" + strSQLOrderBy + strSQLELSE + " END";


                //Depending on LOW or HIGH do an ASCENDING or DESCENDING sort
                if (context.op.Type == SQLParser.K_LOW)
                {

                    strSQL += " ASC";
                }
                else if (context.op.Type == SQLParser.K_HIGH)
                {
                    strSQL += " DESC";

                }
                //Add the preference to the list               
                pref.Skyline.Add(new AttributeModel(strColumn, "UNDEFINED"));
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
                    if (context.expr(1).GetType().ToString() == "prefSQL.SQLParser.SQLParser+GeocoordinateContext")
                    {
                        strSQL = "ABS(DISTANCE(" + context.expr(0).GetText() + ", \"" + context.expr(1).GetChild(1).GetText() + "," + context.expr(1).GetChild(3).GetText() + "\")) ASC";
                    }
                    else
                    {
                        strSQL = "ABS(" + context.expr(0).GetText() + " - " + context.expr(1).GetText() + ") ASC";
                    }
                    break;

                case SQLParser.K_FAVOUR:
                    //Value should be as close as possible to a given string value
                    strSQL = "CASE WHEN " + context.expr(0).GetText() + " = " + context.expr(1).GetText() + " THEN 1 ELSE 2 END ASC";
                    break;

                case SQLParser.K_DISFAVOUR:
                    //Value should be as far away as possible to a given string value
                    strSQL = "CASE WHEN " + context.expr(0).GetText() + " = " + context.expr(1).GetText() + " THEN 1 ELSE 2 END DESC";
                    break;

            }

            PrefSQLModel pref = new PrefSQLModel();
            pref.OrderBy.Add(strSQL);
            return pref;

            //return strSQL + base.VisitPreferenceAROUND(context);


        }

    }
}
