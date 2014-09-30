using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace prefSQL.SQLParser
{
    class SQLVisitor : SQLBaseVisitor<String>
    {
        public override string VisitPreferenceLOWHIGH(SQLParser.PreferenceLOWHIGHContext context)
        {
            String strSQL = "";

            if (context.ChildCount == 2)
            {
                //Abfrage auf Keyword LOW oder HIGH, danach ein ORDER BY daraus machen
                if (context.op.Type == SQLParser.K_LOW)
                {

                    strSQL = " ORDER BY " + context.expr(0).GetText() + " ASC";
                }
                else if (context.op.Type == SQLParser.K_HIGH)
                {
                    strSQL = " ORDER BY " + context.expr(0).GetText() + " DESC";

                }

                Console.WriteLine("Visit Preference : " + strSQL);
            }
            else
            {

                //Build CASE ORDER with arguments
                String strExpr = context.expr(1).GetText();
                String strColumn = context.expr(0).GetText();
                Char[] charDelimiter = new Char[] { '>', '>' };
                string[] strTemp = strExpr.Split(charDelimiter, StringSplitOptions.RemoveEmptyEntries);
                string strSQLOrderBy = "";
                string strSQLELSE = "";

                //Define sort order value for each attribute
                for (int i = 0; i < strTemp.GetLength(0); i++)
                {
                    //Special word others = all other attributes are defined with this order by value
                    if (strTemp[i].Equals("OTHERS") == true)
                    {
                        strSQLELSE = "\nELSE " + i;
                    }
                    else
                    {
                        strSQLOrderBy += "\nWHEN " + strColumn + " = " + strTemp[i] + " THEN " + i.ToString() + " ";
                    }
                }
                strSQL = " ORDER BY CASE " + strSQLOrderBy + strSQLELSE + " END ";


                //Depending on LOW or HIGH do an ASCENDING or DESCENDING sort
                if (context.op.Type == SQLParser.K_LOW)
                {

                    strSQL += " ASC";
                }
                else if (context.op.Type == SQLParser.K_HIGH)
                {
                    strSQL += " DESC";

                }
            }

            return strSQL;
        }

        public override string VisitPreferenceAROUND(SQLParser.PreferenceAROUNDContext context)
        {
            String strSQL = "";
            //Abfrage auf Keyword AROUND, FAVOUR und DISFAVOUR, danach ein ORDER BY daraus machen

            switch (context.op.Type)
            {
                case SQLParser.K_AROUND:
                    //Value should be as close as possible to a given numeric value
                    strSQL = " ORDER BY ABS(" + context.expr(0).GetText() + " - " + context.expr(1).GetText() + ") ASC";
                    break;
                    
                case SQLParser.K_FAVOUR:
                    //Value should be as close as possible to a given string value
                    strSQL = " ORDER BY CASE WHEN " + context.expr(0).GetText() + " = " +  context.expr(1).GetText() + " THEN 1 ELSE 2 END ASC";
                    break;

                case SQLParser.K_DISFAVOUR:
                    //Value should be as far away as possible to a given string value
                    strSQL = " ORDER BY CASE WHEN " + context.expr(0).GetText() + " = " + context.expr(1).GetText() + " THEN 1 ELSE 2 END DESC";
                    break;

            }
            

            Console.WriteLine("Visit Preference : " + strSQL);
            return strSQL;
        }
        
    }
}
