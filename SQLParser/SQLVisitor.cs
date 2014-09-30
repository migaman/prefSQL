using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace prefSQL.SQLParser
{
    class SQLVisitor : SQLBaseVisitor<String>
    {

        public override string VisitPreferenceAROUND(SQLParser.PreferenceAROUNDContext context)
        {
            String strSQL = "";
            //Abfrage auf Keyword LOW oder HIGH, danach ein ORDER BY daraus machen
          
            strSQL = " ORDER BY ABS(" + context.expr(0).GetText() + " - " + context.expr(1).GetText()  + ") ASC";

            Console.WriteLine("Visit Preference : " + strSQL);
            return strSQL;
        }

        public override string VisitPreferenceLOW(SQLParser.PreferenceLOWContext context)
        {
      
            String strSQL = "";
            //Abfrage auf Keyword LOW oder HIGH, danach ein ORDER BY daraus machen
            if (context.op.Type == SQLParser.K_LOW)
            {
                
                strSQL = " ORDER BY " + context.expr().GetText() + " ASC";
            }
            else if (context.op.Type == SQLParser.K_HIGH)
            {
                strSQL = " ORDER BY " + context.expr().GetText() + " DESC";
                
            }

            Console.WriteLine("Visit Preference : " + strSQL);
            return strSQL;

        }


        
    }
}
