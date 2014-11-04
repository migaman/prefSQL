using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using prefSQL.SQLParser;

namespace Utility
{
    class Demo
    {
        public void generateDemoQueries()
        {
            String strSQL = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars " +
            "LEFT OUTER JOIN colors ON cars.color_id = colors.ID " +
            "LEFT OUTER JOIN bodies ON cars.body_id = bodies.ID ";

            String strPreference = "PREFERENCE LOW cars.price AND LOW colors.name {'schwarz' >> 'grün' == 'blau' >> 'pink' == 'rosa'} AND LOW bodies.name {'cabriolet' >> OTHERS}";



            SQLCommon parser = new SQLCommon();
            parser.SkylineType = SQLCommon.Algorithm.NativeSQL;
            String strNewSQL = parser.parsePreferenceSQL(strSQL + " " + strPreference);

            Console.WriteLine(strNewSQL);
            Console.WriteLine("------------------------------------------\nDONE");
        }


        
    }
}
