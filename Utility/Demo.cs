using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using prefSQL.SQLParser;
using System.Diagnostics;

namespace Utility
{
    class Demo
    {
        public void generateDemoQueries()
        {
            String strSQL = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars " +
            "LEFT OUTER JOIN colors ON cars.color_id = colors.ID " +
            "LEFT OUTER JOIN bodies ON cars.body_id = bodies.ID ";

            String strPreference = "PREFERENCE LOW cars.price AND HIGH colors.name {'schwarz' >> 'grün' == 'blau' >> 'pink' == 'rosa'} AND HIGH bodies.name {'cabriolet' >> OTHERS}";



            SQLCommon parser = new SQLCommon();
            parser.SkylineType = SQLCommon.Algorithm.NativeSQL;
            String strNewSQL = parser.parsePreferenceSQL(strSQL + " " + strPreference);

            Debug.WriteLine(strNewSQL);
            Debug.WriteLine("------------------------------------------\nDONE");
        }


        
    }
}
