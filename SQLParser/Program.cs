using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;



namespace prefSQL.SQLParser
{
    public class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        private static void Main(string[] args) 
        {
            (new Program()).Run();
        }

        public void Run()
        {
            try
            {


                String strPrefSQL = "SELECT cars.id, cars.title, cars.price, colors.name AS colour FROM cars " +
                    "LEFT OUTER JOIN colors ON cars.color_id = colors.ID " +
                    //"WHERE horsepower > 10 AND price < 10000 " +
                    //"PREFERENCE LOW colors.name {'rot' >> 'blau' >> OTHERS >> 'grau'} ";
                    "PREFERENCE LOW price ";
                    //"PREFERENCE colors.name DISFAVOUR 'rot' ";
                Console.WriteLine(strPrefSQL);
                Console.WriteLine("--------------------------------------------");


                SQLCommon parser = new SQLCommon();
                String strSQL = parser.parsePreferenceSQL(strPrefSQL);

                Console.WriteLine(strSQL);

                Console.WriteLine("------------------------------------------\nDONE");
                

            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: " + ex);
                Console.Write("Hit RETURN to exit: ");
            }

            Environment.Exit(0);
        }




        





      


    }
}
