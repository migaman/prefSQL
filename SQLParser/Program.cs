using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;

using Antlr4.Runtime;
using Antlr4.Runtime.Tree;



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
                //Ablauf aktuell
                /*
                    1.Pareto Front
                    2.Filter auf die Kritik (z.B. Preis < 50000')
                    3.Pareto Front aufgrund der Präferenzen
                    4.Similarity berechnen
                */



                String strPrefSQL = "SELECT cars.id, cars.title, cars.price, colors.name AS colour FROM cars " +
                //String strPrefSQL = "SELECT * FROM cars " +
                //String strPrefSQL = "SELECT id FROM cars " +
                    "LEFT OUTER JOIN colors ON cars.color_id = colors.ID " +
                    //"WHERE horsepower > 10 AND price < 10000 " +
                    "PREFERENCE LOW colors.name {'rot' == 'blau' >> OTHERS >> 'grau'} ";
                    //"PREFERENCE LOW price AND LOW mileage AND LOW horsepower";
                    //"PREFERENCE colors.name DISFAVOUR 'rot' ";
                    //"PREFERENCE Location AROUND (47.0484, 8.32629) ";
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
