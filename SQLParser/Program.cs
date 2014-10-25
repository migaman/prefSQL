using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Text;
using System.IO;
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
            GeneratePerformanceQueries();
            //(new Program()).Run();
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



                //String strPrefSQL = "SELECT cars.id, cars.title, colors.name, fuels.name FROM cars " +
                //String strPrefSQL = "SELECT cars.id, cars.title, cars.price, colors.name, mileage FROM cars " +
                String strPrefSQL = "SELECT cars.id, cars.title FROM cars " +
                //String strPrefSQL = "SELECT cars.id, cars.title, cars.price, cars.mileage, cars.horsepower, cars.enginesize, cars.registration, cars.consumption, cars.doors, colors.name, fuels.name FROM cars " +
                //String strPrefSQL = "SELECT cars.id, cars.title, colors.name AS colourname, fuels.name AS fuelname, cars.price FROM cars " +
                    //String strPrefSQL = "SELECT id FROM cars " +
                    "LEFT OUTER JOIN colors ON cars.color_id = colors.ID " +
                    "LEFT OUTER JOIN fuels ON cars.fuel_id = fuels.ID " +
                    //"WHERE cars.horsepower > 10 AND cars.price < 10000 " +
                    //"PREFERENCE LOW colors.name {'rot' == 'blau' >> OTHERS >> 'grau'} AND LOW cars.price";
                    //"PREFERENCE LOW cars.title {'MERCEDES-BENZ SL 600' >> OTHERS} AND LOW cars.price";
                    //"PREFERENCE LOW colors.name {'rot' >> OTHERS} AND LOW cars.price";
                    //"PREFERENCE Low cars.price AND Low cars.mileage";
                    //"PREFERENCE LOW cars.price AND LOW cars.mileage AND HIGH cars.horsepower AND HIGH cars.enginesize AND HIGH cars.registration AND LOW cars.consumption AND HIGH cars.doors AND LOW colors.name {'rot' == 'blau' >> OTHERS >> 'grau'} AND LOW fuels.name {'Benzin' >> OTHERS >> 'Diesel'}";
                    "PREFERENCE LOW cars.price AND LOW cars.mileage AND LOW fuels.name {'Benzin' >> OTHERS >> 'Diesel'}";
                    //"PREFERENCE LOW colors.name {'gelb' >> OTHERS >> 'grau'} AND LOW fuels.name {'Benzin' >> OTHERS >> 'Diesel'} AND LOW cars.price ";
                    //"PREFERENCE colors.name DISFAVOUR 'rot' ";
                    //"PREFERENCE cars.location AROUND (47.0484, 8.32629) ";
                Console.WriteLine(strPrefSQL);
                Console.WriteLine("--------------------------------------------");


                SQLCommon parser = new SQLCommon();
                parser.SkylineType = SQLCommon.Algorithm.NativeSQL;
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



        private static void GeneratePerformanceQueries()
        {
            //Add more columns
            String[] columns = { "cars.price", "cars.mileage", "cars.horsepower", "cars.enginesize", "cars.registration", "cars.consumption", "cars.doors", "colors.name", "fuels.name", "bodies.name", "cars.title", "makes.name", "conditions.name" };
            String[] preferences = { "LOW cars.price", "LOW cars.mileage", "HIGH cars.horsepower", "HIGH cars.enginesize", "HIGH cars.registration", "LOW cars.consumption", "HIGH cars.doors", "LOW colors.name {'rot' == 'blau' >> OTHERS >> 'grau'}", "LOW fuels.name {'Benzin' >> OTHERS >> 'Diesel'}", "LOW bodies.name {'Kleinwagen' >> 'Bus' >> 'Kombi' >> 'Roller' >> 'OTHERS' >> 'Pick-Up'}", "LOW cars.title {'MERCEDES-BENZ SL 600' >> OTHERS}", "LOW makes.name {'ASTON MARTIN' >> 'VW' == 'Audi' >> OTHERS >> 'FERRARI'}", "LOW conditions.name {'Neu' >> OTHERS}" };
            String[] sizes = { "small", "medium", "large", "superlarge" };

            
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("set statistics time on");
            

            for (int i = columns.GetUpperBound(0); i >= 1; i--)
            {
                //SELECT FROM
                String strSQL = "SELECT " + string.Join(",", columns) + " FROM cars ";
                int countJoins = 0;

                //Add Joins
                if (strSQL.IndexOf("colors") > 0)
                {
                    strSQL += "LEFT OUTER JOIN colors ON cars.color_id = colors.ID ";
                    countJoins++;
                }
                if (strSQL.IndexOf("fuels") > 0)
                {
                    strSQL += "LEFT OUTER JOIN fuels ON cars.fuel_id = fuels.ID ";
                    countJoins++;
                }
                if (strSQL.IndexOf("bodies") > 0)
                {
                    strSQL += "LEFT OUTER JOIN bodies ON cars.body_id = bodies.ID ";
                    countJoins++;
                }
                if (strSQL.IndexOf("makes") > 0)
                {
                    strSQL += "LEFT OUTER JOIN makes ON cars.make_id = makes.ID ";
                    countJoins++;
                }
                if (strSQL.IndexOf("conditions") > 0)
                {
                    strSQL += "LEFT OUTER JOIN conditions ON cars.condition_id = conditions.ID ";
                    countJoins++;
                }


                //ADD Preferences
                strSQL += "PREFERENCE " + string.Join(" AND ", preferences);


                //Convert to real SQL
                SQLCommon parser = new SQLCommon();
                parser.SkylineType = SQLCommon.Algorithm.NativeSQL;
                strSQL = parser.parsePreferenceSQL(strSQL);


                //Format for each of the customer profiles
                sb.AppendLine("PRINT '----- -------------------------------------------------------- ------'");
                sb.AppendLine("PRINT '----- " + (i + 1) + " dimensions, " + (countJoins) + " join(s) ------'");
                sb.AppendLine("PRINT '----- -------------------------------------------------------- ------'");
                foreach (String size in sizes)
                {
                    sb.AppendLine(strSQL.Replace("cars", "cars_" + size));
                     
                }


                //strPerformanceQuery += "\n\n\n";
                sb.AppendLine("");
                sb.AppendLine("");
                sb.AppendLine("");
                //Console.WriteLine();

                


                //Remove current column
                columns = columns.Where(w => w != columns[i]).ToArray();
                preferences = preferences.Where(w => w != preferences[i]).ToArray();
            }
            //
            sb.AppendLine("set statistics time off");

            //Write in file
            StreamWriter outfile = new StreamWriter("E:\\Doc\\Studies\\PRJ_Thesis\\10 Arcmedia Profiles\\Performance_Auto.sql");
            outfile.Write(sb.ToString());
            outfile.Close();
            Console.WriteLine("THE END!!");

        }




      


    }
}
