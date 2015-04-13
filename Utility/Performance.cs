using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using prefSQL.SQLParser;
using System.IO;
using System.Diagnostics;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using prefSQL.SQLSkyline;
using System.Collections;

namespace Utility
{
    class Performance
    {

        private const string path = "E:\\Doc\\Studies\\PRJ_Thesis\\19 Performance Level\\";
        private const string cnnStringLocalhost = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
        private const string driver = "System.Data.SqlClient";
        private int trials = 5;
        private PreferenceSet set;
        private bool generateScript = false;
        private SkylineStrategy strategy;
        static Random rnd = new Random();
        private int minDimensions = 3;
        private int randomPreferences = 6;

        public SkylineStrategy Strategy
        {
            get { return strategy; }
            set { strategy = value; }
        }

        public bool GenerateScript
        {
            get { return generateScript; }
            set { generateScript = value; }
        }

        internal PreferenceSet Set
        {
            get { return set; }
            set { set = value; }
        }

        public int Trials
        {
            get { return trials; }
            set { trials = value; }
        }

        public enum PreferenceSet
        {
            Jon,
            Mya,
            Barra,
            Shuffle,
            Combination
        };


        private ArrayList calculateCorrelation()
        {
            ArrayList preferences = new ArrayList();
            
            preferences.Add("cars.price LOW");
            preferences.Add("cars.mileage LOW");
            preferences.Add("cars.horsepower HIGH");
            preferences.Add("cars.enginesize HIGH");
            preferences.Add("cars.consumption LOW");
            preferences.Add("cars.doors HIGH");
            preferences.Add("cars.seats HIGH");
            preferences.Add("cars.cylinders HIGH");
            preferences.Add("cars.gears HIGH");
            //preferencesAll.Add("DATEDIFF(DAY, '1900-01-01', cars.Registration) HIGH");

            double[] correlation = new double[preferences.Count];

            string strSQL = "SELECT ";

            for (int i = 0; i < preferences.Count; i++)
            {
                strSQL += preferences[i] + ",";
            }
            strSQL = strSQL.TrimEnd(',') + " FROM cars";


            SqlConnection conn = new SqlConnection(cnnStringLocalhost);
            conn.Open();
            SqlCommand cmd = new SqlCommand(strSQL, conn);

            DataTable dt = new DataTable();
            dt.Load(cmd.ExecuteReader());
            double[] colA = new double[dt.Rows.Count];
            double[] colB = new double[dt.Rows.Count];


            

            //Calculate correlation between the attributes
            for (int iPref = 0; iPref < preferences.Count; iPref++)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    colA[i] = (int)dt.Rows[i][0];
                    colB[i] = (int)dt.Rows[i][iPref];
                }    


                correlation[iPref] = getPearson(colA, colB);
            }


            return preferences;
        }

        private double getPearson(double[] x,  double[] y)
        {
            //will regularize the unusual case of complete correlation
            const double TINY = 1.0e-20;
            int j, n = x.Length;
            Double yt, xt;
            Double syy = 0.0, sxy = 0.0, sxx = 0.0, ay = 0.0, ax = 0.0;
            for (j = 0; j < n; j++)
            {
                //finds the mean
                ax += x[j];
                ay += y[j];
            }
            ax /= n;
            ay /= n;
            for (j = 0; j < n; j++)
            {
                // compute correlation coefficient
                xt = x[j] - ax;
                yt = y[j] - ay;
                sxx += xt * xt;
                syy += yt * yt;
                sxy += xt * yt;
            }
            return sxy / (Math.Sqrt(sxx * syy) + TINY);
            
        }


       




        public void generatePerformanceQueries()
        {
            //Use the correct line, depending on how incomparable items should be compared
            ArrayList listPreferences = new ArrayList();

            if (set == PreferenceSet.Jon)
            {
                ArrayList preferences = new ArrayList();
                preferences.Add("cars.price LOW");
                preferences.Add("cars.mileage LOW");
                preferences.Add("cars.horsepower HIGH");
                preferences.Add("cars.enginesize HIGH");
                preferences.Add("cars.consumption LOW");
                preferences.Add("cars.doors HIGH");
                preferences.Add("cars.seats HIGH");
                preferences.Add("cars.cylinders HIGH");
                preferences.Add("cars.gears HIGH");
                listPreferences.Add(preferences);

            }
            else if (set == PreferenceSet.Mya)
            {
                ArrayList preferences = new ArrayList();
                preferences.Add("fuels.name ('Benzin' >> OTHERS EQUAL)");
                preferences.Add("makes.name ('FISKER' >> OTHERS EQUAL)");
                preferences.Add("bodies.name ('Roller' >> OTHERS EQUAL)");
                preferences.Add("models.name ('123' >> OTHERS EQUAL)");
                listPreferences.Add(preferences);
            }
            else if (set == PreferenceSet.Barra)
            {
                ArrayList preferences = new ArrayList();
                preferences.Add("cars.price LOW 3000");
                preferences.Add("cars.mileage LOW 20000");
                preferences.Add("cars.horsepower HIGH 20");
                preferences.Add("cars.enginesize HIGH 1000");
                preferences.Add("cars.consumption LOW 10");
                preferences.Add("cars.doors HIGH");
                preferences.Add("cars.seats HIGH 2");
                preferences.Add("cars.cylinders HIGH");
                preferences.Add("cars.gears HIGH");
                listPreferences.Add(preferences);
            }
            else if (set == PreferenceSet.Combination)
            {
                ArrayList preferences = calculateCorrelation();
                getCombinations(preferences, randomPreferences, 0, new ArrayList(), ref listPreferences);
                

                //set mindimensions to maxdimension 
                minDimensions = randomPreferences;

            }
            else if (set == PreferenceSet.Shuffle)
            {
                ArrayList preferencesAll = calculateCorrelation();
                ArrayList preferences = new ArrayList();
                
                ArrayList combinations = new ArrayList();
                getCombinations(preferencesAll, 6, 0, new ArrayList(), ref combinations);


                //Choose 6 preferences randomly
                for (int i = 0; i < randomPreferences; i++)
                {
                    int r = rnd.Next(preferencesAll.Count);
                    preferences.Add(preferencesAll[r]);
                    preferencesAll.RemoveAt(r);
                }

                //set mindimensions to maxdimension 
                minDimensions = randomPreferences;

                listPreferences.Add(preferences);
                
            }
            
            //Header
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("               Algorithm:" + strategy.ToString());
            sb.AppendLine("          Preference Set:" + set.ToString());
            sb.AppendLine("                    Host:" + System.Environment.MachineName);
            sb.AppendLine("      Set of Preferences:" + listPreferences.Count);
            sb.AppendLine("                  Trials:" + Trials);
            //sb.AppendLine("Correlation Coefficients:" + string.Join(",", (string[])preferences.ToArray(Type.GetType("System.String"))));
            sb.AppendLine("");
            sb.AppendLine("preference set|      trial|dimensions|skyline size|time total|time algorithm");
            string strSeparatorLine = ("").PadLeft(14, '-') + "|" + ("").PadLeft(11, '-') + "|" + ("").PadLeft(10, '-') + "|" + ("-").PadLeft(12, '-') + "|" + ("-").PadLeft(10, '-') + "|" + ("-").PadLeft(14, '-');
            sb.AppendLine(strSeparatorLine);
            System.Diagnostics.Debug.Write(sb);



            List<long> reportDimensions = new List<long>();
            List<long> reportSkylineSize = new List<long>();
            List<long> reportTimeTotal = new List<long>();
            List<long> reportTimeAlgorithm = new List<long>();

            //For each preference set in the preference list
            for(int iPreferenceIndex = 0; iPreferenceIndex < listPreferences.Count; iPreferenceIndex++)
            {
                ArrayList preferences = (ArrayList)listPreferences[iPreferenceIndex];

                //Go only down two 3 dimension (because there are special algorithms for 1 and 2 dimensional skyline)
                for (int i = minDimensions; i <= preferences.Count; i++)
                {
                    //SELECT FROM
                    string strSQL = "SELECT cars.id FROM cars ";
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
                    if (strSQL.IndexOf("models") > 0)
                    {
                        strSQL += "LEFT OUTER JOIN models ON cars.model_id = models.ID ";
                        countJoins++;
                    }


                    //ADD Preferences to SKYLINE
                    ArrayList subPreferences = preferences.GetRange(0, i);
                    strSQL += "SKYLINE OF " + string.Join(",", (string[])subPreferences.ToArray(Type.GetType("System.String")));


                    //Convert to real SQL
                    SQLCommon parser = new SQLCommon();
                    parser.SkylineType = strategy;
                    parser.ShowSkylineAttributes = true;

                
                    if (GenerateScript == false)
                    {
                        for (int iTrial = 0; iTrial < Trials; iTrial++ )
                        {
                            Stopwatch sw = new Stopwatch();

                            try
                            {

                                sw.Start();
                                DataTable dt = parser.parseAndExecutePrefSQL(cnnStringLocalhost, driver, strSQL);
                                long timeAlgorithm = parser.TimeInMilliseconds;
                                sw.Stop();

                                reportDimensions.Add(i);
                                reportSkylineSize.Add(dt.Rows.Count);
                                reportTimeTotal.Add(sw.ElapsedMilliseconds);
                                reportTimeAlgorithm.Add(timeAlgorithm);


                                //trial|dimensions|skyline size|time total|time algorithm
                                string strTrial = iTrial+1 + " / " +  trials;
                                string strPreferenceSet = iPreferenceIndex + 1 + " / " + listPreferences.Count;
                                string strLine = strPreferenceSet.PadLeft(14) + "|" + strTrial.PadLeft(11) + "|" + (i).ToString().PadLeft(10) + "|" + dt.Rows.Count.ToString().PadLeft(12) + "|" + sw.ElapsedMilliseconds.ToString().PadLeft(10) + "|" + timeAlgorithm.ToString().PadLeft(14);

                                System.Diagnostics.Debug.WriteLine(strLine);
                                sb.AppendLine(strLine);


                            }
                            catch (Exception e)
                            {
                                Debug.WriteLine(e.Message);
                                return;
                            }
                        }
                    }
                    else
                    {
                        string[] sizes = { "small", "medium", "large", "superlarge" };

                        //Format for each of the customer profiles
                        sb.AppendLine("PRINT '----- -------------------------------------------------------- ------'");
                        sb.AppendLine("PRINT '----- " + (i + 1) + " dimensions, " + (countJoins) + " join(s) ------'");
                        sb.AppendLine("PRINT '----- -------------------------------------------------------- ------'");
                        foreach (string size in sizes)
                        {
                            sb.AppendLine("GO"); //we need this in order the profiler shows each query in a new line
                            sb.AppendLine(strSQL.Replace("cars", "cars_" + size));

                        }

                        sb.AppendLine("");
                        sb.AppendLine("");
                        sb.AppendLine("");
                    }


                    

                }
            }

            ////////////////////////////////
            //Summary
            ///////////////////////////////

            addSummary(sb, strSeparatorLine, reportDimensions, reportSkylineSize, reportTimeTotal, reportTimeAlgorithm);



            //Write in file
            string strFileName = "";
            string strFiletype = "";
            
            if(generateScript == false)
            {
                strFiletype = ".csv";
            }
            else
            {
                strFiletype = ".sql";
            }
            //create filename
            strFileName = path + "Performance_" + set.ToString() + "_" + strategy.ToString()   + strFiletype;
            
            StreamWriter outfile = new StreamWriter(strFileName);
            outfile.Write(sb.ToString());
            outfile.Close();

            
            

            //Debug.Write(result.Combinations<int>());
        }


        private void addSummary(StringBuilder sb, String strSeparatorLine, List<long> reportDimensions, List<long> reportSkylineSize, List<long> reportTimeTotal, List<long> reportTimeAlgorithm)
        {
            //Separator Line
            System.Diagnostics.Debug.WriteLine(strSeparatorLine);
            sb.AppendLine(strSeparatorLine);

            //average line
            //trial|dimensions|skyline size|time total|time algorithm
            string strAverage = "average".PadLeft(14) + "|" +  "".PadLeft(11) + "|" + reportDimensions.Average().ToString().PadLeft(10) + "|" + Math.Round(reportSkylineSize.Average(), 2).ToString().PadLeft(12) + "|" + Math.Round(reportTimeTotal.Average(), 2).ToString().PadLeft(10) + "|" + Math.Round(reportTimeAlgorithm.Average(),2).ToString().PadLeft(14);
            string strMin = "minimum".PadLeft(14) + "|" + "".PadLeft(11) + "|" + reportDimensions.Min().ToString().PadLeft(10) + "|" + reportSkylineSize.Min().ToString().PadLeft(12) + "|" + reportTimeTotal.Min().ToString().PadLeft(10) + "|" + reportTimeAlgorithm.Min().ToString().PadLeft(14);
            string strMax = "maximum".PadLeft(14) + "|" + "".PadLeft(11) + "|" + reportDimensions.Max().ToString().PadLeft(10) + "|" + reportSkylineSize.Max().ToString().PadLeft(12) + "|" + reportTimeTotal.Max().ToString().PadLeft(10) + "|" + reportTimeAlgorithm.Max().ToString().PadLeft(14);
            System.Diagnostics.Debug.WriteLine(strAverage);
            System.Diagnostics.Debug.WriteLine(strMin);
            System.Diagnostics.Debug.WriteLine(strMax);
            sb.AppendLine(strAverage);


            //Separator Line
            System.Diagnostics.Debug.WriteLine(strSeparatorLine);
            sb.AppendLine(strSeparatorLine);

        }


        private void getCombinations(ArrayList arr, int len, int startPosition, ArrayList result, ref ArrayList returnArray)
        {
            if(result.Count == 0)
            {
                for (int i = 0; i < len; i++)
                {
                    result.Add("");
                }
                    
            }

            if (len == 0)
            {
                Debug.WriteLine(string.Join(",", (string[])result.ToArray(Type.GetType("System.String"))));
                returnArray.Add(result.Clone());
                return;
            }
            for (int i = startPosition; i <= arr.Count - len; i++)
            {
                result[result.Count - len] = (string)arr[i];
                getCombinations(arr, len - 1, i + 1, result, ref returnArray);
            }
        } 
    

    }
    

}
