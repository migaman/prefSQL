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
using Utility.Model;
using prefSQL.SQLParser.Models;

namespace Utility
{
    class Performance
    {

        private const string path = "E:\\Doc\\Studies\\PRJ_Thesis\\19 Performance Level\\";
        private const string cnnStringLocalhost = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
        private const string driver = "System.Data.SqlClient";
        private int trials = 5;
        private int dimensions = 6;
        private int randomLoops = 25; //Only used for the shuffle set. How many random set will be generated
        private PreferenceSet set;
        private bool generateScript = false;
        private SkylineStrategy strategy;
        static Random rnd = new Random();
        private int minDimensions = 3;
        private Mathematic mathematic = new Mathematic();


        #region getter/setters

        public int Dimensions
        {
            get { return dimensions; }
            set { dimensions = value; }
        }
        

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

        #endregion


        public enum PreferenceSet
        {
            Jon,
            Mya,
            Barra,
            Shuffle,
            Combination,
            Correlation,
            AntiCorrelation,
            Independent
        };



        private ArrayList getNumericPreferences()
        {
            ArrayList preferences = new ArrayList();

            //Numeric/Date preferences
            preferences.Add("cars.price LOW");
            preferences.Add("cars.mileage LOW");
            preferences.Add("cars.horsepower HIGH");
            preferences.Add("cars.enginesize HIGH");
            preferences.Add("cars.consumption LOW");
            preferences.Add("cars.doors HIGH");
            preferences.Add("cars.seats HIGH");
            preferences.Add("cars.cylinders HIGH");
            preferences.Add("cars.gears HIGH");
            preferences.Add("cars.registrationNumeric HIGH");
            //preferencesAll.Add("DATEDIFF(DAY, '1900-01-01', cars.Registration) HIGH");

            return preferences;
        }

        private ArrayList getCategoricalPreferences()
        {
            ArrayList preferences = new ArrayList();

            //Categorical preferences with a cardinality from 2 to 8 (descending)
            preferences.Add("colors.name ('rot' >> 'blau' >> 'grün' >> 'gold' >> 'schwarz' >> 'grau' >> 'bordeaux' >> OTHERS EQUAL)");
            preferences.Add("bodies.name ('Bus' >> 'Cabriolet' >> 'Limousine' >> 'Coupé' >> 'Kasten' >> 'Kombi' >> OTHERS EQUAL)");
            preferences.Add("fuels.name ('Benzin' >> 'Diesel' >> 'Bioethanol' >> 'Elektro' >> 'Gas' >> 'Hybrid' >> OTHERS EQUAL)");
            preferences.Add("makes.name ('BENTLEY' >> 'DAIMLER' >> 'FIAT'>> 'FORD'  >> OTHERS EQUAL)");
            preferences.Add("conditions.name ('Neu' >> 'Occasion' >> 'Vorführmodell' >> 'Oldtimer' >> OTHERS EQUAL)");
            preferences.Add("drives.name ('Vorderradantrieb' >> 'Allrad' >> 'Hinterradantrieb' >> OTHERS EQUAL)");
            preferences.Add("transmissions.name ('Schaltgetriebe' >> 'Automat' >> OTHERS EQUAL)");


            return preferences;
        }

        private ArrayList getAllPreferences()
        {
            ArrayList preferences = new ArrayList();
            preferences.AddRange(getNumericPreferences());
            preferences.AddRange(getCategoricalPreferences());
           
            return preferences;
        }



        private DataTable getSQLFromPreferences(ArrayList preferences, bool cardinality)
        {
            SQLCommon common = new SQLCommon();
            string strPrefSQL = "SELECT cars.id FROM cars ";
            strPrefSQL += "SKYLINE OF ";   


            for (int i = 0; i < preferences.Count; i++)
            {
                strPrefSQL += preferences[i].ToString() + ",";
            }
            strPrefSQL = strPrefSQL.TrimEnd(',');

            PrefSQLModel prefModel = common.GetPrefSqlModelFromPreferenceSql(strPrefSQL);

            string strSQL = "SELECT ";

            for (int i = 0; i < prefModel.Skyline.Count; i++)
            {
                if (cardinality == true)
                {
                    strSQL += "COUNT(DISTINCT " + prefModel.Skyline[i].Expression + "),";
                }
                else
                {
                    strSQL += prefModel.Skyline[i].Expression + ",";
                }
            }
            strSQL = strSQL.TrimEnd(',') + " FROM cars ";
            strSQL += "LEFT OUTER JOIN colors ON cars.color_id = colors.ID ";
            strSQL += "LEFT OUTER JOIN fuels ON cars.fuel_id = fuels.ID ";
            strSQL += "LEFT OUTER JOIN bodies ON cars.body_id = bodies.ID ";
            strSQL += "LEFT OUTER JOIN makes ON cars.make_id = makes.ID ";
            strSQL += "LEFT OUTER JOIN conditions ON cars.condition_id = conditions.ID ";
            strSQL += "LEFT OUTER JOIN models ON cars.model_id = models.ID ";
            strSQL += "LEFT OUTER JOIN transmissions ON cars.transmission_id = transmissions.ID ";
            strSQL += "LEFT OUTER JOIN drives ON cars.drive_id = drives.ID ";


            SqlConnection conn = new SqlConnection(cnnStringLocalhost);
            conn.Open();
            SqlCommand cmd = new SqlCommand(strSQL, conn);

            DataTable dt = new DataTable();
            dt.Load(cmd.ExecuteReader());
            return dt;
        }

        private ArrayList getCorrelationMatrix(ArrayList preferences)
        {
            DataTable dt = getSQLFromPreferences(preferences, false);
            double[] colA = new double[dt.Rows.Count];
            double[] colB = new double[dt.Rows.Count];

            //Calculate correlation between the attributes
            ArrayList listCorrelation = new ArrayList();
            for (int iIndex = 0; iIndex < preferences.Count; iIndex++)
            {
                for (int iPref = 0; iPref <= iIndex; iPref++)
                {
                    //Don't compare same preferences (correlation is always 1)
                    if (iIndex != iPref)
                    {
                        for (int i = 0; i < dt.Rows.Count; i++)
                        {
                            colA[i] = (int)dt.Rows[i][iIndex];
                            colB[i] = (int)dt.Rows[i][iPref];
                        }

                        double correlation = mathematic.getPearson(colA, colB);
                        Utility.Model.CorrelationModel model = new Model.CorrelationModel(preferences[iIndex].ToString(), preferences[iPref].ToString(), correlation);
                        listCorrelation.Add(model);
                    }

                    
                }
            }

            return listCorrelation;
        }



        private ArrayList getCardinalityOfPreferences(ArrayList preferences)
        {
            DataTable dt = getSQLFromPreferences(preferences, true);
            
            //Calculate correlation between the attributes
            ArrayList listCardinality = new ArrayList();

            for (int iIndex = 0; iIndex < preferences.Count; iIndex++)
            {
                CardinalityModel model = new CardinalityModel(preferences[iIndex].ToString(), (int)dt.Rows[0][iIndex]);
                listCardinality.Add(model);
            }

            return listCardinality;
        }
       




        public void generatePerformanceQueries()
        {
            //Open DBConnection --> Otherwise first query is slower as usual, because DBConnection is not open
            SQLCommon parser = new SQLCommon();
            DataTable dt = parser.parseAndExecutePrefSQL(cnnStringLocalhost, driver, "SELECT cars.id FROM cars SKYLINE OF cars.price LOW");

            //Use the correct line, depending on how incomparable items should be compared
            ArrayList listPreferences = new ArrayList();
            ArrayList correlationMatrix = new ArrayList();
            ArrayList listCardinality = new ArrayList();
            

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
                correlationMatrix = getCorrelationMatrix(preferences);

                listPreferences.Add(preferences);
                listCardinality = getCardinalityOfPreferences(preferences);

            }
            else if (set == PreferenceSet.Mya)
            {
                ArrayList preferences = new ArrayList();
                preferences.Add("fuels.name ('Benzin' >> OTHERS EQUAL)");
                preferences.Add("makes.name ('FISKER' >> OTHERS EQUAL)");
                preferences.Add("bodies.name ('Roller' >> OTHERS EQUAL)");
                preferences.Add("models.name ('123' >> OTHERS EQUAL)");
                correlationMatrix = getCorrelationMatrix(preferences);

                listPreferences.Add(preferences);
                listCardinality = getCardinalityOfPreferences(preferences);
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
                correlationMatrix = getCorrelationMatrix(preferences);

                listPreferences.Add(preferences);
                listCardinality = getCardinalityOfPreferences(preferences);
            }
            else if (set == PreferenceSet.Combination)
            {
                //Tests every possible combination with y preferences from the whole set of preferences
                ArrayList preferences = getAllPreferences();
                correlationMatrix = getCorrelationMatrix(preferences);
                listCardinality = getCardinalityOfPreferences(preferences);

                getCombinations(preferences, dimensions, 0, new ArrayList(), ref listPreferences);
                
                //set mindimensions to maxdimension (test with fixed amount of preferences)
                minDimensions = dimensions;

            }
            else if (set == PreferenceSet.Shuffle)
            {
                //Tests x times randomly y preferences

                ArrayList preferencesAll = getAllPreferences();
                correlationMatrix = getCorrelationMatrix(preferencesAll);
                listCardinality = getCardinalityOfPreferences(preferencesAll);

                for (int iChoose = 0; iChoose < randomLoops; iChoose++)
                {
                    ArrayList preferences = new ArrayList();
                    ArrayList preferencesChoose = (ArrayList)preferencesAll.Clone();

                    //Choose x preferences randomly
                    for (int i = 0; i < dimensions; i++)
                    {
                        int r = rnd.Next(preferencesChoose.Count);
                        preferences.Add(preferencesChoose[r]);
                        preferencesChoose.RemoveAt(r);
                    }

                    listPreferences.Add(preferences);

                }

                //set mindimensions to maxdimension (test with fixed amount of preferences)
                minDimensions = dimensions;
            }
            else if (set == PreferenceSet.Correlation)
            {
                //Tests every possible combination with y preferences from the whole set of preferences
                ArrayList preferences = getAllPreferences();
                correlationMatrix = getCorrelationMatrix(preferences);
                listCardinality = getCardinalityOfPreferences(preferences);

                //Sort correlations to find the strongest
                correlationMatrix.Sort(new CorrelationModel());

                preferences.Clear();
                
                //Take only the two preferences with the best correlation
                CorrelationModel model = (CorrelationModel)correlationMatrix[0];          
                preferences.Add(model.ColA);
                preferences.Add(model.ColB);
                listPreferences.Add(preferences);
                
                //only the two dimensions should be tested
                minDimensions = 2;
                dimensions = 2;

            }
            else if (set == PreferenceSet.AntiCorrelation)
            {
                //Tests every possible combination with y preferences from the whole set of preferences
                ArrayList preferences = getAllPreferences();
                correlationMatrix = getCorrelationMatrix(preferences);
                listCardinality = getCardinalityOfPreferences(preferences);

                //Sort correlations to find the strongest
                correlationMatrix.Sort(new CorrelationModel());

                preferences.Clear();

                //Take only the two preferences with the best correlation
                CorrelationModel model = (CorrelationModel)correlationMatrix[correlationMatrix.Count-1];
                preferences.Add(model.ColA);
                preferences.Add(model.ColB);
                listPreferences.Add(preferences);


                //only the two dimensions should be tested
                minDimensions = 2;
                dimensions = 2;
            }
            else if (set == PreferenceSet.Independent)
            {
                //Tests every possible combination with y preferences from the whole set of preferences
                ArrayList preferences = getAllPreferences();
                correlationMatrix = getCorrelationMatrix(preferences);
                listCardinality = getCardinalityOfPreferences(preferences);

                //Sort correlations to find the strongest
                correlationMatrix.Sort(new CorrelationModel());

                preferences.Clear();

                //Find the most independent atributes (closest to zero)
                CorrelationModel modelBefore = new CorrelationModel();
                CorrelationModel modelAfter = new CorrelationModel();
                for (int i = 0; i <= correlationMatrix.Count; i++)
                {
                    CorrelationModel model = (CorrelationModel)correlationMatrix[i];
                    if(model.Correlation > 0)
                    {
                        //continue until the correlation turnaround
                        modelBefore = model;
                    }
                    else
                    {
                        modelAfter = model;
                        //Leave the function, because now the correlation is getting worse
                        break;
                    }
                }

                //Add the two preferences to the list, that are closer to zero
                if (Math.Abs(modelBefore.Correlation) > Math.Abs(modelAfter.Correlation))
                {
                    preferences.Add(modelAfter.ColA);
                    preferences.Add(modelAfter.ColB);
                }
                else
                {
                    preferences.Add(modelBefore.ColA);
                    preferences.Add(modelBefore.ColB);
                }
                listPreferences.Add(preferences);


                //only the two dimensions should be tested
                minDimensions = 2;
                dimensions = 2;
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
            sb.AppendLine(formatLineString(' ', "preference set","trial", "dimensions", "skyline size", "time total", "time algorithm", "sum correlation", "sum cardinality"));
            string strSeparatorLine = formatLineString('-', "", "", "", "", "", "", "", "");


            sb.AppendLine(strSeparatorLine);
            Debug.Write(sb);



            List<long> reportDimensions = new List<long>();
            List<long> reportSkylineSize = new List<long>();
            List<long> reportTimeTotal = new List<long>();
            List<long> reportTimeAlgorithm = new List<long>();
            List<double> reportCorrelation = new List<double>();
            List<double> reportCardinality = new List<double>();

            //For each preference set in the preference list
            for(int iPreferenceIndex = 0; iPreferenceIndex < listPreferences.Count; iPreferenceIndex++)
            {
                ArrayList preferences = (ArrayList)listPreferences[iPreferenceIndex];
                //Go only down two 3 dimension (because there are special algorithms for 1 and 2 dimensional skyline)
                for (int i = minDimensions; i <= preferences.Count; i++)
                {
                    //ADD Preferences to SKYLINE
                    ArrayList subPreferences = preferences.GetRange(0, i);
                    string strSkylineOf = "SKYLINE OF " + string.Join(",", (string[])subPreferences.ToArray(Type.GetType("System.String")));

                    //SELECT FROM
                    string strSQL = "SELECT cars.id FROM cars ";
                    int countJoins = 0;
                    //Add Joins
                    if (strSkylineOf.IndexOf("colors") > 0)
                    {
                        strSQL += "LEFT OUTER JOIN colors ON cars.color_id = colors.ID ";
                        countJoins++;
                    }
                    if (strSkylineOf.IndexOf("fuels") > 0)
                    {
                        strSQL += "LEFT OUTER JOIN fuels ON cars.fuel_id = fuels.ID ";
                        countJoins++;
                    }
                    if (strSkylineOf.IndexOf("bodies") > 0)
                    {
                        strSQL += "LEFT OUTER JOIN bodies ON cars.body_id = bodies.ID ";
                        countJoins++;
                    }
                    if (strSkylineOf.IndexOf("makes") > 0)
                    {
                        strSQL += "LEFT OUTER JOIN makes ON cars.make_id = makes.ID ";
                        countJoins++;
                    }
                    if (strSkylineOf.IndexOf("conditions") > 0)
                    {
                        strSQL += "LEFT OUTER JOIN conditions ON cars.condition_id = conditions.ID ";
                        countJoins++;
                    }
                    if (strSkylineOf.IndexOf("models") > 0)
                    {
                        strSQL += "LEFT OUTER JOIN models ON cars.model_id = models.ID ";
                        countJoins++;
                    }
                    if (strSkylineOf.IndexOf("transmissions") > 0)
                    {
                        strSQL += "LEFT OUTER JOIN transmissions ON cars.transmission_id = transmissions.ID ";
                        countJoins++;
                    }
                    if (strSkylineOf.IndexOf("drives") > 0)
                    {
                        strSQL += "LEFT OUTER JOIN drives ON cars.drive_id = drives.ID ";
                        countJoins++;
                    }


                    //Add Skyline-Clause
                    strSQL += strSkylineOf;


                    //Convert to real SQL
                    parser = new SQLCommon();
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
                                dt = parser.parseAndExecutePrefSQL(cnnStringLocalhost, driver, strSQL);
                                long timeAlgorithm = parser.TimeInMilliseconds;
                                sw.Stop();
                                double correlation = searchCorrelation(subPreferences, correlationMatrix);
                                double cardinality = searchCardinality(subPreferences, listCardinality);

                                reportDimensions.Add(i);
                                reportSkylineSize.Add(dt.Rows.Count);
                                reportTimeTotal.Add(sw.ElapsedMilliseconds);
                                reportTimeAlgorithm.Add(timeAlgorithm);
                                reportCorrelation.Add(correlation);
                                reportCardinality.Add(cardinality);

                                //trial|dimensions|skyline size|time total|time algorithm
                                string strTrial = iTrial+1 + " / " +  trials;
                                string strPreferenceSet = iPreferenceIndex + 1 + " / " + listPreferences.Count;


                                string strLine = formatLineString(strPreferenceSet, strTrial, i, dt.Rows.Count, sw.ElapsedMilliseconds, timeAlgorithm, correlation, cardinality);
 
                                
                                Debug.WriteLine(strLine);
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

            addSummary(sb, strSeparatorLine, reportDimensions, reportSkylineSize, reportTimeTotal, reportTimeAlgorithm, reportCorrelation, reportCardinality);



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

        }


        private double searchCorrelation(ArrayList preferences, ArrayList correlationMatrix)
        {

            double sumCorrelation = 0;
            for (int i = 0; i < preferences.Count; i++)
            {
                for (int ii = i+1; ii < preferences.Count; ii++)
                {
                    bool bFound = false;
                    for (int iModel = 0; iModel < correlationMatrix.Count; iModel++)
                    {
                        CorrelationModel model = (CorrelationModel)correlationMatrix[iModel];
                        if (model.ColA.Equals(preferences[i].ToString()) && model.ColB.Equals(preferences[ii].ToString()))
                        {
                            sumCorrelation += model.Correlation;
                            bFound = true;
                            break;
                        }
                        else if (model.ColB.Equals(preferences[i].ToString()) && model.ColA.Equals(preferences[ii].ToString()))
                        {
                            bFound = true;
                            sumCorrelation += model.Correlation;
                            break;
                        }
                    }
                    if (bFound == false)
                    {
                        throw new Exception("correlation factor not found");
                    }
                }
            }
            return sumCorrelation;
        }



        private double searchCardinality(ArrayList preferences, ArrayList cardinality)
        {

            double sum = 0;
            for (int i = 0; i < preferences.Count; i++)
            {
                bool bFound = false;
                for (int iModel = 0; iModel < cardinality.Count; iModel++)
                {
                    CardinalityModel model = (CardinalityModel)cardinality[iModel];
                    if (model.Col.Equals(preferences[i].ToString()))
                    {
                        sum += model.Cardinality;
                        bFound = true;
                        break;
                    }
                }
                if (bFound == false)
                {
                    throw new Exception("cardinality factor not found");
                }
            }
            return sum;
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
                //Debug.WriteLine(string.Join(",", (string[])result.ToArray(Type.GetType("System.String"))));
                returnArray.Add(result.Clone());
                return;
            }
            for (int i = startPosition; i <= arr.Count - len; i++)
            {
                result[result.Count - len] = (string)arr[i];
                getCombinations(arr, len - 1, i + 1, result, ref returnArray);
            }
        }

        


        #region formatOutput


        private void addSummary(StringBuilder sb, String strSeparatorLine, List<long> reportDimensions, List<long> reportSkylineSize, List<long> reportTimeTotal, List<long> reportTimeAlgorithm, List<double> reportCorrelation, List<double> reportCardinality)
        {
            //Separator Line
            Debug.WriteLine(strSeparatorLine);
            sb.AppendLine(strSeparatorLine);


            string strAverage = formatLineString("average", "", reportDimensions.Average(), reportSkylineSize.Average(), reportTimeTotal.Average(), reportTimeAlgorithm.Average(), reportCorrelation.Average(), reportCardinality.Average());
            string strMin = formatLineString("minimum", "", reportDimensions.Min(), reportSkylineSize.Min(), reportTimeTotal.Min(), reportTimeAlgorithm.Min(), reportCorrelation.Min(), reportCardinality.Min());
            string strMax = formatLineString("maximum", "", reportDimensions.Max(), reportSkylineSize.Max(), reportTimeTotal.Max(), reportTimeAlgorithm.Max(), reportCorrelation.Max(), reportCardinality.Max());
            string strVar = formatLineString("variance", "", mathematic.getVariance(reportDimensions), mathematic.getVariance(reportSkylineSize), mathematic.getVariance(reportTimeTotal), mathematic.getVariance(reportTimeAlgorithm), mathematic.getVariance(reportCorrelation), mathematic.getVariance(reportCardinality));
            string strStd = formatLineString("stdderivation", "", mathematic.getStdDerivation(reportDimensions), mathematic.getStdDerivation(reportSkylineSize), mathematic.getStdDerivation(reportTimeTotal), mathematic.getStdDerivation(reportTimeAlgorithm), mathematic.getStdDerivation(reportCorrelation), mathematic.getStdDerivation(reportCardinality));

            sb.AppendLine(strAverage);
            sb.AppendLine(strMin);
            sb.AppendLine(strMax);
            sb.AppendLine(strVar);
            sb.AppendLine(strStd);
            Debug.WriteLine(strAverage);
            Debug.WriteLine(strMin);
            Debug.WriteLine(strMax);
            Debug.WriteLine(strVar);
            Debug.WriteLine(strStd);

            //Separator Line
            sb.AppendLine(strSeparatorLine);
            Debug.WriteLine(strSeparatorLine);
        }



        private string formatLineString(char paddingChar, string strTitle, string strTrial, string strDimension, string strSkyline, string strTimeTotal, string strTimeAlgo, string strCorrelation, string strCardinality)
        {
            //average line
            //trial|dimensions|skyline size|time total|time algorithm|correlation|
            string[] line = new string[9];
            line[0] = strTitle.PadLeft(14, paddingChar);
            line[1] = strTrial.PadLeft(11, paddingChar);
            line[2] = strDimension.PadLeft(10, paddingChar);
            line[3] = strSkyline.PadLeft(12, paddingChar);
            line[4] = strTimeTotal.PadLeft(10, paddingChar);
            line[5] = strTimeAlgo.PadLeft(14, paddingChar);
            line[6] = strCorrelation.PadLeft(15, paddingChar);
            line[7] = strCardinality.PadLeft(15, paddingChar);
            line[8] = "";
            return string.Join("|", line);
        }

        private string formatLineString(string strTitle, string strTrial, double dimension, double skyline, double timeTotal, double timeAlgo, double correlation, double cardinality)
        {
            return formatLineString(' ', strTitle, strTrial, Math.Round(dimension, 2).ToString(), Math.Round(skyline, 2).ToString(), Math.Round(timeTotal, 2).ToString(), Math.Round(timeAlgo, 2).ToString(), Math.Round(correlation, 2).ToString(), Math.Round(cardinality, 2).ToString());
        }



        #endregion


    }
    

}
