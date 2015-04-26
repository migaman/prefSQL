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
    /// <summary>
    /// Performance class implented on a similar idea like Lofi (2014)
    /// 
    /// </summary>
    /// <remarks>
    /// Lofi, Christoph (2014): skyline_simulator
    /// Bitbucket.      /// Available online at https://bitbucket.org/clofi/skyline_simulator.
    /// 
    /// </remarks>
    


    class Performance
    {

        private const string path = "E:\\Doc\\Studies\\PRJ_Thesis\\43 Correlation\\";
        private const string cnnStringLocalhost = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
        private const string driver = "System.Data.SqlClient";
        private int trials = 5;                 //How many times each preferene query is executed
        private int dimensions = 6;             //Up to this amount of dimension should be tested
        private int randomDraws = 25;          //Only used for the shuffle set. How many random set will be generated
        private PreferenceSet set;              //Which preference set (see enum) that should be tested
        private bool generateScript = false;   
        private SkylineStrategy strategy;
        static Random rnd = new Random();
        private int minDimensions = 2;
        private Size tableSize;

        internal Size TableSize
        {
            get { return tableSize; }
            set { tableSize = value; }
        }


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

        public int RandomDraws
        {
            get { return randomDraws; }
            set { randomDraws = value; }
        }

        #endregion


        public enum Size
        {
            Small,
            Medium,
            Large,
            Superlarge
        }

        public enum PreferenceSet
        {
            ArchiveComparable,      //Preferences from first performance tests, up to 13 dimnension
            ArchiveIncomparable,    //Preferences from first performance tests, up to 13 dimnension
            Jon,                    //Preference set from 2nd peformance phase
            Mya,                    //Preference set from 2nd peformance phase
            Barra,                  //Preference set from 2nd peformance phase
            Shuffle,                //Choose randomly preferences from all preferences
            Combination,            //Take all preferences
            CombinationNumeric,     //Take only numeric preferences
            CombinationCategoric,   //Take only categoric preferences
            CombinationHexagon,      //Special collection of preferences which perform well on Hexagon
            Correlation,            //Take 2 best correlated preferences
            AntiCorrelation,        //Take 2 worst correlated preferences
            Independent,            //Take 2 most independent correlated preferences
            
        };


        private ArrayList getArchiveComparablePreferences()
        {
            ArrayList preferences = new ArrayList();

            preferences.Add("cars.price LOW");
            preferences.Add("cars.mileage LOW");
            preferences.Add("cars.horsepower HIGH");
            preferences.Add("cars.enginesize HIGH");
            preferences.Add("cars.registrationNumeric HIGH");
            preferences.Add("cars.consumption LOW");
            preferences.Add("cars.doors HIGH");
            preferences.Add("colors.name ('red' == 'blue' >> OTHERS EQUAL >> 'gray')");
            preferences.Add("fuels.name ('petrol' >> OTHERS EQUAL >> 'Diesel')");
            preferences.Add("bodies.name ('compact car' >> 'bus' >> 'estate car' >> 'scooter' >> OTHERS EQUAL >> 'pick-up')");
            preferences.Add("cars.title ('MERCEDES-BENZ SL 600' >> OTHERS EQUAL)");
            preferences.Add("makes.name ('ASTON MARTIN' >> 'VW' == 'Audi' >> OTHERS EQUAL >> 'FERRARI')");
            preferences.Add("conditions.name ('new' >> OTHERS EQUAL)");

            return preferences;
        }

        private ArrayList getArchiveIncomparablePreferences()
        {
            ArrayList preferences = new ArrayList();

            preferences.Add("cars.price LOW");
            preferences.Add("cars.mileage LOW");
            preferences.Add("cars.horsepower HIGH");
            preferences.Add("cars.enginesize HIGH");
            preferences.Add("cars.registrationNumeric HIGH");
            preferences.Add("cars.consumption LOW");
            preferences.Add("cars.doors HIGH");
            preferences.Add("colors.name ('red' == 'blue' >> OTHERS INCOMPARABLE >> 'gray')");
            preferences.Add("fuels.name ('petrol' >> OTHERS INCOMPARABLE >> 'Diesel')");
            preferences.Add("bodies.name ('compact car' >> 'bus' >> 'estate car' >> 'scooter' >> OTHERS INCOMPARABLE >> 'pick-up')");
            preferences.Add("cars.title ('MERCEDES-BENZ SL 600' >> OTHERS INCOMPARABLE)");
            preferences.Add("makes.name ('ASTON MARTIN' >> 'VW' == 'Audi' >> OTHERS INCOMPARABLE >> 'FERRARI')");
            preferences.Add("conditions.name ('new' >> OTHERS INCOMPARABLE)");


            return preferences;
        }


        private ArrayList getJonsPreferences()
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

            return preferences;
        }

        private ArrayList getMyasPreferences()
        {
            ArrayList preferences = new ArrayList();
            preferences.Add("fuels.name ('petrol' >> OTHERS EQUAL)");
            preferences.Add("makes.name ('FISKER' >> OTHERS EQUAL)");
            preferences.Add("bodies.name ('scooter' >> OTHERS EQUAL)");
            preferences.Add("models.name ('123' >> OTHERS EQUAL)");
            return preferences;
        }

        private ArrayList getBarrasPreferences()
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
            return preferences;
        }

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

            return preferences;
        }

        private ArrayList getCategoricalPreferences()
        {
            ArrayList preferences = new ArrayList();

            //Categorical preferences with a cardinality from 2 to 8 (descending)
            preferences.Add("colors.name ('red' >> 'blue' >> 'green' >> 'gold' >> 'black' >> 'gray' >> 'bordeaux' >> OTHERS EQUAL)");
            preferences.Add("bodies.name ('bus' >> 'cabriolet' >> 'limousine' >> 'coupé' >> 'van' >> 'estate car' >> OTHERS EQUAL)");
            preferences.Add("fuels.name ('petrol' >> 'diesel' >> 'bioethanol' >> 'electro' >> 'gas' >> 'hybrid' >> OTHERS EQUAL)");
            preferences.Add("makes.name ('BENTLEY' >> 'DAIMLER' >> 'FIAT'>> 'FORD'  >> OTHERS EQUAL)");
            preferences.Add("conditions.name ('new' >> 'occasion' >> 'demonstraction car' >> 'oldtimer' >> OTHERS EQUAL)");
            preferences.Add("drives.name ('front wheel' >> 'all wheel' >> 'rear wheel' >> OTHERS EQUAL)");
            preferences.Add("transmissions.name ('manual' >> 'automatic' >> OTHERS EQUAL)");


            return preferences;
        }

        private ArrayList getSpecialHexagonPreferences()
        {
            ArrayList preferences = new ArrayList();

            //Categorical preferences with a cardinality from 2 to 8 (descending)
            preferences.Add("cars.doors HIGH");
            preferences.Add("fuels.name ('petrol' >> 'diesel' >> 'bioethanol' >> 'elektro' >> 'gas' >> 'hybrid' >> OTHERS EQUAL)");
            preferences.Add("conditions.name ('new' >> 'occasion' >> 'demonstration model' >> 'oldtimer' >> OTHERS EQUAL)");
            preferences.Add("drives.name ('front wheel' >> 'all wheel' >> 'rear wheel' >> OTHERS EQUAL)");
            preferences.Add("transmissions.name ('manual' >> 'automatic' >> OTHERS EQUAL)");
            

            /*
             * TODO: Mit diesen beiden speziellen präferenzen ist hexagon schneller als SQL und andere algos
             * */
            //preferences.Add("fuels.name ('petrol' >> 'diesel' >> 'bioethanol' >> 'elektro' >> 'gas' >> 'hybrid' >> OTHERS EQUAL)");
            //preferences.Add("cars.title ('AUDI Q7 3.0 TDI quattro' >> OTHERS EQUAL)");
            

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
            string strPrefSQL = "SELECT cars.id FROM ";
            if (tableSize == Size.Small)
            {
                strPrefSQL += "cars_small";
            }
            else if (tableSize == Size.Medium)
            {
                strPrefSQL += "cars_medium";
            }
            else if (tableSize == Size.Large)
            {
                strPrefSQL += "cars_large";
            }
            strPrefSQL += " cars ";
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
            strSQL += getJoinsForPreferences(strSQL);
            


            SqlConnection conn = new SqlConnection(cnnStringLocalhost);
            conn.Open();
            SqlCommand cmd = new SqlCommand(strSQL, conn);

            DataTable dt = new DataTable();
            dt.Load(cmd.ExecuteReader());
            return dt;
        }

        private string getJoinsForPreferences(string strSkylineOf)
        {
            string strSQL = "";
            if (strSkylineOf.IndexOf("colors") > 0)
            {
                strSQL += "LEFT OUTER JOIN colors ON cars.color_id = colors.ID ";
            }
            if (strSkylineOf.IndexOf("fuels") > 0)
            {
                strSQL += "LEFT OUTER JOIN fuels ON cars.fuel_id = fuels.ID ";
            }
            if (strSkylineOf.IndexOf("bodies") > 0)
            {
                strSQL += "LEFT OUTER JOIN bodies ON cars.body_id = bodies.ID ";
            }
            if (strSkylineOf.IndexOf("makes") > 0)
            {
                strSQL += "LEFT OUTER JOIN makes ON cars.make_id = makes.ID ";
            }
            if (strSkylineOf.IndexOf("conditions") > 0)
            {
                strSQL += "LEFT OUTER JOIN conditions ON cars.condition_id = conditions.ID ";
            }
            if (strSkylineOf.IndexOf("models") > 0)
            {
                strSQL += "LEFT OUTER JOIN models ON cars.model_id = models.ID ";
            }
            if (strSkylineOf.IndexOf("transmissions") > 0)
            {
                strSQL += "LEFT OUTER JOIN transmissions ON cars.transmission_id = transmissions.ID ";
            }
            if (strSkylineOf.IndexOf("drives") > 0)
            {
                strSQL += "LEFT OUTER JOIN drives ON cars.drive_id = drives.ID ";
            }


            return strSQL;
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


            if (set == PreferenceSet.ArchiveComparable)
            {
                ArrayList preferences = getArchiveComparablePreferences();
                correlationMatrix = getCorrelationMatrix(preferences);
                listCardinality = getCardinalityOfPreferences(preferences);
                //Only one set
                listPreferences.Add(preferences);
            }
            else if (set == PreferenceSet.ArchiveIncomparable)
            {
                ArrayList preferences = getArchiveIncomparablePreferences();
                correlationMatrix = getCorrelationMatrix(preferences);
                listCardinality = getCardinalityOfPreferences(preferences);
                //Only one set
                listPreferences.Add(preferences);
            }
            else if (set == PreferenceSet.Jon)
            {
                ArrayList preferences = getJonsPreferences();
                correlationMatrix = getCorrelationMatrix(preferences);
                listCardinality = getCardinalityOfPreferences(preferences);
                //Only one set
                listPreferences.Add(preferences);

            }
            else if (set == PreferenceSet.Mya)
            {
                ArrayList preferences = getMyasPreferences();
                correlationMatrix = getCorrelationMatrix(preferences);
                listCardinality = getCardinalityOfPreferences(preferences);
                //Only one set
                listPreferences.Add(preferences);
            }
            else if (set == PreferenceSet.Barra)
            {
                ArrayList preferences = getBarrasPreferences();
                correlationMatrix = getCorrelationMatrix(preferences);
                listCardinality = getCardinalityOfPreferences(preferences);
                //Only one set
                listPreferences.Add(preferences);
            }
            else if (set == PreferenceSet.Combination)
            {
                //Tests every possible combination with y preferences from the whole set of preferences
                ArrayList preferences = getAllPreferences();
                correlationMatrix = getCorrelationMatrix(preferences);
                listCardinality = getCardinalityOfPreferences(preferences);

                //create all possible combinations and add it to listPreferences
                getCombinations(preferences, dimensions, 0, new ArrayList(), ref listPreferences);
                
                //set mindimensions to maxdimension (test with fixed amount of preferences)
                minDimensions = dimensions;

            }
            else if (set == PreferenceSet.CombinationNumeric)
            {
                //Tests every possible combination with y preferences from the whole set of preferences
                ArrayList preferences = getNumericPreferences();
                correlationMatrix = getCorrelationMatrix(preferences);
                listCardinality = getCardinalityOfPreferences(preferences);

                //create all possible combinations and add it to listPreferences
                getCombinations(preferences, dimensions, 0, new ArrayList(), ref listPreferences);
                
                //set mindimensions to maxdimension (test with fixed amount of preferences)
                minDimensions = dimensions;

            }
            else if (set == PreferenceSet.CombinationCategoric)
            {
                //Tests every possible combination with y preferences from the whole set of preferences
                ArrayList preferences = getCategoricalPreferences();
                correlationMatrix = getCorrelationMatrix(preferences);
                listCardinality = getCardinalityOfPreferences(preferences);

                //create all possible combinations and add it to listPreferences
                getCombinations(preferences, dimensions, 0, new ArrayList(), ref listPreferences);
                
                //set mindimensions to maxdimension (test with fixed amount of preferences)
                minDimensions = dimensions;

            }
            else if (set == PreferenceSet.CombinationHexagon)
            {
                //Tests every possible combination with y preferences from the whole set of preferences
                ArrayList preferences = getSpecialHexagonPreferences();
                correlationMatrix = getCorrelationMatrix(preferences);
                listCardinality = getCardinalityOfPreferences(preferences);

                //create all possible combinations and add it to listPreferences
                getCombinations(preferences, dimensions, 0, new ArrayList(), ref listPreferences);

                //set mindimensions to maxdimension (test with fixed amount of preferences)
                minDimensions = dimensions;
            }
            
            else if (set == PreferenceSet.Shuffle)
            {
                //Tests x times randomly y preferences

                ArrayList preferences = getAllPreferences();
                correlationMatrix = getCorrelationMatrix(preferences);
                listCardinality = getCardinalityOfPreferences(preferences);

                for (int iChoose = 0; iChoose < randomDraws; iChoose++)
                {
                    ArrayList preferencesRandom = new ArrayList();
                    ArrayList preferencesChoose = (ArrayList)preferences.Clone();

                    //Choose x preferences randomly
                    for (int i = 0; i < dimensions; i++)
                    {
                        int r = rnd.Next(preferencesChoose.Count);
                        preferencesRandom.Add(preferencesChoose[r]);
                        preferencesChoose.RemoveAt(r);
                    }

                    //add random preferences to listPreferences
                    listPreferences.Add(preferencesRandom);

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

                //Sort correlations ascending
                CorrelationModel model = (CorrelationModel)correlationMatrix[0];
                preferences.Clear();
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

                //Sort correlations ascending
                correlationMatrix.Sort(new CorrelationModel());
                
                //Take only the two preferences with the worst correlation
                CorrelationModel model = (CorrelationModel)correlationMatrix[correlationMatrix.Count-1];
                preferences.Clear();
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
                preferences.Clear();
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
            
            
            StringBuilder sb = new StringBuilder();
            string strSeparatorLine = formatLineString('-', "", "", "", "", "", "", "", "", "");
            if (GenerateScript == false)
            {
                //Header
                sb.AppendLine("               Algorithm: " + strategy.ToString());
                sb.AppendLine("          Preference Set: " + set.ToString());
                sb.AppendLine("                    Host: " + System.Environment.MachineName);
                sb.AppendLine("      Set of Preferences: " + listPreferences.Count);
                sb.AppendLine("                  Trials: " + Trials);
                sb.AppendLine("              Table size: " + tableSize.ToString());
                sb.AppendLine("          Dimension from: " + minDimensions.ToString());
                sb.AppendLine("            Dimension to: " + dimensions.ToString());
                //sb.AppendLine("Correlation Coefficients:" + string.Join(",", (string[])preferences.ToArray(Type.GetType("System.String"))));
                //sb.AppendLine("           Cardinalities:" + string.Join(",", (string[])preferences.ToArray(Type.GetType("System.String"))));
                sb.AppendLine("");
                sb.AppendLine(formatLineString(' ', "preference set", "trial", "dimensions", "skyline size", "time total", "time algorithm", "sum correlation", "sum cardinality", "size BTG"));
                sb.AppendLine(strSeparatorLine);
                Debug.Write(sb);
            }


            
            List<long> reportDimensions = new List<long>();
            List<long> reportSkylineSize = new List<long>();
            List<long> reportTimeTotal = new List<long>();
            List<long> reportTimeAlgorithm = new List<long>();
            List<double> reportCorrelation = new List<double>();
            List<double> reportCardinality = new List<double>();
            List<double> reportSizeBTG = new List<double>();

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
                    string strSQL = "SELECT cars.id FROM ";
                    if (tableSize == Size.Small)
                    {
                        strSQL += "cars_small";
                    }
                    else if (tableSize == Size.Medium)
                    {
                        strSQL += "cars_medium";
                    }
                    else if (tableSize == Size.Large)
                    {
                        strSQL += "cars_large";
                    }
                    strSQL += " cars ";
                    //Add Joins
                    strSQL += getJoinsForPreferences(strSkylineOf);
                    


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
                                double sizeBTG = parser.SizeBTG;
                                reportDimensions.Add(i);
                                reportSkylineSize.Add(dt.Rows.Count);
                                reportTimeTotal.Add(sw.ElapsedMilliseconds);
                                reportTimeAlgorithm.Add(timeAlgorithm);
                                reportCorrelation.Add(correlation);
                                reportCardinality.Add(cardinality);
                                reportSizeBTG.Add(sizeBTG);

                                //trial|dimensions|skyline size|time total|time algorithm
                                string strTrial = iTrial+1 + " / " +  trials;
                                string strPreferenceSet = iPreferenceIndex + 1 + " / " + listPreferences.Count;


                                string strLine = formatLineString(strPreferenceSet, strTrial, i, dt.Rows.Count, sw.ElapsedMilliseconds, timeAlgorithm, correlation, cardinality, sizeBTG);

                                
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


                        strSQL = parser.parsePreferenceSQL(strSQL);

                        string[] sizes = { "small", "medium", "large", "superlarge" };

                        //Format for each of the customer profiles
                        sb.AppendLine("PRINT '----- -------------------------------------------------------- ------'");
                        sb.AppendLine("PRINT '----- " + (i + 1) + " dimensions  ------'");
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
            if (GenerateScript == false)
            {
                addSummary(sb, strSeparatorLine, reportDimensions, reportSkylineSize, reportTimeTotal, reportTimeAlgorithm, reportCorrelation, reportCardinality, reportSizeBTG);
            }
            



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


        private ArrayList getCorrelationMatrix(ArrayList preferences)
        {
            Mathematic mathematic = new Mathematic();
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


        private void addSummary(StringBuilder sb, String strSeparatorLine, List<long> reportDimensions, List<long> reportSkylineSize, List<long> reportTimeTotal, List<long> reportTimeAlgorithm, List<double> reportCorrelation, List<double> reportCardinality, List<double> reportSizeBTG)
        {
            //Separator Line
            Debug.WriteLine(strSeparatorLine);
            sb.AppendLine(strSeparatorLine);

            Mathematic mathematic = new Mathematic();
            string strAverage = formatLineString("average", "", reportDimensions.Average(), reportSkylineSize.Average(), reportTimeTotal.Average(), reportTimeAlgorithm.Average(), reportCorrelation.Average(), reportCardinality.Average(), reportSizeBTG.Average());
            string strMin = formatLineString("minimum", "", reportDimensions.Min(), reportSkylineSize.Min(), reportTimeTotal.Min(), reportTimeAlgorithm.Min(), reportCorrelation.Min(), reportCardinality.Min(), reportSizeBTG.Min());
            string strMax = formatLineString("maximum", "", reportDimensions.Max(), reportSkylineSize.Max(), reportTimeTotal.Max(), reportTimeAlgorithm.Max(), reportCorrelation.Max(), reportCardinality.Max(), reportSizeBTG.Max());
            string strVar = formatLineString("variance", "", mathematic.getVariance(reportDimensions), mathematic.getVariance(reportSkylineSize), mathematic.getVariance(reportTimeTotal), mathematic.getVariance(reportTimeAlgorithm), mathematic.getVariance(reportCorrelation), mathematic.getVariance(reportCardinality), mathematic.getVariance(reportSizeBTG));
            string strStd = formatLineString("stddeviation", "", mathematic.getStdDeviation(reportDimensions), mathematic.getStdDeviation(reportSkylineSize), mathematic.getStdDeviation(reportTimeTotal), mathematic.getStdDeviation(reportTimeAlgorithm), mathematic.getStdDeviation(reportCorrelation), mathematic.getStdDeviation(reportCardinality), mathematic.getStdDeviation(reportSizeBTG));

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



        private string formatLineString(char paddingChar, string strTitle, string strTrial, string strDimension, string strSkyline, string strTimeTotal, string strTimeAlgo, string strCorrelation, string strCardinality, string strSizeBTG)
        {
            //average line
            //trial|dimensions|skyline size|time total|time algorithm|correlation|
            string[] line = new string[10];
            line[0] = strTitle.PadLeft(14, paddingChar);
            line[1] = strTrial.PadLeft(11, paddingChar);
            line[2] = strDimension.PadLeft(10, paddingChar);
            line[3] = strSkyline.PadLeft(12, paddingChar);
            line[4] = strTimeTotal.PadLeft(10, paddingChar);
            line[5] = strTimeAlgo.PadLeft(14, paddingChar);
            line[6] = strCorrelation.PadLeft(15, paddingChar);
            line[7] = strCardinality.PadLeft(15, paddingChar);
            line[8] = strSizeBTG.PadLeft(8, paddingChar);
            line[9] = "";
            return string.Join("|", line);
        }

        private string formatLineString(string strTitle, string strTrial, double dimension, double skyline, double timeTotal, double timeAlgo, double correlation, double cardinality, double sizeBTG)
        {
            return formatLineString(' ', strTitle, strTrial, Math.Round(dimension, 2).ToString(), Math.Round(skyline, 2).ToString(), Math.Round(timeTotal, 2).ToString(), Math.Round(timeAlgo, 2).ToString(), Math.Round(correlation, 2).ToString(), Math.Round(cardinality, 2).ToString(), Math.Round(sizeBTG, 2).ToString());
        }



        #endregion


    }
    

}
