using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using prefSQL.SQLParser;
using prefSQL.SQLParser.Models;
using prefSQL.SQLSkyline;
using Utility.Model;

namespace Utility
{
    using System.Collections.ObjectModel;
    using System.Data.Common;
    using System.Globalization;
    using System.Numerics;
    using System.Windows.Forms;
    using prefSQL.Evaluation;
    using prefSQL.SQLParserTest;
    using prefSQL.SQLSkyline.SkylineSampling;

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

        private const string Path = @"C:\Users\Public\Documents\workspace\prefcom\prefSQL\root\PerformanceTests\";
        private int _trials = 5;                 //How many times each preferene query is executed  
        private int _randomDraws = 25;          //Only used for the shuffle set. How many random set will be generated
        static readonly Random Rnd = new Random();
        static readonly Mathematic MyMathematic = new Mathematic();

        public Performance()
        {
            UseCLR = false;
        }

        public int MinDimensions { get; set; }  //Up from this amount of dimension should be tested
        public int MaxDimensions { get; set; }  //Up to this amount of dimension should be tested

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
            
            All,                    //Take all preferences
            Numeric,                //Take only numeric preferences
            Categoric,              //Take only categoric preferences
            MinCardinality,         //Special collection of preferences which should perform well on Hexagon


        };

        public enum PreferenceChooseMode
        {
            Combination,            //Test every possible combination of the preferences
            Shuffle,                //Choose x randomly preferences from all possible combinations
            Correlation,            //Take 2 best correlated preferences
            AntiCorrelation,        //Take 2 worst correlated preferences
            Independent,            //Take 2 most independent correlated preferences
        }


        #region getter/setters

        public bool UseCLR { get; set; }

        internal Size TableSize { get; set; }
        

        public SkylineStrategy Strategy { get; set; }

        public bool GenerateScript { get; set; }

        internal PreferenceSet Set { get; set; }

        public PreferenceChooseMode Mode { get; set; }

        public int Trials
        {
            get { return _trials; }
            set { _trials = value; }
        }

        public int RandomDraws
        {
            get { return _randomDraws; }
            set { _randomDraws = value; }
        }

        public bool Sampling { get; set; }

        internal int SamplingSubspacesCount { get; set; }

        internal int SamplingSubspaceDimension { get; set; }

        internal int SamplingSamplesCount { get; set; }

        #endregion


        #region preferences


        private ArrayList GetArchiveComparablePreferences()
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

        private ArrayList GetArchiveIncomparablePreferences()
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


        private ArrayList GetJonsPreferences()
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

        private ArrayList GetMyasPreferences()
        {
            ArrayList preferences = new ArrayList();
            preferences.Add("fuels.name ('petrol' >> OTHERS EQUAL)");
            preferences.Add("makes.name ('FISKER' >> OTHERS EQUAL)");
            preferences.Add("bodies.name ('scooter' >> OTHERS EQUAL)");
            preferences.Add("models.name ('123' >> OTHERS EQUAL)");
            return preferences;
        }

        private ArrayList GetBarrasPreferences()
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

        internal static ArrayList GetNumericPreferences()
        {
            ArrayList preferences = new ArrayList();

            //Numeric preferences
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

        internal static ArrayList GetCategoricalPreferences()
        {
            ArrayList preferences = new ArrayList();

            //Categorical preferences with a cardinality from 2 to 8 (descending)
            preferences.Add("colors.name ('red' >> 'blue' >> 'green' >> 'gold' >> 'black' >> 'gray' >> 'bordeaux' >> OTHERS EQUAL)");
            preferences.Add("bodies.name ('bus' >> 'cabriolet' >> 'limousine' >> 'coupé' >> 'van' >> 'estate car' >> OTHERS EQUAL)");
            preferences.Add("fuels.name ('petrol' >> 'diesel' >> 'bioethanol' >> 'electro' >> 'gas' >> 'hybrid' >> OTHERS EQUAL)");
            preferences.Add("makes.name ('BENTLEY' >> 'DAIMLER' >> 'FIAT'>> 'FORD'  >> OTHERS EQUAL)");
            preferences.Add("conditions.name ('new' >> 'occasion' >> 'demonstration car' >> 'oldtimer' >> OTHERS EQUAL)");
            preferences.Add("drives.name ('front wheel' >> 'all wheel' >> 'rear wheel' >> OTHERS EQUAL)");
            preferences.Add("transmissions.name ('manual' >> 'automatic' >> OTHERS EQUAL)");


            return preferences;
        }

        private ArrayList GetSpecialHexagonPreferences()
        {
            ArrayList preferences = new ArrayList();

            //Categorical preferences with a cardinality from 2 to 8 (descending)
            preferences.Add("cars.doors HIGH");
            preferences.Add("fuels.name ('petrol' >> 'diesel' >> 'bioethanol' >> 'elektro' >> 'gas' >> 'hybrid' >> OTHERS EQUAL)");
            preferences.Add("conditions.name ('new' >> 'occasion' >> 'demonstration car' >> 'oldtimer' >> OTHERS EQUAL)");
            preferences.Add("drives.name ('front wheel' >> 'all wheel' >> 'rear wheel' >> OTHERS EQUAL)");
            preferences.Add("transmissions.name ('manual' >> 'automatic' >> OTHERS EQUAL)");

            return preferences;
        }



        internal static ArrayList GetAllPreferences()
        {
            ArrayList preferences = new ArrayList();
            preferences.AddRange(GetNumericPreferences());
            preferences.AddRange(GetCategoricalPreferences());
            return preferences;
        }

        #endregion


        public void GeneratePerformanceQueries()
        {
            if (MaxDimensions < MinDimensions)
            {
                Debug.WriteLine("Max Dimensions must be >= Min Dimensions!");
                return;
            }

            //Open DBConnection --> Otherwise first query is slower as usual, because DBConnection is not open
            SQLCommon parser = new SQLCommon();
            DataTable dt = parser.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, "SELECT cars.id FROM cars SKYLINE OF cars.price LOW");

            //Use the correct line, depending on how incomparable items should be compared
            ArrayList listPreferences = new ArrayList();
            SqlConnection cnnSQL = new SqlConnection(Helper.ConnectionString); //for CLR performance tets
            ArrayList preferencesMode = new ArrayList();
            if (UseCLR)
            {
                cnnSQL.Open();
            }

            switch (Set)
            {
                case PreferenceSet.ArchiveComparable:
                    preferencesMode = GetArchiveComparablePreferences();
                    break;
                case PreferenceSet.ArchiveIncomparable:
                    preferencesMode = GetArchiveIncomparablePreferences();
                    break;
                case PreferenceSet.Jon:
                    preferencesMode = GetJonsPreferences();
                    break;
                case PreferenceSet.Mya:
                    preferencesMode = GetMyasPreferences();
                    break;
                case PreferenceSet.Barra:
                    preferencesMode = GetBarrasPreferences();
                    break;
                case PreferenceSet.All:
                    preferencesMode = GetAllPreferences();
                    break;
                case PreferenceSet.Numeric:
                    preferencesMode = GetNumericPreferences();
                    break;
                case PreferenceSet.Categoric:
                    preferencesMode = GetCategoricalPreferences();
                    break;
                case PreferenceSet.MinCardinality:
                    preferencesMode = GetSpecialHexagonPreferences();
                    break;
            }


            //Calculate correlationmatrix and cardinality from the preferences
            ArrayList correlationMatrix = GetCorrelationMatrix(preferencesMode);
            ArrayList listCardinality = GetCardinalityOfPreferences(preferencesMode);
            
            //Depending on the mode create the sets from the preferences
            if (Mode == PreferenceChooseMode.Combination)
            {
                //Tests every possible combination with y preferences from the whole set of preferences

                if (MaxDimensions > preferencesMode.Count)
                {
                    Debug.WriteLine("Combination with more dimensions than preferences. Please reduce dimensions!");
                    return;
                }

                //create all possible combinations and add it to listPreferences
                for (int i = MinDimensions; i <= MaxDimensions; i++)
                {
                    GetCombinations(preferencesMode, i, 0, new ArrayList(), ref listPreferences);    
                }
                

            }
            else if (Mode == PreferenceChooseMode.Shuffle)
            {
                //Tests x times randomly y preferences
                for (int iChoose = 0; iChoose < _randomDraws; iChoose++)
                {
                    ArrayList preferencesRandom = new ArrayList();
                    ArrayList preferencesChoose = (ArrayList)preferencesMode.Clone();

                    //First define define randomly how many dimensions
                    int differentDimensions = MaxDimensions - MinDimensions + 1;
                    int sampleDimensions = Rnd.Next(differentDimensions) + MinDimensions;
                    
                    //Choose x preferences randomly
                    for (int i = 0; i < sampleDimensions; i++)
                    {
                        int r = Rnd.Next(preferencesChoose.Count);
                        preferencesRandom.Add(preferencesChoose[r]);
                        preferencesChoose.RemoveAt(r);
                    }

                    //add random preferences to listPreferences
                    listPreferences.Add(preferencesRandom);

                }

            }
            else if (Mode == PreferenceChooseMode.Correlation)
            {
                if (MaxDimensions > 2)
                {
                    Debug.WriteLine("This test mode only works for 2 dimensions!");
                    return;
                }

                //Sort correlations to find the strongest
                correlationMatrix.Sort(new CorrelationModel());

                //Sort correlations ascending
                CorrelationModel model = (CorrelationModel)correlationMatrix[0];
                preferencesMode.Clear();
                preferencesMode.Add(model.ColA);
                preferencesMode.Add(model.ColB);
                listPreferences.Add(preferencesMode);


            }
            else if (Mode == PreferenceChooseMode.AntiCorrelation)
            {
                if (MaxDimensions > 2)
                {
                    Debug.WriteLine("This test mode only works for 2 dimensions!");
                    return;
                }

                //Sort correlations ascending
                correlationMatrix.Sort(new CorrelationModel());

                //Take only the two preferences with the worst correlation
                CorrelationModel model = (CorrelationModel)correlationMatrix[correlationMatrix.Count - 1];
                preferencesMode.Clear();
                preferencesMode.Add(model.ColA);
                preferencesMode.Add(model.ColB);
                listPreferences.Add(preferencesMode);

            }
            else if (Mode == PreferenceChooseMode.Independent)
            {
                if (MaxDimensions > 2)
                {
                    Debug.WriteLine("This test mode only works for 2 dimensions!");
                    return;
                }

                //Sort correlations to find the strongest
                correlationMatrix.Sort(new CorrelationModel());

                //Find the most independent atributes (closest to zero)
                CorrelationModel modelBefore = new CorrelationModel();
                CorrelationModel modelAfter = new CorrelationModel();
                for (int i = 0; i <= correlationMatrix.Count; i++)
                {
                    CorrelationModel model = (CorrelationModel)correlationMatrix[i];
                    if (model.Correlation > 0)
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
                preferencesMode.Clear();
                if (Math.Abs(modelBefore.Correlation) > Math.Abs(modelAfter.Correlation))
                {
                    preferencesMode.Add(modelAfter.ColA);
                    preferencesMode.Add(modelAfter.ColB);
                }
                else
                {
                    preferencesMode.Add(modelBefore.ColA);
                    preferencesMode.Add(modelBefore.ColB);
                }
                listPreferences.Add(preferencesMode);

            }


            List<SkylineStrategy> listStrategy = new List<SkylineStrategy>();
            if (Strategy == null)
            {
                //If no strategy is defined --> Take all possible algorithms
                //listStrategy.Add(new SkylineSQL());
                listStrategy.Add(new SkylineBNLSort());
                listStrategy.Add(new SkylineDQ());
                listStrategy.Add(new SkylineHexagon());
            }
            else
            {
                listStrategy.Add(Strategy);
            }
            foreach(SkylineStrategy currentStrategy in listStrategy)
            {
                //Take all strategies



                StringBuilder sb = new StringBuilder();
                string strSeparatorLine;
                if (Sampling)
                {
                    strSeparatorLine = PerformanceSampling.GetSeparatorLine();
                }
                else
                {
                     strSeparatorLine = FormatLineString('-', "", "", "", "", "", "", "", "");
                }
                
                if (GenerateScript == false)
                {
                    //Header
                    sb.AppendLine("               Algorithm: " + currentStrategy);
                    sb.AppendLine("                 Use CLR: " + UseCLR);
                    sb.AppendLine("          Preference Set: " + Set.ToString());
                    sb.AppendLine("         Preference Mode: " + Mode.ToString());
                    sb.AppendLine("                    Host: " + Environment.MachineName);
                    sb.AppendLine("      Set of Preferences: " + listPreferences.Count);
                    sb.AppendLine("                  Trials: " + Trials);
                    sb.AppendLine("              Table size: " + TableSize.ToString());
                    sb.AppendLine("          Dimension from: " + MinDimensions.ToString());
                    sb.AppendLine("            Dimension to: " + MaxDimensions.ToString());
                    //sb.AppendLine("Correlation Coefficients:" + string.Join(",", (string[])preferences.ToArray(Type.GetType("System.String"))));
                    //sb.AppendLine("           Cardinalities:" + string.Join(",", (string[])preferences.ToArray(Type.GetType("System.String"))));
                    if (Sampling)
                    {
                        sb.AppendLine("                Sampling: true");
                        sb.AppendLine("         Subspaces Count: " + SamplingSubspacesCount);
                        sb.AppendLine("      Subspace Dimension: " + SamplingSubspaceDimension);
                        sb.AppendLine("           Sampling Runs: " + SamplingSamplesCount);
                    }
                    sb.AppendLine("");
                    if (Sampling)
                    {
                        sb.AppendLine(PerformanceSampling.GetHeaderLine());
                    } else
                    {
                        sb.AppendLine(FormatLineString(' ', "preference set", "trial", "dimensions", "skyline size", "time total", "time algorithm", "sum correlation*", "product cardinality"));                        
                    }
                    sb.AppendLine(strSeparatorLine);
                    Debug.Write(sb);
                }



                List<long> reportDimensions = new List<long>();
                List<long> reportSkylineSize = new List<long>();
                List<long> reportTimeTotal = new List<long>();
                List<long> reportTimeAlgorithm = new List<long>();
                List<double> reportCorrelation = new List<double>();
                List<double> reportCardinality = new List<double>();

                var perfSampling = new PerformanceSampling(SamplingSubspacesCount, SamplingSubspaceDimension, SamplingSamplesCount);

                //For each preference set in the preference list
                for (int iPreferenceIndex = 0; iPreferenceIndex < listPreferences.Count; iPreferenceIndex++)
                {
                    ArrayList preferences = (ArrayList)listPreferences[iPreferenceIndex];
                    //Go only down two 3 dimension (because there are special algorithms for 1 and 2 dimensional skyline)
                    //for (int i = MinDimensions; i <= preferences.Count; i++)
                    //{
                    //ADD Preferences to SKYLINE
                    ArrayList subPreferences = preferences; //.GetRange(0, i);
                    string strSkylineOf = "SKYLINE OF " + string.Join(",", (string[])subPreferences.ToArray(Type.GetType("System.String")));

                    //SELECT FROM
                    string strSQL = "SELECT cars.id FROM ";
                    if (TableSize == Size.Small)
                    { 
                        strSQL += "cars_small";
                    }
                    else if (TableSize == Size.Medium)
                    {
                        strSQL += "cars_medium";
                    }
                    else if (TableSize == Size.Large)
                    {
                        strSQL += "cars_large";
                    }
                    strSQL += " cars ";
                    //Add Joins
                    strSQL += GetJoinsForPreferences(strSkylineOf);



                    //Add Skyline-Clause
                    strSQL += strSkylineOf;


                    //Convert to real SQL
                    parser = new SQLCommon();
                    parser.SkylineType = currentStrategy;
                    parser.ShowSkylineAttributes = true;


                    if (GenerateScript == false)
                    {
                        for (int iTrial = 0; iTrial < Trials; iTrial++)
                        {
                            Stopwatch sw = new Stopwatch();

                            try
                            {
                                double correlation = SearchCorrelation(subPreferences, correlationMatrix);
                                double cardinality = SearchCardinality(subPreferences, listCardinality);

                                if (Sampling)
                                {
                                    string strTrial = iTrial + 1 + " / " + _trials;
                                    string strPreferenceSet = iPreferenceIndex + 1 + " / " + listPreferences.Count;
                                    Console.WriteLine(strPreferenceSet);

                                    string strLine = perfSampling.MeasurePerformance(iTrial, iPreferenceIndex, listPreferences, preferences, parser, sw, reportDimensions, reportSkylineSize, reportTimeTotal, reportTimeAlgorithm, reportCorrelation, correlation, reportCardinality, cardinality, strSQL, strPreferenceSet, strTrial);

                                    Debug.WriteLine(strLine);
                                    sb.AppendLine(strLine);
                                }
                                else
                                {
                                    sw.Start();
                                    if (UseCLR)
                                    {
                                        string strSP = parser.ParsePreferenceSQL(strSQL);
                                        var dap = new SqlDataAdapter(strSP, cnnSQL);
                                        dt.Clear(); //clear datatable
                                        dap.Fill(dt);
                                    }
                                    else
                                    {
                                        parser.Cardinality = (long) cardinality;
                                        dt = parser.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName,
                                            strSQL);
                                    }
                                    long timeAlgorithm = parser.TimeInMilliseconds;
                                    long numberOfOperations = parser.NumberOfOperations;
                                    sw.Stop();

                                    reportDimensions.Add(preferences.Count);
                                    reportSkylineSize.Add(dt.Rows.Count);
                                    reportTimeTotal.Add(sw.ElapsedMilliseconds);
                                    reportTimeAlgorithm.Add(timeAlgorithm);
                                    reportCorrelation.Add(correlation);
                                    reportCardinality.Add(cardinality);

                                    //trial|dimensions|skyline size|time total|time algorithm
                                    string strTrial = iTrial + 1 + " / " + _trials;
                                    string strPreferenceSet = iPreferenceIndex + 1 + " / " + listPreferences.Count;
                                    Console.WriteLine(strPreferenceSet);

                                    string strLine = FormatLineString(strPreferenceSet, strTrial, preferences.Count,
                                        dt.Rows.Count, sw.ElapsedMilliseconds, timeAlgorithm, correlation, cardinality);
                                    
                                    Debug.WriteLine(strLine);
                                    sb.AppendLine(strLine);
                                }
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


                        strSQL = parser.ParsePreferenceSQL(strSQL);

                        string[] sizes = { "small", "medium", "large", "superlarge" };

                        //Format for each of the customer profiles
                        sb.AppendLine("PRINT '----- -------------------------------------------------------- ------'");
                        sb.AppendLine("PRINT '----- " + (preferences.Count + 1) + " dimensions  ------'");
                        sb.AppendLine("PRINT '----- -------------------------------------------------------- ------'");
                        foreach (string size in sizes)
                        {
                            sb.AppendLine("GO"); //we need this in order the profiler shows each query in a new line
                            sb.AppendLine(strSQL.Replace("cars", "cars_" + size));

                        }

                            
                    }

                    //}

                }

                ////////////////////////////////
                //Summary
                ///////////////////////////////
                if (GenerateScript == false)
                {
                    if (Sampling)
                    {
                        perfSampling.AddSummary(sb, strSeparatorLine, reportDimensions, reportSkylineSize, reportTimeTotal, reportTimeAlgorithm, reportCorrelation, reportCardinality);
                        perfSampling.AddPreferenceSetInformation(sb, listPreferences, strSeparatorLine);
                    }
                    else
                    {
                        AddSummary(sb, strSeparatorLine, reportDimensions, reportSkylineSize, reportTimeTotal, reportTimeAlgorithm, reportCorrelation, reportCardinality);
                    }
                }

                //Write some empty lines (clarification in output window)
                Debug.WriteLine("");
                Debug.WriteLine("");
                Debug.WriteLine("");


                //Write in file
                string strFiletype;

                if (GenerateScript == false)
                {
                    strFiletype = ".csv";
                }
                else
                {
                    strFiletype = ".sql";
                }
                //create filename
                string strFileName = Path + "Performance_" + Set.ToString() + "_" + currentStrategy + strFiletype;

                StreamWriter outfile = new StreamWriter(strFileName);
                outfile.Write(sb.ToString());
                outfile.Close();
            }

            //close connection
            if (UseCLR)
            {
                cnnSQL.Close();
            }
            
        }

        private DataTable GetSQLFromPreferences(ArrayList preferences, bool cardinality)
        {
            SQLCommon common = new SQLCommon();
            string strPrefSQL = "SELECT cars.id FROM ";
            if (TableSize == Size.Small)
            {
                strPrefSQL += "cars_small";
            }
            else if (TableSize == Size.Medium)
            {
                strPrefSQL += "cars_medium";
            }
            else if (TableSize == Size.Large)
            {
                strPrefSQL += "cars_large";
            }
            strPrefSQL += " cars ";
            strPrefSQL += "SKYLINE OF ";


            for (int i = 0; i < preferences.Count; i++)
            {
                strPrefSQL += preferences[i] + ",";
            }
            strPrefSQL = strPrefSQL.TrimEnd(',');

            PrefSQLModel prefModel = common.GetPrefSqlModelFromPreferenceSql(strPrefSQL);

            string strSQL = "SELECT ";

            for (int i = 0; i < prefModel.Skyline.Count; i++)
            {
                if (cardinality)
                {
                    strSQL += "COUNT(DISTINCT " + prefModel.Skyline[i].Expression + "),";
                }
                else
                {
                    strSQL += prefModel.Skyline[i].Expression + ",";
                }
            }
            strSQL = strSQL.TrimEnd(',') + " FROM cars ";
            strSQL += GetJoinsForPreferences(strSQL);





            DataTable dt = Helper.ExecuteStatement(strSQL);

            return dt;
        }

        private string GetJoinsForPreferences(string strSkylineOf)
        {
            string strSQL = "";
            if (strSkylineOf.IndexOf("colors", StringComparison.Ordinal) > 0)
            {
                strSQL += "LEFT OUTER JOIN colors ON cars.color_id = colors.ID ";
            }
            if (strSkylineOf.IndexOf("fuels", StringComparison.Ordinal) > 0)
            {
                strSQL += "LEFT OUTER JOIN fuels ON cars.fuel_id = fuels.ID ";
            }
            if (strSkylineOf.IndexOf("bodies", StringComparison.Ordinal) > 0)
            {
                strSQL += "LEFT OUTER JOIN bodies ON cars.body_id = bodies.ID ";
            }
            if (strSkylineOf.IndexOf("makes", StringComparison.Ordinal) > 0)
            {
                strSQL += "LEFT OUTER JOIN makes ON cars.make_id = makes.ID ";
            }
            if (strSkylineOf.IndexOf("conditions", StringComparison.Ordinal) > 0)
            {
                strSQL += "LEFT OUTER JOIN conditions ON cars.condition_id = conditions.ID ";
            }
            if (strSkylineOf.IndexOf("models", StringComparison.Ordinal) > 0)
            {
                strSQL += "LEFT OUTER JOIN models ON cars.model_id = models.ID ";
            }
            if (strSkylineOf.IndexOf("transmissions", StringComparison.Ordinal) > 0)
            {
                strSQL += "LEFT OUTER JOIN transmissions ON cars.transmission_id = transmissions.ID ";
            }
            if (strSkylineOf.IndexOf("drives", StringComparison.Ordinal) > 0)
            {
                strSQL += "LEFT OUTER JOIN drives ON cars.drive_id = drives.ID ";
            }


            return strSQL;
        }

        private ArrayList GetCorrelationMatrix(ArrayList preferences)
        {
            DataTable dt = GetSQLFromPreferences(preferences, false);
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

                        double correlation = MyMathematic.GetPearson(colA, colB);
                        CorrelationModel model = new CorrelationModel(preferences[iIndex].ToString(), preferences[iPref].ToString(), correlation);
                        listCorrelation.Add(model);
                    }


                }
            }


            return listCorrelation;
        }



        private ArrayList GetCardinalityOfPreferences(ArrayList preferences)
        {
            DataTable dt = GetSQLFromPreferences(preferences, true);

            //Calculate correlation between the attributes
            ArrayList listCardinality = new ArrayList();

            for (int iIndex = 0; iIndex < preferences.Count; iIndex++)
            {
                CardinalityModel model = new CardinalityModel(preferences[iIndex].ToString(), (int)dt.Rows[0][iIndex]);
                listCardinality.Add(model);
            }

            return listCardinality;
        }

        private double SearchCorrelation(ArrayList preferences, ArrayList correlationMatrix)
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



        private double SearchCardinality(ArrayList preferences, ArrayList cardinality)
        {

            double product = 1;
            for (int i = 0; i < preferences.Count; i++)
            {
                bool bFound = false;
                for (int iModel = 0; iModel < cardinality.Count; iModel++)
                {
                    CardinalityModel model = (CardinalityModel)cardinality[iModel];
                    if (model.Col.Equals(preferences[i].ToString()))
                    {
                        product *= model.Cardinality;
                        bFound = true;
                        break;
                    }
                }
                if (bFound == false)
                {
                    throw new Exception("cardinality factor not found");
                }
            }
            return product;
        }


        /// <summary>
        /// Create all possible combinations from x preferences
        /// </summary>
        /// <param name="arr"></param>
        /// <param name="len"></param>
        /// <param name="startPosition"></param>
        /// <param name="result"></param>
        /// <param name="returnArray"></param>
        private void GetCombinations(ArrayList arr, int len, int startPosition, ArrayList result, ref ArrayList returnArray)
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
                returnArray.Add(result.Clone());
                return;
            }
            for (int i = startPosition; i <= arr.Count - len; i++)
            {
                result[result.Count - len] = (string)arr[i];
                GetCombinations(arr, len - 1, i + 1, result, ref returnArray);
            }
        }

        


        #region formatOutput

    

        private void AddSummary(StringBuilder sb, String strSeparatorLine, List<long> reportDimensions, List<long> reportSkylineSize, List<long> reportTimeTotal, List<long> reportTimeAlgorithm, List<double> reportCorrelation, List<double> reportCardinality)
        {
            //Separator Line
            Debug.WriteLine(strSeparatorLine);
            sb.AppendLine(strSeparatorLine);

            string strAverage = FormatLineString("average", "", reportDimensions.Average(), reportSkylineSize.Average(), reportTimeTotal.Average(), reportTimeAlgorithm.Average(), reportCorrelation.Average(), reportCardinality.Average());
            string strMin = FormatLineString("minimum", "", reportDimensions.Min(), reportSkylineSize.Min(), reportTimeTotal.Min(), reportTimeAlgorithm.Min(), reportCorrelation.Min(), reportCardinality.Min());
            string strMax = FormatLineString("maximum", "", reportDimensions.Max(), reportSkylineSize.Max(), reportTimeTotal.Max(), reportTimeAlgorithm.Max(), reportCorrelation.Max(), reportCardinality.Max());
            string strVar = FormatLineString("variance", "", MyMathematic.GetVariance(reportDimensions), MyMathematic.GetVariance(reportSkylineSize), MyMathematic.GetVariance(reportTimeTotal), MyMathematic.GetVariance(reportTimeAlgorithm), MyMathematic.GetVariance(reportCorrelation), MyMathematic.GetVariance(reportCardinality));
            string strStd = FormatLineString("stddeviation", "", MyMathematic.GetStdDeviation(reportDimensions), MyMathematic.GetStdDeviation(reportSkylineSize), MyMathematic.GetStdDeviation(reportTimeTotal), MyMathematic.GetStdDeviation(reportTimeAlgorithm), MyMathematic.GetStdDeviation(reportCorrelation), MyMathematic.GetStdDeviation(reportCardinality));
            string strSamplevar = FormatLineString("sample variance", "", MyMathematic.GetSampleVariance(reportDimensions), MyMathematic.GetSampleVariance(reportSkylineSize), MyMathematic.GetSampleVariance(reportTimeTotal), MyMathematic.GetSampleVariance(reportTimeAlgorithm), MyMathematic.GetSampleVariance(reportCorrelation), MyMathematic.GetSampleVariance(reportCardinality));
            string strSampleStd = FormatLineString("sample stddeviation", "", MyMathematic.GetSampleStdDeviation(reportDimensions), MyMathematic.GetSampleStdDeviation(reportSkylineSize), MyMathematic.GetSampleStdDeviation(reportTimeTotal), MyMathematic.GetSampleStdDeviation(reportTimeAlgorithm), MyMathematic.GetSampleStdDeviation(reportCorrelation), MyMathematic.GetSampleStdDeviation(reportCardinality));

            sb.AppendLine(strAverage);
            sb.AppendLine(strMin);
            sb.AppendLine(strMax);
            sb.AppendLine(strVar);
            sb.AppendLine(strStd);
            sb.AppendLine(strSamplevar);
            sb.AppendLine(strSampleStd);
            Debug.WriteLine(strAverage);
            Debug.WriteLine(strMin);
            Debug.WriteLine(strMax);
            Debug.WriteLine(strVar);
            Debug.WriteLine(strStd);
            Debug.WriteLine(strSamplevar);
            Debug.WriteLine(strSampleStd);

            //Separator Line
            sb.AppendLine(strSeparatorLine);
            Debug.WriteLine(strSeparatorLine);
        }



        private string FormatLineString(char paddingChar, string strTitle, string strTrial, string strDimension, string strSkyline, string strTimeTotal, string strTimeAlgo, string strCorrelation, string strCardinality)
        {
            //average line
            //trial|dimensions|skyline size|time total|time algorithm|correlation|
            string[] line = new string[9];
            line[0] = strTitle.PadLeft(19, paddingChar);
            line[1] = strTrial.PadLeft(11, paddingChar);
            line[2] = strDimension.PadLeft(10, paddingChar);
            line[3] = strSkyline.PadLeft(20, paddingChar);
            line[4] = strTimeTotal.PadLeft(20, paddingChar);
            line[5] = strTimeAlgo.PadLeft(20, paddingChar);
            line[6] = strCorrelation.PadLeft(20, paddingChar);
            line[7] = strCardinality.PadLeft(25, paddingChar);
            line[8] = "";
            return string.Join("|", line);
        }

    
        private string FormatLineString(string strTitle, string strTrial, double dimension, double skyline, double timeTotal, double timeAlgo, double correlation, double cardinality)
        {
            return FormatLineString(' ', strTitle, strTrial, Math.Round(dimension, 2).ToString(CultureInfo.InvariantCulture), Math.Round(skyline, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeTotal, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeAlgo, 2).ToString(CultureInfo.InvariantCulture), Math.Round(correlation, 2).ToString(CultureInfo.InvariantCulture), ToLongString(Math.Round(cardinality, 2)));
        }  
        
        /// <summary>
        /// Source: http://stackoverflow.com/questions/1546113/double-to-string-conversion-without-scientific-notation
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        internal static string ToLongString(double input)
        {
            string str = input.ToString(CultureInfo.InvariantCulture).ToUpper();

            // if string representation was collapsed from scientific notation, just return it:
            if (!str.Contains("E")) return str;

            bool negativeNumber = false;

            if (str[0] == '-')
            {
                str = str.Remove(0, 1);
                negativeNumber = true;
            }

            string sep = Thread.CurrentThread.CurrentCulture.NumberFormat.NumberDecimalSeparator;
            char decSeparator = sep.ToCharArray()[0];

            string[] exponentParts = str.Split('E');
            string[] decimalParts = exponentParts[0].Split(decSeparator);

            // fix missing decimal point:
            if (decimalParts.Length == 1) decimalParts = new[] { exponentParts[0], "0" };

            int exponentValue = int.Parse(exponentParts[1]);

            string newNumber = decimalParts[0] + decimalParts[1];

            string result;

            if (exponentValue > 0)
            {
                result =
                    newNumber +
                    GetZeros(exponentValue - decimalParts[1].Length);
            }
            else // negative exponent
            {
                result =
                    "0" +
                    decSeparator +
                    GetZeros(exponentValue + decimalParts[0].Length) +
                    newNumber;

                result = result.TrimEnd('0');
            }

            if (negativeNumber)
                result = "-" + result;

            return result;
        }

        /// <summary>
        /// Source: http://stackoverflow.com/questions/1546113/double-to-string-conversion-without-scientific-notation
        /// </summary>
        /// <param name="zeroCount"></param>
        /// <returns></returns>
        private static string GetZeros(int zeroCount)
        {
            if (zeroCount < 0)
                zeroCount = Math.Abs(zeroCount);

            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < zeroCount; i++) sb.Append("0");

            return sb.ToString();
        }



        #endregion




        

    }
    

}
