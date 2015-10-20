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
        private string Path = System.IO.Path.GetTempPath(); // @"C:\Users\Public\Documents\workspace\prefcom\prefSQL\root\PerformanceTests\";
        private int _trials = 5;                 //How many times each preferene query is executed  
        private int _randomDraws = 25;          //Only used for the shuffle set. How many random set will be generated
        private bool _excessiveTests = true;
        static readonly Random Rnd = new Random();
        static readonly Mathematic MyMathematic = new Mathematic();

        public Performance()
        {
            UseCLR = false;
            UseNormalizedValues = false;
        }

        public int MinDimensions { get; set; }  //Up from this amount of dimension should be tested
        public int MaxDimensions { get; set; }  //Up to this amount of dimension should be tested

        public int WindowHandling { get; set; }

        public SQLCommon.Ordering WindowSort { get; set; }

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
            NumericIncomparable,    //Take only numeric preferences with incomparable levels
            Categoric,              //Take only categoric preferences
            CategoricIncomparable,  //Take only categoric preferences that contain incomparable tuples
            MinCardinality,         //Special collection of preferences which should perform well on Hexagon

            LowCardinality,
            HighCardinality,
            LowAndHighCardinality,
            ForRandom10,
            ForRandom17
        };

        public enum PreferenceChooseMode
        {
            Combination,            //Test every possible combination of the preferences
            SameOrder,              //Test in the given order
            Shuffle,                //Choose x randomly preferences from all possible combinations
            Correlation,            //Take 2 best correlated preferences
            AntiCorrelation,        //Take 2 worst correlated preferences
            Independent,            //Take 2 most independent correlated preferences
        }


        #region getter/setters

        public bool UseCLR { get; set; }

        public bool UseNormalizedValues { get; set; }

        public int SkylineUpToLevel { get; set; }

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

        internal int SamplingSubsetsCount { get; set; }

        internal int SamplingSubsetDimension { get; set; }

        internal int SamplingSamplesCount { get; set; }

        public bool ExcessiveTests
        {
            get { return _excessiveTests; }
            set { _excessiveTests = value; }
        }

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
            preferences.Add("fuels.name ('petrol' >> OTHERS EQUAL >> 'diesel')");
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
            preferences.Add("fuels.name ('petrol' >> OTHERS INCOMPARABLE >> 'diesel')");
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
            preferences.Add("cars.registrationnumeric HIGH");
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


        internal static ArrayList GetNumericIncomparablePreferences()
        {
            ArrayList preferences = new ArrayList();

            //Numeric preferences

            preferences.Add("colors.name ('red' >> 'blue' >> 'green' >> 'gold' >> 'black' >> 'gray' >> 'bordeaux' >> OTHERS INCOMPARABLE)");
            preferences.Add("cars.price LOW");
            preferences.Add("cars.mileage LOW");
            preferences.Add("cars.horsepower HIGH");
            preferences.Add("cars.enginesize HIGH");
            preferences.Add("cars.consumption LOW");
            preferences.Add("cars.doors HIGH");
            preferences.Add("cars.seats HIGH");
            preferences.Add("cars.cylinders HIGH");
            preferences.Add("cars.gears HIGH");
            //preferences.Add("cars.registrationNumeric HIGH");

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

        internal static ArrayList GetLowCardinalityPreferences()
        {
            ArrayList preferences = new ArrayList();

            preferences.Add("transmissions.name ('manual' >> 'automatic' >> OTHERS EQUAL)"); // cardinality 2 in superlarge
            preferences.Add("drives.name ('front wheel' >> 'all wheel' >> 'rear wheel' >> OTHERS EQUAL)"); // cardinality 3 in superlarge
            preferences.Add("conditions.name ('new' >> 'occasion' >> 'demonstration car' >> 'oldtimer' >> OTHERS EQUAL)"); // cardinality 4 in superlarge
            preferences.Add("cars.doors HIGH"); // cardinality 5 in superlarge
            preferences.Add("fuels.name ('petrol' >> 'diesel' >> 'bioethanol' >> 'electro' >> 'gas' >> 'hybrid' >> OTHERS EQUAL)"); // cardinality 6 in superlarge
            preferences.Add("cars.gears HIGH"); // cardinality 7 in superlarge
            preferences.Add("cars.cylinders HIGH"); // cardinality 8 in superlarge
            preferences.Add("cars.seats HIGH"); // cardinality 10 in superlarge
            preferences.Add("cars.Body_Id HIGH"); // cardinality 11 in superlarge
            preferences.Add("cars.Color_Id HIGH"); // cardinality 17 in superlarge

            return preferences;
        }

        internal static ArrayList GetHighCardinalityPreferences()
        {
            ArrayList preferences = new ArrayList();

            preferences.Add("cars.mileage LOW"); // cardinality 6515 in superlarge
            preferences.Add("cars.price LOW"); // cardinality 5988 in superlarge
            preferences.Add("cars.Model_Id HIGH"); // cardinality 813 in superlarge
            preferences.Add("cars.enginesize HIGH"); // cardinality 507 in superlarge
            preferences.Add("cars.horsepower HIGH"); // cardinality 397 in superlarge
            preferences.Add("cars.registrationNumeric HIGH"); // cardinality 270 in superlarge
            preferences.Add("cars.consumption LOW"); // cardinality 181 in superlarge
            preferences.Add("cars.Make_Id HIGH"); // cardinality 71 in superlarge

            return preferences;
        }

        internal static ArrayList Get10ForRandomPreferences()
        {
            ArrayList preferences = new ArrayList();
            preferences.AddRange(GetNumericPreferences());
            return preferences;
        }

        internal static ArrayList Get17ForRandomPreferences()
        {
            ArrayList preferences = new ArrayList();

            preferences.AddRange(Get10ForRandomPreferences());
            preferences.Add("cars.Body_Id HIGH");
            preferences.Add("cars.Color_Id LOW");
            preferences.Add("cars.Make_Id LOW");
            preferences.Add("cars.Model_Id HIGH");
            preferences.Add("cars.Drive_Id HIGH");
            preferences.Add("cars.Transmission_Id HIGH");
            preferences.Add("cars.Fuel_Id LOW");

            return preferences;
        }

        internal static ArrayList GetLowAndHighCardinalityPreferences()
        {
            ArrayList preferences = new ArrayList();
            preferences.AddRange(GetHighCardinalityPreferences());
            preferences.AddRange(GetLowCardinalityPreferences());
            return preferences;
        }

        private ArrayList GetSpecialHexagonPreferences()
        {
            ArrayList preferences = new ArrayList();

            //Categorical preferences with a cardinality from 2 to 8 (descending)
            preferences.Add("colors.name ('red' >> OTHERS INCOMPARABLE >> 'blue' >> 'green' >> 'gold' >> 'black' >> 'gray' >> 'bordeaux')");
            preferences.Add("bodies.name ('bus' >> OTHERS EQUAL >> 'cabriolet' >> 'limousine' >> 'coupé' >> 'van' >> 'estate car')");
            preferences.Add("fuels.name ('petrol' >> OTHERS EQUAL >> 'diesel' >> 'electro' >> 'gas' >> 'hybrid')");
            preferences.Add("makes.name ('BENTLEY' >> OTHERS EQUAL >> 'DAIMLER' >> 'FIAT'>> 'FORD')");
            preferences.Add("conditions.name ('new' >> OTHERS EQUAL >> 'occasion' >> 'oldtimer')");
            preferences.Add("drives.name ('front wheel' >> OTHERS EQUAL >> 'all wheel')");
            preferences.Add("transmissions.name ('manual' >> OTHERS EQUAL)");

            return preferences;
        }

        private ArrayList GetMinimalCardinalityPreferences()
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
                case PreferenceSet.NumericIncomparable:
                    preferencesMode = GetNumericIncomparablePreferences();
                    break;
                case PreferenceSet.Categoric:
                    preferencesMode = GetCategoricalPreferences();
                    break;
                case PreferenceSet.CategoricIncomparable:
                    preferencesMode = GetCategoricalIncomparablePreferences();
                    break;
                case PreferenceSet.MinCardinality:
                    preferencesMode = GetMinimalCardinalityPreferences();
                    break;
                case PreferenceSet.LowCardinality:
                    preferencesMode = GetLowCardinalityPreferences();
                    break;
                case PreferenceSet.HighCardinality:
                    preferencesMode = GetHighCardinalityPreferences();
                    break;
                case PreferenceSet.LowAndHighCardinality:
                    preferencesMode = GetLowAndHighCardinalityPreferences();
                    break;
                case PreferenceSet.ForRandom10:
                    preferencesMode = Get10ForRandomPreferences();
                    break;
                case PreferenceSet.ForRandom17:
                    preferencesMode = Get17ForRandomPreferences();
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
            else if (Mode == PreferenceChooseMode.SameOrder)
            {
                int draws = MaxDimensions - MinDimensions + 1;
                //Tests x times randomly y preferences
                for (int iChoose = 0; iChoose < draws; iChoose++)
                {
                    ArrayList preferencesRandom = new ArrayList();
                    ArrayList preferencesChoose = (ArrayList)preferencesMode.Clone();

                    //First define define randomly how many dimensions
                    int sampleDimensions = iChoose + MinDimensions;

                    //Choose x preferences randomly
                    for (int i = 0; i < sampleDimensions; i++)
                    {
                        preferencesRandom.Add(preferencesChoose[i]);
                    }

                    //add random preferences to listPreferences
                    listPreferences.Add(preferencesRandom);

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
                listStrategy.Add(new SkylineDecisionTree());
            }
            else
            {
                listStrategy.Add(Strategy);
            }
            foreach (SkylineStrategy currentStrategy in listStrategy)
            {
                //Take all strategies



                StringBuilder sb = new StringBuilder();
                string strSeparatorLine;
                if (Sampling)
                {
                    strSeparatorLine = PerformanceSampling.GetSeparatorLine(ExcessiveTests);
                }
                else
                {
                     strSeparatorLine = FormatLineString('-', "", "", "", "", "", "", "", "", "", "", "");
                }

                if (GenerateScript == false)
                {
                    //Header
                    sb.AppendLine("                    Path: " + Path);
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
                    sb.AppendLine("        BNL Initial Sort: " + WindowSort.ToString());
                    sb.AppendLine("     BNL Window Handling: " + WindowHandling.ToString());
                    //sb.AppendLine("Correlation Coefficients:" + string.Join(",", (string[])preferences.ToArray(Type.GetType("System.String"))));
                    //sb.AppendLine("           Cardinalities:" + string.Join(",", (string[])preferences.ToArray(Type.GetType("System.String"))));
                    if (Sampling)
                    {
                        sb.AppendLine("                Sampling: true");
                        sb.AppendLine("           Subsets Count: " + SamplingSubsetsCount);
                        sb.AppendLine("        Subset Dimension: " + SamplingSubsetDimension);
                        sb.AppendLine("           Sampling Runs: " + SamplingSamplesCount);
                    }
                    sb.AppendLine("");
                    if (Sampling)
                    {
                        sb.AppendLine(PerformanceSampling.GetHeaderLine());
                    }
                    else
                    {
                        sb.AppendLine(FormatLineString(' ', "preference set", "trial", "dimensions", "skyline size", "time total", "time algorithm", "min correlation", "max correlation", "product cardinality", "number of moves", "number of comparisons"));                        
                    }
                    sb.AppendLine(strSeparatorLine);
                    Debug.Write(sb);
                }



                List<long> reportDimensions = new List<long>();
                List<long> reportSkylineSize = new List<long>();
                List<long> reportTimeTotal = new List<long>();
                List<long> reportTimeAlgorithm = new List<long>();
                List<long> reportNumberOfMoves = new List<long>();
                List<long> reportNumberOfComparisons = new List<long>();
                List<double> reportMinCorrelation = new List<double>();
                List<double> reportMaxCorrelation = new List<double>();
                List<double> reportCardinality = new List<double>();

                var perfSampling = new PerformanceSampling(SamplingSubsetsCount, SamplingSubsetDimension, SamplingSamplesCount, ExcessiveTests);

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
                    if (!UseNormalizedValues)
                    {
                        strSQL += " cars ";
                    }
                    else
                    {
                        strSQL += "cars_normalized cars ";
                    }




                    //Add Joins
                    strSQL += GetJoinsForPreferences(strSkylineOf);



                    //Add Skyline-Clause
                    strSQL += strSkylineOf;


                    //Convert to real SQL
                    parser = new SQLCommon();
                    parser.SkylineType = currentStrategy;
                    parser.ShowSkylineAttributes = true;
                    parser.WindowHandling = WindowHandling;
                    parser.WindowSort = WindowSort;


                    if (GenerateScript == false)
                    {
                        for (int iTrial = 0; iTrial < Trials; iTrial++)
                        {
                            Stopwatch sw = new Stopwatch();

                            try
                            {
                                double minCorrelation = 0;
                                double maxCorrelation = 0;


                                SearchCorrelation(subPreferences, correlationMatrix, ref minCorrelation, ref maxCorrelation);
                                double cardinality = SearchCardinality(subPreferences, listCardinality);
                                long timeAlgorithm = 0;
                                long skylineSize = 0;
                                long numberOfMoves = 0;
                                long numberOfComparisons = 0;

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

                                    ArrayList clauseID = new ArrayList();
                                    String strIDs = "";
                                    for (int skylineLevel = 1; skylineLevel <= SkylineUpToLevel; skylineLevel++)
                                    {
                                        if (UseCLR)
                                        {
                                            string strSP = parser.ParsePreferenceSQL(strSQL);
                                            SqlDataAdapter dap = new SqlDataAdapter(strSP, cnnSQL);
                                            dt.Clear(); //clear datatable
                                            dap.Fill(dt);
                                        }
                                        else
                                        {
                                            parser.Cardinality = (long)cardinality;

                                            string strSQLWithWHERE = strSQL;
                                            strSQLWithWHERE = strSQL.Substring(0, strSQL.IndexOf("SKYLINE OF"));
                                            strSQLWithWHERE += strIDs;
                                            strSQLWithWHERE += strSQL.Substring(strSQL.IndexOf("SKYLINE OF"));
                                            dt = parser.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, strSQLWithWHERE);

                                        }

                                        timeAlgorithm += parser.TimeInMilliseconds;
                                        numberOfMoves += parser.NumberOfMoves;
                                        numberOfComparisons += parser.NumberOfComparisons;

                                        skylineSize += dt.Rows.Count;

                                        //only if more queries are requested

                                        if (skylineLevel < SkylineUpToLevel && currentStrategy.GetType() != typeof(prefSQL.SQLSkyline.MultipleSkylineBNL))
                                        {
                                            //Add ids to WHERE clause
                                            foreach (DataRow row in dt.Rows)
                                            {
                                                clauseID.Add((int)row[0]);
                                            }



                                            //Add WHERE clause with IDs that were already in the skyline
                                            strIDs = "";
                                            foreach (int id in clauseID)
                                            {
                                                strIDs += id + ",";
                                            }
                                            if (strIDs.Length > 0)
                                            {
                                                strIDs = "WHERE cars.id NOT IN (" + strIDs.TrimEnd(',') + ") ";
                                            }


                                        }
                                        else
                                        {
                                            skylineLevel = SkylineUpToLevel;
                                        }

                                    }

                                    
                                    
                                    sw.Stop();

                                    reportDimensions.Add(preferences.Count);
                                    reportSkylineSize.Add(skylineSize);
                                    reportTimeTotal.Add(sw.ElapsedMilliseconds);
                                    reportTimeAlgorithm.Add(timeAlgorithm);
                                    reportNumberOfMoves.Add(numberOfMoves);
                                    reportNumberOfComparisons.Add(numberOfComparisons);
                                    reportMinCorrelation.Add(minCorrelation);
                                    reportMaxCorrelation.Add(maxCorrelation);
                                    reportCardinality.Add(cardinality);

                                    //trial|dimensions|skyline size|time total|time algorithm
                                    string strTrial = iTrial + 1 + " / " + _trials;
                                    string strPreferenceSet = iPreferenceIndex + 1 + " / " + listPreferences.Count;
                                    Console.WriteLine(strPreferenceSet);

                                    string strLine = "";

                                    //Was there an error?
                                    if (dt.Rows.Count == 0)
                                    {
                                        strLine = FormatLineString("Error! " + strPreferenceSet, strTrial, preferences.Count, dt.Rows.Count, sw.ElapsedMilliseconds, timeAlgorithm, minCorrelation, maxCorrelation, cardinality, numberOfMoves, numberOfComparisons);    
                                    }
                                    else
                                    {
                                        strLine = FormatLineString(strPreferenceSet, strTrial, preferences.Count, dt.Rows.Count, sw.ElapsedMilliseconds, timeAlgorithm, minCorrelation, maxCorrelation, cardinality, numberOfMoves, numberOfComparisons);    
                                    }
                                    
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
                        AddSummary(sb, strSeparatorLine, reportDimensions, reportSkylineSize, reportTimeTotal, reportTimeAlgorithm, reportMinCorrelation, reportMaxCorrelation, reportCardinality, reportNumberOfMoves, reportNumberOfComparisons);
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
                    strFiletype = ".txt";
                }
                else
                {
                    strFiletype = ".sql";
                }
                //create filename
                string strFileName = Path + Set.ToString() + "_" + Mode.ToString() + "_" + MinDimensions + "_" + MaxDimensions + "_" + currentStrategy + strFiletype;

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

        private double SearchCorrelation(ArrayList preferences, ArrayList correlationMatrix, ref double minCorrelation, ref double maxCorrelation)
        {

            //Define impossible correlations for min and max
            minCorrelation = 1.01;
            maxCorrelation = -1.01;

            for (int i = 0; i < preferences.Count; i++)
            {
                for (int ii = i + 1; ii < preferences.Count; ii++)
                {
                    bool bFound = false;
                    for (int iModel = 0; iModel < correlationMatrix.Count; iModel++)
                    {
                        CorrelationModel model = (CorrelationModel)correlationMatrix[iModel];
                        if (model.ColA.Equals(preferences[i].ToString()) && model.ColB.Equals(preferences[ii].ToString()))
                        {
                            if (minCorrelation > model.Correlation)
                            {
                                minCorrelation = model.Correlation;
                            }
                            if (maxCorrelation < model.Correlation)
                            {
                                maxCorrelation = model.Correlation;
                            }
                            bFound = true;
                            break;
                        }
                        else if (model.ColB.Equals(preferences[i].ToString()) && model.ColA.Equals(preferences[ii].ToString()))
                        {
                            if (minCorrelation > model.Correlation)
                            {
                                minCorrelation = model.Correlation;
                            }
                            if (maxCorrelation < model.Correlation)
                            {
                                maxCorrelation = model.Correlation;
                            }
                            bFound = true;
                            break;
                        }
                    }
                    if (bFound == false)
                    {
                        throw new Exception("correlation factor not found");
                    }
                }
            }

            return minCorrelation;
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
            if (result.Count == 0)
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

        private void AddSummarySample(StringBuilder sb, String strSeparatorLine, List<long> reportDimensions,
            List<long> reportSkylineSize, List<long> reportTimeTotal, List<long> reportTimeAlgorithm, IDictionary<ReportsSampling, List<long>> rsl, IDictionary<ReportsSampling, List<double>> rsd, Dictionary<SkylineTypesSampling, List<double>> setCoverageSampling, Dictionary<SkylineTypesSampling, List<double>> representationErrorSampling, Dictionary<SkylineTypesSampling, List<double>> representationErrorSumSampling, Dictionary<SkylineTypesSampling, List<double>> dominatedObjectsCountSampling, Dictionary<SkylineTypesSampling, List<double>> dominatedObjectsOfBestObjectSampling, List<double> reportMinCorrelation, List<double> reportMaxCorrelation, List<double> reportCardinality)
        {
            //Separator Line
            Debug.WriteLine(strSeparatorLine);
            sb.AppendLine(strSeparatorLine);


             string[] setCoverageSamplingMin = GetSummaryMin(setCoverageSampling);
             string[] representationErrorSamplingMin = GetSummaryMin(representationErrorSampling);
             string[] representationErrorSumSamplingMin = GetSummaryMin(representationErrorSumSampling);
             string[] dominatedObjectsCountSamplingMin = GetSummaryMin(dominatedObjectsCountSampling);
             string[] dominatedObjectsOfBestObjectSamplingMin = GetSummaryMin(dominatedObjectsOfBestObjectSampling);

              string[] setCoverageSamplingMax = GetSummaryMax(setCoverageSampling);
             string[] representationErrorSamplingMax = GetSummaryMax(representationErrorSampling);
             string[] representationErrorSumSamplingMax = GetSummaryMax(representationErrorSumSampling);
             string[] dominatedObjectsCountSamplingMax = GetSummaryMax(dominatedObjectsCountSampling);
             string[] dominatedObjectsOfBestObjectSamplingMax = GetSummaryMax(dominatedObjectsOfBestObjectSampling);

             string[] setCoverageSamplingVariance = GetSummaryVariance(setCoverageSampling);
             string[] representationErrorSamplingVariance = GetSummaryVariance(representationErrorSampling);
             string[] representationErrorSumSamplingVariance = GetSummaryVariance(representationErrorSumSampling);
             string[] dominatedObjectsCountSamplingVariance = GetSummaryVariance(dominatedObjectsCountSampling);
             string[] dominatedObjectsOfBestObjectSamplingVariance = GetSummaryVariance(dominatedObjectsOfBestObjectSampling);

             string[] setCoverageSamplingStdDev = GetSummaryStdDev(setCoverageSampling);
             string[] representationErrorSamplingStdDev = GetSummaryStdDev(representationErrorSampling);
             string[] representationErrorSumSamplingStdDev = GetSummaryStdDev(representationErrorSumSampling);
             string[] dominatedObjectsCountSamplingStdDev = GetSummaryStdDev(dominatedObjectsCountSampling);
             string[] dominatedObjectsOfBestObjectSamplingStdDev = GetSummaryStdDev(dominatedObjectsOfBestObjectSampling);

             string[] setCoverageSamplingMedian = GetSummaryMedian(setCoverageSampling);
             string[] representationErrorSamplingMedian = GetSummaryMedian(representationErrorSampling);
             string[] representationErrorSumSamplingMedian = GetSummaryMedian(representationErrorSumSampling);
             string[] dominatedObjectsCountSamplingMedian = GetSummaryMedian(dominatedObjectsCountSampling);
             string[] dominatedObjectsOfBestObjectSamplingMedian = GetSummaryMedian(dominatedObjectsOfBestObjectSampling);

             string[] setCoverageSamplingQ1 = GetSummaryQ1(setCoverageSampling);
             string[] representationErrorSamplingQ1 = GetSummaryQ1(representationErrorSampling);
             string[] representationErrorSumSamplingQ1 = GetSummaryQ1(representationErrorSumSampling);
             string[] dominatedObjectsCountSamplingQ1 = GetSummaryQ1(dominatedObjectsCountSampling);
             string[] dominatedObjectsOfBestObjectSamplingQ1 = GetSummaryQ1(dominatedObjectsOfBestObjectSampling);

             string[] setCoverageSamplingQ3 = GetSummaryQ3(setCoverageSampling);
             string[] representationErrorSamplingQ3 = GetSummaryQ3(representationErrorSampling);
             string[] representationErrorSumSamplingQ3 = GetSummaryQ3(representationErrorSumSampling);
             string[] dominatedObjectsCountSamplingQ3 = GetSummaryQ3(dominatedObjectsCountSampling);
             string[] dominatedObjectsOfBestObjectSamplingQ3 = GetSummaryQ3(dominatedObjectsOfBestObjectSampling);

             string strAverage = FormatLineStringSample("average", "", reportDimensions.Average(), reportSkylineSize.Average(), reportTimeTotal.Average(), reportTimeAlgorithm.Average(), rsl[ReportsSampling.TimeMin].Average(), rsl[ReportsSampling.TimeMax].Average(), rsd[ReportsSampling.TimeVar].Average(), rsd[ReportsSampling.TimeStdDev].Average(), rsl[ReportsSampling.TimeMed].Average(), rsl[ReportsSampling.TimeQ1].Average(), rsl[ReportsSampling.TimeQ3].Average(), rsl[ReportsSampling.SizeMin].Average(), rsl[ReportsSampling.SizeMax].Average(), rsd[ReportsSampling.SizeVar].Average(), rsd[ReportsSampling.SizeStdDev].Average(), rsl[ReportsSampling.SizeMed].Average(), rsl[ReportsSampling.SizeQ1].Average(), rsl[ReportsSampling.SizeQ3].Average(), setCoverageSamplingAverage, representationErrorSamplingAverage, representationErrorSumSamplingAverage, dominatedObjectsCountSamplingAverage, dominatedObjectsOfBestObjectSamplingAverage, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, reportMinCorrelation.Average(), reportMaxCorrelation.Average(), reportCardinality.Average());
             string strMin = FormatLineStringSample("minimum", "", reportDimensions.Min(), reportSkylineSize.Min(), reportTimeTotal.Min(), reportTimeAlgorithm.Min(), rsl[ReportsSampling.TimeMin].Min(), rsl[ReportsSampling.TimeMax].Min(), rsd[ReportsSampling.TimeVar].Min(), rsd[ReportsSampling.TimeStdDev].Min(), rsl[ReportsSampling.TimeMed].Min(), rsl[ReportsSampling.TimeQ1].Min(), rsl[ReportsSampling.TimeQ3].Min(), rsl[ReportsSampling.SizeMin].Min(), rsl[ReportsSampling.SizeMax].Min(), rsd[ReportsSampling.SizeVar].Min(), rsd[ReportsSampling.SizeStdDev].Min(), rsl[ReportsSampling.SizeMed].Min(), rsl[ReportsSampling.SizeQ1].Min(), rsl[ReportsSampling.SizeQ3].Min(), setCoverageSamplingMin, representationErrorSamplingMin, representationErrorSumSamplingMin, dominatedObjectsCountSamplingMin, dominatedObjectsOfBestObjectSamplingMin, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, reportMinCorrelation.Min(), reportMaxCorrelation.Min(), reportCardinality.Min());
             string strMax = FormatLineStringSample("maximum", "", reportDimensions.Max(), reportSkylineSize.Max(), reportTimeTotal.Max(), reportTimeAlgorithm.Max(), rsl[ReportsSampling.TimeMin].Max(), rsl[ReportsSampling.TimeMax].Max(), rsd[ReportsSampling.TimeVar].Max(), rsd[ReportsSampling.TimeStdDev].Max(), rsl[ReportsSampling.TimeMed].Max(), rsl[ReportsSampling.TimeQ1].Max(), rsl[ReportsSampling.TimeQ3].Max(), rsl[ReportsSampling.SizeMin].Max(), rsl[ReportsSampling.SizeMax].Max(), rsd[ReportsSampling.SizeVar].Max(), rsd[ReportsSampling.SizeStdDev].Max(), rsl[ReportsSampling.SizeMed].Max(), rsl[ReportsSampling.SizeQ1].Max(), rsl[ReportsSampling.SizeQ3].Max(), setCoverageSamplingMax, representationErrorSamplingMax, representationErrorSumSamplingMax, dominatedObjectsCountSamplingMax, dominatedObjectsOfBestObjectSamplingMax, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, reportMinCorrelation.Max(), reportMaxCorrelation.Max(), reportCardinality.Max());
             string strVar = FormatLineStringSample("variance", "", MyMathematic.GetSampleVariance(reportDimensions), MyMathematic.GetSampleVariance(reportSkylineSize), MyMathematic.GetSampleVariance(reportTimeTotal), MyMathematic.GetSampleVariance(reportTimeAlgorithm), MyMathematic.GetSampleVariance(rsl[ReportsSampling.TimeMin]), MyMathematic.GetSampleVariance(rsl[ReportsSampling.TimeMax]), MyMathematic.GetSampleVariance(rsd[ReportsSampling.TimeVar]), MyMathematic.GetSampleVariance(rsd[ReportsSampling.TimeStdDev]), MyMathematic.GetSampleVariance(rsl[ReportsSampling.TimeMed]), MyMathematic.GetSampleVariance(rsl[ReportsSampling.TimeQ1]), MyMathematic.GetSampleVariance(rsl[ReportsSampling.TimeQ3]), MyMathematic.GetSampleVariance(rsl[ReportsSampling.SizeMin]), MyMathematic.GetSampleVariance(rsl[ReportsSampling.SizeMax]), MyMathematic.GetSampleVariance(rsd[ReportsSampling.SizeVar]), MyMathematic.GetSampleVariance(rsd[ReportsSampling.SizeStdDev]), MyMathematic.GetSampleVariance(rsl[ReportsSampling.SizeMed]), MyMathematic.GetSampleVariance(rsl[ReportsSampling.SizeQ1]), MyMathematic.GetSampleVariance(rsl[ReportsSampling.SizeQ3]), setCoverageSamplingVariance, representationErrorSamplingVariance, representationErrorSumSamplingVariance, dominatedObjectsCountSamplingVariance, dominatedObjectsOfBestObjectSamplingVariance, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, MyMathematic.GetSampleVariance(reportMinCorrelation), MyMathematic.GetSampleVariance(reportMaxCorrelation), MyMathematic.GetSampleVariance(reportCardinality));
             string strStd = FormatLineStringSample("stddeviation", "", MyMathematic.GetSampleStdDeviation(reportDimensions), MyMathematic.GetSampleStdDeviation(reportSkylineSize), MyMathematic.GetSampleStdDeviation(reportTimeTotal), MyMathematic.GetSampleStdDeviation(reportTimeAlgorithm), MyMathematic.GetSampleStdDeviation(rsl[ReportsSampling.TimeMin]), MyMathematic.GetSampleStdDeviation(rsl[ReportsSampling.TimeMax]), MyMathematic.GetSampleStdDeviation(rsd[ReportsSampling.TimeVar]), MyMathematic.GetSampleStdDeviation(rsd[ReportsSampling.TimeStdDev]), MyMathematic.GetSampleStdDeviation(rsl[ReportsSampling.TimeMed]), MyMathematic.GetSampleStdDeviation(rsl[ReportsSampling.TimeQ1]), MyMathematic.GetSampleStdDeviation(rsl[ReportsSampling.TimeQ3]), MyMathematic.GetSampleStdDeviation(rsl[ReportsSampling.SizeMin]), MyMathematic.GetSampleStdDeviation(rsl[ReportsSampling.SizeMax]), MyMathematic.GetSampleStdDeviation(rsd[ReportsSampling.SizeVar]), MyMathematic.GetSampleStdDeviation(rsd[ReportsSampling.SizeStdDev]), MyMathematic.GetSampleStdDeviation(rsl[ReportsSampling.SizeMed]), MyMathematic.GetSampleStdDeviation(rsl[ReportsSampling.SizeQ1]), MyMathematic.GetSampleStdDeviation(rsl[ReportsSampling.SizeQ3]), setCoverageSamplingStdDev, representationErrorSamplingStdDev, representationErrorSumSamplingStdDev, dominatedObjectsCountSamplingStdDev, dominatedObjectsOfBestObjectSamplingStdDev, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, MyMathematic.GetSampleStdDeviation(reportMinCorrelation), MyMathematic.GetSampleStdDeviation(reportMaxCorrelation), MyMathematic.GetSampleStdDeviation(reportCardinality));
             string strMed = FormatLineStringSample("median", "", Mathematic.Median(reportDimensions), Mathematic.Median(reportSkylineSize), Mathematic.Median(reportTimeTotal), Mathematic.Median(reportTimeAlgorithm), Mathematic.Median(rsl[ReportsSampling.TimeMin]), Mathematic.Median(rsl[ReportsSampling.TimeMax]), Mathematic.Median(rsd[ReportsSampling.TimeVar]), Mathematic.Median(rsd[ReportsSampling.TimeStdDev]), Mathematic.Median(rsl[ReportsSampling.TimeMed]), Mathematic.Median(rsl[ReportsSampling.TimeQ1]), Mathematic.Median(rsl[ReportsSampling.TimeQ3]), Mathematic.Median(rsl[ReportsSampling.SizeMin]), Mathematic.Median(rsl[ReportsSampling.SizeMax]), Mathematic.Median(rsd[ReportsSampling.SizeVar]), Mathematic.Median(rsd[ReportsSampling.SizeStdDev]), Mathematic.Median(rsl[ReportsSampling.SizeMed]), Mathematic.Median(rsl[ReportsSampling.SizeQ1]), Mathematic.Median(rsl[ReportsSampling.SizeQ3]), setCoverageSamplingMedian, representationErrorSamplingMedian, representationErrorSumSamplingMedian, dominatedObjectsCountSamplingMedian, dominatedObjectsOfBestObjectSamplingMedian, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, Mathematic.Median(reportMinCorrelation), Mathematic.Median(reportMaxCorrelation), Mathematic.Median(reportCardinality));
             string strQ1 = FormatLineStringSample("quartil 1", "", Mathematic.LowerQuartile(reportDimensions), Mathematic.LowerQuartile(reportSkylineSize), Mathematic.LowerQuartile(reportTimeTotal), Mathematic.LowerQuartile(reportTimeAlgorithm), Mathematic.LowerQuartile(rsl[ReportsSampling.TimeMin]), Mathematic.LowerQuartile(rsl[ReportsSampling.TimeMax]), Mathematic.LowerQuartile(rsd[ReportsSampling.TimeVar]), Mathematic.LowerQuartile(rsd[ReportsSampling.TimeStdDev]), Mathematic.LowerQuartile(rsl[ReportsSampling.TimeMed]), Mathematic.LowerQuartile(rsl[ReportsSampling.TimeQ1]), Mathematic.LowerQuartile(rsl[ReportsSampling.TimeQ3]), Mathematic.LowerQuartile(rsl[ReportsSampling.SizeMin]), Mathematic.LowerQuartile(rsl[ReportsSampling.SizeMax]), Mathematic.LowerQuartile(rsd[ReportsSampling.SizeVar]), Mathematic.LowerQuartile(rsd[ReportsSampling.SizeStdDev]), Mathematic.LowerQuartile(rsl[ReportsSampling.SizeMed]), Mathematic.LowerQuartile(rsl[ReportsSampling.SizeQ1]), Mathematic.LowerQuartile(rsl[ReportsSampling.SizeQ3]), setCoverageSamplingQ1, representationErrorSamplingQ1, representationErrorSumSamplingQ1, dominatedObjectsCountSamplingQ1, dominatedObjectsOfBestObjectSamplingQ1, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, Mathematic.LowerQuartile(reportMinCorrelation), Mathematic.LowerQuartile(reportMaxCorrelation), Mathematic.LowerQuartile(reportCardinality));
             string strQ3 = FormatLineStringSample("quartil 3", "", Mathematic.UpperQuartile(reportDimensions), Mathematic.UpperQuartile(reportSkylineSize), Mathematic.UpperQuartile(reportTimeTotal), Mathematic.UpperQuartile(reportTimeAlgorithm), Mathematic.UpperQuartile(rsl[ReportsSampling.TimeMin]), Mathematic.UpperQuartile(rsl[ReportsSampling.TimeMax]), Mathematic.UpperQuartile(rsd[ReportsSampling.TimeVar]), Mathematic.UpperQuartile(rsd[ReportsSampling.TimeStdDev]), Mathematic.UpperQuartile(rsl[ReportsSampling.TimeMed]), Mathematic.UpperQuartile(rsl[ReportsSampling.TimeQ1]), Mathematic.UpperQuartile(rsl[ReportsSampling.TimeQ3]), Mathematic.UpperQuartile(rsl[ReportsSampling.SizeMin]), Mathematic.UpperQuartile(rsl[ReportsSampling.SizeMax]), Mathematic.UpperQuartile(rsd[ReportsSampling.SizeVar]), Mathematic.UpperQuartile(rsd[ReportsSampling.SizeStdDev]), Mathematic.UpperQuartile(rsl[ReportsSampling.SizeMed]), Mathematic.UpperQuartile(rsl[ReportsSampling.SizeQ1]), Mathematic.UpperQuartile(rsl[ReportsSampling.SizeQ3]), setCoverageSamplingQ3, representationErrorSamplingQ3, representationErrorSumSamplingQ3, dominatedObjectsCountSamplingQ3, dominatedObjectsOfBestObjectSamplingQ3, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, new[] { "", "", "", "", "" }, Mathematic.UpperQuartile(reportMinCorrelation), Mathematic.UpperQuartile(reportMaxCorrelation), Mathematic.UpperQuartile(reportCardinality));

            sb.AppendLine(strAverage);
            sb.AppendLine(strMin);
            sb.AppendLine(strMax);
            sb.AppendLine(strVar);
            sb.AppendLine(strStd);
            sb.AppendLine(strMed);
            sb.AppendLine(strQ1);
            sb.AppendLine(strQ3);
            Debug.WriteLine(strAverage);
            Debug.WriteLine(strMin);
            Debug.WriteLine(strMax);
            Debug.WriteLine(strVar);
            Debug.WriteLine(strStd);
            Debug.WriteLine(strMed);
            Debug.WriteLine(strQ1);
            Debug.WriteLine(strQ3);

            //Separator Line
            sb.AppendLine(strSeparatorLine);
            Debug.WriteLine(strSeparatorLine);
        }

        private static string[] GetSummaryAverage(Dictionary<SkylineTypesSampling, List<double>> list)
        {
            var array = new string[Enum.GetValues(typeof (SkylineTypesSampling)).Length];

            var count = 0;
            foreach (
                SkylineTypesSampling skylineTypesSamplingType in
                    Enum.GetValues(typeof (SkylineTypesSampling)).Cast<SkylineTypesSampling>())
            {
                {
                    array[count] =
                        Math.Round(list[skylineTypesSamplingType].Average(), 2)
                            .ToString(CultureInfo.InvariantCulture);
                    count++;
                }
            }

            return array;
        }

        private static string[] GetSummaryMin(Dictionary<SkylineTypesSampling, List<double>> list)
        {
            var array = new string[Enum.GetValues(typeof(SkylineTypesSampling)).Length];

            var count = 0;
            foreach (
                SkylineTypesSampling skylineTypesSamplingType in
                    Enum.GetValues(typeof(SkylineTypesSampling)).Cast<SkylineTypesSampling>())
            {
                {
                    array[count] =
                        Math.Round(list[skylineTypesSamplingType].Min(), 2)
                            .ToString(CultureInfo.InvariantCulture);
                    count++;
                }
            }

            return array;
        }

        private static string[] GetSummaryMax(Dictionary<SkylineTypesSampling, List<double>> list)
        {
            var array = new string[Enum.GetValues(typeof(SkylineTypesSampling)).Length];

            var count = 0;
            foreach (
                SkylineTypesSampling skylineTypesSamplingType in
                    Enum.GetValues(typeof(SkylineTypesSampling)).Cast<SkylineTypesSampling>())
            {
                {
                    array[count] =
                        Math.Round(list[skylineTypesSamplingType].Max(), 2)
                            .ToString(CultureInfo.InvariantCulture);
                    count++;
                }
            }

            return array;
        }

        private static string[] GetSummaryVariance(Dictionary<SkylineTypesSampling, List<double>> list)
        {
            var array = new string[Enum.GetValues(typeof(SkylineTypesSampling)).Length];

            var count = 0;
            foreach (
                SkylineTypesSampling skylineTypesSamplingType in
                    Enum.GetValues(typeof(SkylineTypesSampling)).Cast<SkylineTypesSampling>())
            {
                {
                    array[count] =
                        Math.Round(MyMathematic.GetSampleVariance(list[skylineTypesSamplingType]), 2)
                            .ToString(CultureInfo.InvariantCulture);
                    count++;
                }
            }

            return array;            
        }

        private static string[] GetSummaryStdDev(Dictionary<SkylineTypesSampling, List<double>> list)
        {
               var array = new string[Enum.GetValues(typeof(SkylineTypesSampling)).Length];

            var count = 0;
            foreach (
                SkylineTypesSampling skylineTypesSamplingType in
                    Enum.GetValues(typeof(SkylineTypesSampling)).Cast<SkylineTypesSampling>())
            {
                {
                    array[count] =
                        Math.Round(MyMathematic.GetSampleStdDeviation(list[skylineTypesSamplingType]), 2)
                            .ToString(CultureInfo.InvariantCulture);
                    count++;
                }
            }

            return array;            
        }

                private static string[] GetSummaryMedian(Dictionary<SkylineTypesSampling, List<double>> list)
        {
               var array = new string[Enum.GetValues(typeof(SkylineTypesSampling)).Length];

            var count = 0;
            foreach (
                SkylineTypesSampling skylineTypesSamplingType in
                    Enum.GetValues(typeof(SkylineTypesSampling)).Cast<SkylineTypesSampling>())
            {
                {
                    array[count] =
                        Math.Round(Mathematic.Median(list[skylineTypesSamplingType]), 2)
                            .ToString(CultureInfo.InvariantCulture);
                    count++;
                }
            }

            return array;            
        }

                private static string[] GetSummaryQ1(Dictionary<SkylineTypesSampling, List<double>> list)
        {
               var array = new string[Enum.GetValues(typeof(SkylineTypesSampling)).Length];

            var count = 0;
            foreach (
                SkylineTypesSampling skylineTypesSamplingType in
                    Enum.GetValues(typeof(SkylineTypesSampling)).Cast<SkylineTypesSampling>())
            {
                {
                    array[count] =
                        Math.Round(Mathematic.LowerQuartile(list[skylineTypesSamplingType]), 2)
                            .ToString(CultureInfo.InvariantCulture);
                    count++;
                }
            }

            return array;            
        }

                private static string[] GetSummaryQ3(Dictionary<SkylineTypesSampling, List<double>> list)
        {
               var array = new string[Enum.GetValues(typeof(SkylineTypesSampling)).Length];

            var count = 0;
            foreach (
                SkylineTypesSampling skylineTypesSamplingType in
                    Enum.GetValues(typeof(SkylineTypesSampling)).Cast<SkylineTypesSampling>())
            {
                {
                    array[count] =
                        Math.Round(Mathematic.UpperQuartile(list[skylineTypesSamplingType]), 2)
                            .ToString(CultureInfo.InvariantCulture);
                    count++;
                }
            }

            return array;            
        }
    
        private void AddSummary(StringBuilder sb, String strSeparatorLine, List<long> reportDimensions, List<long> reportSkylineSize, List<long> reportTimeTotal, List<long> reportTimeAlgorithm, List<double> reportMinCorrelation, List<double> reportMaxCorrelation, List<double> reportCardinality, List<long> reportNumberOfMoves, List<long> reportNumberOfOperations)
        {
            //Separator Line
            Debug.WriteLine(strSeparatorLine);
            sb.AppendLine(strSeparatorLine);
            
            string strAverage = FormatLineString("average", "", reportDimensions.Average(), reportSkylineSize.Average(), reportTimeTotal.Average(), reportTimeAlgorithm.Average(), reportMinCorrelation.Average(), reportMaxCorrelation.Average(), reportCardinality.Average(), reportNumberOfMoves.Average(), reportNumberOfOperations.Average());
            string strMin = FormatLineString("minimum", "", reportDimensions.Min(), reportSkylineSize.Min(), reportTimeTotal.Min(), reportTimeAlgorithm.Min(), reportMinCorrelation.Min(), reportMaxCorrelation.Min(), reportCardinality.Min(), reportNumberOfMoves.Min(), reportNumberOfOperations.Min());
            string strMax = FormatLineString("maximum", "", reportDimensions.Max(), reportSkylineSize.Max(), reportTimeTotal.Max(), reportTimeAlgorithm.Max(), reportMinCorrelation.Max(), reportMaxCorrelation.Max(), reportCardinality.Max(), reportNumberOfMoves.Max(), reportNumberOfOperations.Max());
            //string strVar = FormatLineString("variance", "", mathematic.GetVariance(reportDimensions), mathematic.GetVariance(reportSkylineSize), mathematic.GetVariance(reportTimeTotal), mathematic.GetVariance(reportTimeAlgorithm), mathematic.GetVariance(reportMinCorrelation), mathematic.GetVariance(reportMaxCorrelation), mathematic.GetVariance(reportCardinality));
            //string strStd = FormatLineString("stddeviation", "", mathematic.GetStdDeviation(reportDimensions), mathematic.GetStdDeviation(reportSkylineSize), mathematic.GetStdDeviation(reportTimeTotal), mathematic.GetStdDeviation(reportTimeAlgorithm), mathematic.GetStdDeviation(reportMinCorrelation), mathematic.GetStdDeviation(reportCardinality));
            string strSamplevar = FormatLineString("sample variance", "", MyMathematic.GetSampleVariance(reportDimensions), MyMathematic.GetSampleVariance(reportSkylineSize), MyMathematic.GetSampleVariance(reportTimeTotal), MyMathematic.GetSampleVariance(reportTimeAlgorithm), MyMathematic.GetSampleVariance(reportMinCorrelation), MyMathematic.GetSampleVariance(reportMaxCorrelation), MyMathematic.GetSampleVariance(reportCardinality), MyMathematic.GetSampleVariance(reportNumberOfMoves), MyMathematic.GetSampleVariance(reportNumberOfOperations));
            string strSampleStd = FormatLineString("sample stddeviation", "", MyMathematic.GetSampleStdDeviation(reportDimensions), MyMathematic.GetSampleStdDeviation(reportSkylineSize), MyMathematic.GetSampleStdDeviation(reportTimeTotal), MyMathematic.GetSampleStdDeviation(reportTimeAlgorithm), MyMathematic.GetSampleStdDeviation(reportMinCorrelation), MyMathematic.GetSampleStdDeviation(reportMaxCorrelation), MyMathematic.GetSampleStdDeviation(reportCardinality), MyMathematic.GetSampleStdDeviation(reportNumberOfMoves), MyMathematic.GetSampleStdDeviation(reportNumberOfOperations));

            sb.AppendLine(strAverage);
            sb.AppendLine(strMin);
            sb.AppendLine(strMax);
            //sb.AppendLine(strVar);
            //sb.AppendLine(strStd);
            sb.AppendLine(strSamplevar);
            sb.AppendLine(strSampleStd);
            Debug.WriteLine(strAverage);
            Debug.WriteLine(strMin);
            Debug.WriteLine(strMax);
            //Debug.WriteLine(strVar);
            //Debug.WriteLine(strStd);
            Debug.WriteLine(strSamplevar);
            Debug.WriteLine(strSampleStd);

            //Separator Line
            sb.AppendLine(strSeparatorLine);
            Debug.WriteLine(strSeparatorLine);
        }



        private string FormatLineString(char paddingChar, string strTitle, string strTrial, string strDimension, string strSkyline, string strTimeTotal, string strTimeAlgo, string strMinCorrelation, string strMaxCorrelation, string strCardinality, string strNumberOfMoves, string strNumberOfComparisons)
        {
            //average line
            //trial|dimensions|skyline size|time total|time algorithm|correlation|
            string[] line = new string[12];
            line[0] = strTitle.PadLeft(19, paddingChar);
            line[1] = strTrial.PadLeft(11, paddingChar);
            line[2] = strDimension.PadLeft(10, paddingChar);
            line[3] = strSkyline.PadLeft(20, paddingChar);
            line[4] = strTimeTotal.PadLeft(20, paddingChar);
            line[5] = strTimeAlgo.PadLeft(20, paddingChar);
            line[6] = strMinCorrelation.PadLeft(20, paddingChar);
            line[7] = strMaxCorrelation.PadLeft(20, paddingChar);
            line[8] = strCardinality.PadLeft(25, paddingChar);
            line[9] = strNumberOfMoves.PadLeft(20, paddingChar);
            line[10] = strNumberOfComparisons.PadLeft(20, paddingChar);
            line[11] = "";
            return string.Join("|", line);
        }


        private string FormatLineString(string strTitle, string strTrial, double dimension, double skyline, double timeTotal, double timeAlgo, double minCorrelation, double maxCorrelation, double cardinality, double numberOfMoves, double numberOfComparisons)
        {
            return FormatLineString(' ', strTitle, strTrial, Math.Round(dimension, 2).ToString(CultureInfo.InvariantCulture), Math.Round(skyline, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeTotal, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeAlgo, 2).ToString(CultureInfo.InvariantCulture), Math.Round(minCorrelation, 2).ToString(), Math.Round(maxCorrelation, 2).ToString(), ToLongString(Math.Round(cardinality, 2)), Math.Round(numberOfMoves, 2).ToString(), Math.Round(numberOfComparisons, 2).ToString());
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
