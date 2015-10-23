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
        static readonly Random Rnd = new Random();
        static readonly Mathematic MyMathematic = new Mathematic();


        private enum ReportsSampling
        {
            TimeMin, TimeMax, TimeVar, TimeStdDev, TimeMed, TimeQ1, TimeQ3, SizeMin, SizeMax, SizeVar, SizeStdDev, SizeMed, SizeQ1, SizeQ3,
        }
        
        private enum SkylineTypesSampling
        {
            RandomAvg, RandomMin, RandomMax, RandomVar, RandomStdDev, RandomMed, RandomQ1, RandomQ3,
            SampleAvg, SampleMin, SampleMax, SampleVar, SampleStdDev, SampleMed, SampleQ1, SampleQ3,
            BestRankAvg, BestRankMin, BestRankMax, BestRankVar, BestRankStdDev, BestRankMed, BestRankQ1, BestRankQ3,
            SumRankAvg, SumRankMin, SumRankMax, SumRankVar, SumRankStdDev, SumRankMed, SumRankQ1, SumRankQ3
        }

        private enum SkylineTypesSingleSampling
        {
            Random,
            Sample,
            BestRank,
            SumRank
        }

        private enum ClusterAnalysisSampling
        {
            EntireDb, EntireSkyline, SampleSkyline, BestRank, SumRank
        }

        public Performance()
        {
            UseCLR = false;
            UseNormalizedValues = false;
        }

        public bool WriteRCommand { get; set; }

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
            ArchiveComparable,      //Preference set from 1st performance phase, up to 13 dimnension
            ArchiveIncomparable,    //Preference set from 1st performance phase, up to 13 dimnension
            Jon,                    //Preference set from 2nd performance phase
            Mya,                    //Preference set from 2nd performance phase
            Barra,                  //Preference set from 2nd performance phase
            All,                    //Take all preferences
            Numeric,                //Take only numeric preferences
            NumericIncomparable,    //Take only numeric preferences with incomparable levels
            Categoric,              //Take only categoric preferences
            CategoricIncomparable,  //Take only categoric preferences that contain incomparable tuples
            MinCardinality,         //Special collection of preferences which should perform well on Hexagon
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
        internal int SamplingSubspacesCount { get; set; }

        internal int SamplingSubspaceDimension { get; set; }

        public int SamplingSamplesCount { get; set; }

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

        private ArrayList GetCategoricalIncomparablePreferences()
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


            //Generates the R-Commands for the rpref package (for testig exactly the same statements in rpref)
            if (WriteRCommand)
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendLine("---------------------------------------------");
                sb.AppendLine("-----------------R-Commands------------------");
                sb.AppendLine("---------------------------------------------");


                sb.AppendLine("skylinesize <- array(" + listPreferences.Count + ":1)");

                for (int iPreferenceIndex = 0; iPreferenceIndex < listPreferences.Count; iPreferenceIndex++)
                {
                    ArrayList preferences = (ArrayList)listPreferences[iPreferenceIndex];
                    ArrayList subPreferences = preferences; //.GetRange(0, i);

                    sb.Append("system.time(sky1 <- psel(mydata, ");
                    
                    foreach(String pref in preferences) {
                        String rCommand = "";
                        
                        if(pref.IndexOf("cars.") == -1) {
                            //Categorical preferences
                            //String tableName = pref.Substring(0, pref.IndexOf("."));
                            rCommand = "low(" + pref.Substring(0, pref.IndexOf(" (")).Replace(".name", "") + ")";
                            /*preferences.Add("colors.name ('red' >> 'blue' >> 'green' >> 'gold' >> 'black' >> 'gray' >> 'bordeaux' >> OTHERS EQUAL)");
                            preferences.Add("bodies.name ('bus' >> 'cabriolet' >> 'limousine' >> 'coupé' >> 'van' >> 'estate car' >> OTHERS EQUAL)");
                            preferences.Add("fuels.name ('petrol' >> 'diesel' >> 'bioethanol' >> 'electro' >> 'gas' >> 'hybrid' >> OTHERS EQUAL)");
                            preferences.Add("makes.name ('BENTLEY' >> 'DAIMLER' >> 'FIAT'>> 'FORD'  >> OTHERS EQUAL)");
                            preferences.Add("conditions.name ('new' >> 'occasion' >> 'demonstration car' >> 'oldtimer' >> OTHERS EQUAL)");
                            preferences.Add("drives.name ('front wheel' >> 'all wheel' >> 'rear wheel' >> OTHERS EQUAL)");
                            preferences.Add("transmissions.name ('manual' >> 'automatic' >> OTHERS EQUAL)");*/
                        }
                        else
                        {
                            //Numeric preference
                            if (pref.IndexOf("LOW") > 0)
                            {
                                //LOW preference
                                rCommand = "low(" + pref.Substring(0, pref.IndexOf(" ")).Replace("cars.", "") +")";
                            }
                            else
                            {
                                //HIGH preferences
                                rCommand = "high(" + pref.Substring(0, pref.IndexOf(" ")).Replace("cars.", "") + ")";
                            }
                        }
                        
                        
                        sb.Append(rCommand);
                        //Don't add * on last record
                        if (pref != (string)preferences[preferences.Count-1])
                        {
                            sb.Append(" * ");
                        }
                        
                    }
                    sb.AppendLine("))");
                    sb.AppendLine("skylinesize[" + (iPreferenceIndex + 1) + "] = nrow(sky1)");
                    sb.AppendLine("skylinesize[" + (iPreferenceIndex + 1) + "]");
                    
                    //string strSkylineOf = "SKYLINE OF " + string.Join(",", (string[])subPreferences.ToArray(Type.GetType("System.String")));
                    //sb.AppendLine(strSkylineOf);
                }

                sb.AppendLine("mean(skylinesize)");


                sb.AppendLine("---------------------------------------------");
                sb.AppendLine("---------------------------------------------");
                sb.AppendLine("---------------------------------------------");
                sb.AppendLine("");
                sb.AppendLine("");

                Debug.Write(sb.ToString());
            }

            foreach(SkylineStrategy currentStrategy in listStrategy)
            {
                //Take all strategies



                StringBuilder sb = new StringBuilder();
                string strSeparatorLine;
                if (Sampling)
                {
                    strSeparatorLine = FormatLineStringSample('-', "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", new Dictionary<SkylineTypesSingleSampling, List<double>>(), new Dictionary<SkylineTypesSingleSampling, List<double>>(), new Dictionary<SkylineTypesSingleSampling, List<double>>(), new Dictionary<SkylineTypesSingleSampling, List<double>>(), new Dictionary<SkylineTypesSingleSampling, List<double>>(), new Dictionary<ClusterAnalysisSampling, string>(), new Dictionary<ClusterAnalysisSampling, string>(), new Dictionary<ClusterAnalysisSampling, string>(), new Dictionary<ClusterAnalysisSampling, string>(), "", "", "");
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
                        sb.AppendLine("         Subspaces Count: " + SamplingSubspacesCount);
                        sb.AppendLine("      Subspace Dimension: " + SamplingSubspaceDimension);
                        sb.AppendLine("           Sampling Runs: " + SamplingSamplesCount);
                    }
                    sb.AppendLine("");
                    if (Sampling)
                    {
                        sb.AppendLine(FormatLineStringSample(' ', "preference set", "trial", "dimensions", "avg skyline size", "avg time total", "avg time algorithm", "min time", "max time", "variance time", "stddeviation time", "median time", "q1 time", "q3 time", "min size", "max size", "variance size", "stddeviation size", "median size", "q1 size", "q3 size", new[] { "avg sc random", "min sc random", "max sc random", "var sc random", "stddev sc random", "med sc random", "q1 sc random", "q3 sc random", "avg sc sample", "min sc sample", "max sc sample", "var sc sample", "stddev sc sample", "med sc sample", "q1 sc sample", "q3 sc sample", "avg sc Best", "min sc Best", "max sc Best", "var sc Best", "stddev sc Best", "med sc Best", "q1 sc Best", "q3 sc Best", "avg sc Sum", "min sc Sum", "max sc Sum", "var sc Sum", "stddev sc Sum", "med sc Sum", "q1 sc Sum", "q3 sc Sum" }, new[] { "avg re random", "min re random", "max re random", "var re random", "stddev re random", "med re random", "q1 re random", "q3 re random", "avg re sample", "min re sample", "max re sample", "var re sample", "stddev re sample", "med re sample", "q1 re sample", "q3 re sample", "avg re Best", "min re Best", "max re Best", "var re Best", "stddev re Best", "med re Best", "q1 re Best", "q3 re Best", "avg re Sum", "min re Sum", "max re Sum", "var re Sum", "stddev re Sum", "med re Sum", "q1 re Sum", "q3 re Sum" }, new[] { "avg reSum random", "min reSum random", "max reSum random", "var reSum random", "stddev reSum random", "med reSum random", "q1 reSum random", "q3 reSum random", "avg reSum sample", "min reSum sample", "max reSum sample", "var reSum sample", "stddev reSum sample", "med reSum sample", "q1 reSum sample", "q3 reSum sample", "avg reSum Best", "min reSum Best", "max reSum Best", "var reSum Best", "stddev reSum Best", "med reSum Best", "q1 reSum Best", "q3 reSum Best", "avg reSum Sum", "min reSum Sum", "max reSum Sum", "var reSum Sum", "stddev reSum Sum", "med reSum Sum", "q1 reSum Sum", "q3 reSum Sum" }, new[] { "avg domCnt random", "min domCnt random", "max domCnt random", "var domCnt random", "stddev domCnt random", "med domCnt random", "q1 domCnt random", "q3 domCnt random", "avg domCnt sample", "min domCnt sample", "max domCnt sample", "var domCnt sample", "stddev domCnt sample", "med domCnt sample", "q1 domCnt sample", "q3 domCnt sample", "avg domCnt Best", "min domCnt Best", "max domCnt Best", "var domCnt Best", "stddev domCnt Best", "med domCnt Best", "q1 domCnt Best", "q3 domCnt Best", "avg domCnt Sum", "min domCnt Sum", "max domCnt Sum", "var domCnt Sum", "stddev domCnt Sum", "med domCnt Sum", "q1 domCnt Sum", "q3 domCnt Sum" }, new[] { "avg domBst random", "min domBst random", "max domBst random", "var domBst random", "stddev domBst random", "med domBst random", "q1 domBst random", "q3 domBst random", "avg domBst sample", "min domBst sample", "max domBst sample", "var domBst sample", "stddev domBst sample", "med domBst sample", "q1 domBst sample", "q3 domBst sample", "avg domBst Best", "min domBst Best", "max domBst Best", "var domBst Best", "stddev domBst Best", "med domBst Best", "q1 domBst Best", "q3 domBst Best", "avg domBst Sum", "min domBst Sum", "max domBst Sum", "var domBst Sum", "stddev domBst Sum", "med domBst Sum", "q1 domBst Sum", "q3 domBst Sum" }, new[] { "ca entire db", "ca entire skyline", "ca sample skyline", "ca best rank", "ca sum rank" }, new[] { "caMed entire db", "caMed entire skyline", "caMed sample skyline", "caMed best rank", "caMed sum rank" }, new[] { "caTopB entire db", "caTopB entire skyline", "caTopB sample skyline", "caTopB best rank", "caTopB sum rank" }, new[] { "caMedTopB entire db", "caMedTopB entire skyline", "caMedTopB sample skyline", "caMedTopB best rank", "caMedTopB sum rank" }, "min correlation", "max correlation", "product cardinality"));
                    } else
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

                Dictionary<ReportsSampling, List<long>> reportsSamplingLong;
                Dictionary<ReportsSampling, List<double>> reportsSamplingDouble;
                Dictionary<SkylineTypesSampling, List<double>> setCoverageSampling;
                Dictionary<SkylineTypesSampling, List<double>> representationErrorSampling;
                Dictionary<SkylineTypesSampling, List<double>> representationErrorSumSampling;
                Dictionary<SkylineTypesSampling, List<double>> dominatedObjectsCountSampling;
                Dictionary<SkylineTypesSampling, List<double>> dominatedObjectsOfBestObjectSampling;
                Dictionary<ClusterAnalysisSampling, List<List<double>>> clusterAnalysisSampling;
                Dictionary<ClusterAnalysisSampling, List<List<double>>> clusterAnalysisMedianSampling;
                Dictionary<ClusterAnalysisSampling, Dictionary<BigInteger, List<double>>> clusterAnalysisTopBucketsSampling;
                Dictionary<ClusterAnalysisSampling, Dictionary<BigInteger, List<double>>> clusterAnalysisMedianTopBucketsSampling;
                InitSamplingDataStructures(out reportsSamplingLong, out reportsSamplingDouble, out setCoverageSampling, out representationErrorSampling, out representationErrorSumSampling, out dominatedObjectsCountSampling, out dominatedObjectsOfBestObjectSampling, out clusterAnalysisSampling, out clusterAnalysisMedianSampling, out clusterAnalysisTopBucketsSampling, out clusterAnalysisMedianTopBucketsSampling);

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

                                    List<IEnumerable<CLRSafeHashSet<int>>> producedSubspaces = ProduceSubspaces(preferences);

                                    InitClusterAnalysisSamplingDataStructures(out clusterAnalysisSampling);
                                    InitClusterAnalysisSamplingDataStructures(out clusterAnalysisMedianSampling);
                                    InitClusterAnalysisTopBucketsSamplingDataStructures(out clusterAnalysisTopBucketsSampling);
                                    InitClusterAnalysisTopBucketsSamplingDataStructures(out clusterAnalysisMedianTopBucketsSampling);

                                    DataTable entireSkylineDataTable =
                                        parser.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName,
                                            strSQL);

                                    List<long[]> entireDataTableSkylineValues =
                                        parser.SkylineType.Strategy.SkylineValues;

                                    int[] skylineAttributeColumns =
                                        SkylineSamplingHelper.GetSkylineAttributeColumns(entireSkylineDataTable);

                                    IReadOnlyDictionary<long, object[]> entireSkylineDatabase =
                                        prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(
                                            entireSkylineDataTable, 0);
                                    IReadOnlyDictionary<long, object[]> entireSkylineNormalized =
                                        prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(
                                            entireSkylineDataTable, 0);
                                    SkylineSamplingHelper.NormalizeColumns(entireSkylineNormalized,
                                        skylineAttributeColumns);

                                    DataTable entireDataTable;
                                        IReadOnlyDictionary<long, object[]> entireDatabaseNormalized =
                                        GetEntireDatabaseNormalized(parser, strSQL, skylineAttributeColumns, out entireDataTable);
                                        IReadOnlyDictionary<long, object[]> entireDatabase =
                                            prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(
                                                entireDataTable, 0);

                                      IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                                        entireDatabaseBuckets =
                                            ClusterAnalysis.GetBuckets(entireDatabaseNormalized,
                                                skylineAttributeColumns);

                                    IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                        aggregatedEntireDatabaseBuckets =
                                            ClusterAnalysis.GetAggregatedBuckets(entireDatabaseBuckets);

                                      foreach (KeyValuePair<BigInteger, List<IReadOnlyDictionary<long, object[]>>> s in entireDatabaseBuckets.OrderByDescending(l => l.Value.Count)
                         .ThenBy(l => l.Key).Take(5))
                                    {                                       
                                        double percent = (double) s.Value.Count / entireDatabaseNormalized.Count;
                                        clusterAnalysisTopBucketsSampling[ClusterAnalysisSampling.EntireDb].Add(s.Key,
                                            new List<double>());

                                        for (var i = 0; i < producedSubspaces.Count; i++) // to enable generalized average calculation
                                        {
                                            clusterAnalysisTopBucketsSampling[ClusterAnalysisSampling.EntireDb][s.Key]
                                                .Add(percent);
                                        }
                                    }

                                       IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                                        entireSkylineBuckets =
                                            ClusterAnalysis.GetBuckets(entireSkylineNormalized,
                                                skylineAttributeColumns);

                                    IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                        aggregatedEntireSkylineBuckets =
                                            ClusterAnalysis.GetAggregatedBuckets(entireSkylineBuckets);

                                     FillTopBucketsSampling(clusterAnalysisTopBucketsSampling, ClusterAnalysisSampling.EntireSkyline, entireSkylineBuckets, entireSkylineNormalized.Count, entireDatabaseNormalized.Count, entireSkylineNormalized.Count);
                                     foreach (
                                          KeyValuePair<BigInteger, List<double>> bucket in clusterAnalysisTopBucketsSampling[ClusterAnalysisSampling.EntireSkyline])
                                     {
                                         double percent =
                                             clusterAnalysisTopBucketsSampling[ClusterAnalysisSampling.EntireSkyline][bucket.Key][0];

                                         for (var i = 1; i < producedSubspaces.Count; i++) // to enable generalized average calculation
                                         {
                                             clusterAnalysisTopBucketsSampling[ClusterAnalysisSampling.EntireSkyline][bucket.Key].Add(percent);
                                         }
                                     }

                                    var clusterAnalysisForMedian = new ClusterAnalysis(entireDatabaseNormalized, skylineAttributeColumns);
                                 
                                    IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                                        entireDatabaseMedianBuckets =
                                            clusterAnalysisForMedian.GetBuckets(entireDatabaseNormalized,
                                                skylineAttributeColumns,true);

                                    IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                        aggregatedEntireDatabaseMedianBuckets =
                                            ClusterAnalysis.GetAggregatedBuckets(entireDatabaseMedianBuckets);

                                    foreach (
                                        KeyValuePair<BigInteger, List<IReadOnlyDictionary<long, object[]>>> s in
                                            entireDatabaseMedianBuckets.OrderByDescending(l => l.Value.Count)
                                                .ThenBy(l => l.Key).Take(5))
                                    {
                                        double percent = (double) s.Value.Count / entireDatabaseNormalized.Count;
                                        clusterAnalysisMedianTopBucketsSampling[ClusterAnalysisSampling.EntireDb].Add(
                                            s.Key,
                                            new List<double>());

                                        for (var i = 0; i < producedSubspaces.Count; i++)
                                            // to enable generalized average calculation
                                        {
                                            clusterAnalysisMedianTopBucketsSampling[ClusterAnalysisSampling.EntireDb][
                                                s.Key]
                                                .Add(percent);
                                        }
                                    }

                                    IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                                        entireSkylineMedianBuckets =
                                            clusterAnalysisForMedian.GetBuckets(entireSkylineNormalized,
                                                skylineAttributeColumns,true);

                                    IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                      aggregatedEntireSkylineMedianBuckets =
                                         ClusterAnalysis.GetAggregatedBuckets(entireSkylineMedianBuckets);

                                    FillTopBucketsSampling(clusterAnalysisMedianTopBucketsSampling, ClusterAnalysisSampling.EntireSkyline, entireSkylineMedianBuckets, entireSkylineNormalized.Count, entireDatabaseNormalized.Count, entireSkylineNormalized.Count);
                                    
                                    foreach (
                                       KeyValuePair<BigInteger, List<double>> bucket in clusterAnalysisMedianTopBucketsSampling[ClusterAnalysisSampling.EntireSkyline])
                                    {
                                        double percent =
                                            clusterAnalysisMedianTopBucketsSampling[ClusterAnalysisSampling.EntireSkyline][bucket.Key][0];

                                        for (var i = 1; i < producedSubspaces.Count; i++) // to enable generalized average calculation
                                        {
                                            clusterAnalysisMedianTopBucketsSampling[ClusterAnalysisSampling.EntireSkyline][bucket.Key].Add(percent);
                                        }
                                    }
                                    strSQL += " SAMPLE BY RANDOM_SUBSETS COUNT " + SamplingSubspacesCount +
                                              " DIMENSION " + SamplingSubspaceDimension;

                                    string strQuery;
                                    string operators;
                                    int numberOfRecords;
                                    string[] parameter;

                                    PrefSQLModel prefSqlModel = parser.GetPrefSqlModelFromPreferenceSql(strSQL);
                                    string ansiSql = parser.GetAnsiSqlFromPrefSqlModel(prefSqlModel);
                                    prefSQL.SQLParser.Helper.DetermineParameters(ansiSql, out parameter,
                                        out strQuery, out operators,
                                        out numberOfRecords);

                                    var subspaceObjects = new List<long>();
                                    var subspaceTime = new List<long>();
                                    var subspaceTimeElapsed = new List<long>();
                                    var setCoverageSecondRandom = new List<double>();
                                    var setCoverageSample = new List<double>();
                                    var setCoverageBestRank = new List<double>();
                                    var setCoverageSumRank = new List<double>();

                                    var representationErrorSecondRandom = new List<double>();
                                    var representationErrorSample = new List<double>();
                                    var representationErrorBestRank = new List<double>();
                                    var representationErrorSumRank = new List<double>();

                                    var representationErrorSumSecondRandom = new List<double>();
                                    var representationErrorSumSample = new List<double>();
                                    var representationErrorSumBestRank = new List<double>();
                                    var representationErrorSumSumRank = new List<double>();

                                    var dominatedObjectsCountSamplingSecondRandom = new List<double>();
                                    var dominatedObjectsCountSamplingSample = new List<double>();
                                    var dominatedObjectsCountSamplingBestRank = new List<double>();
                                    var dominatedObjectsCountSamplingSumRank = new List<double>();

                                    var dominatedObjectsOfBestObjectSamplingSecondRandom = new List<double>();
                                    var dominatedObjectsOfBestObjectSamplingSample = new List<double>();
                                    var dominatedObjectsOfBestObjectSamplingBestRank = new List<double>();
                                    var dominatedObjectsOfBestObjectSamplingSumRank = new List<double>();

                                    var subspaceCount = 1;
                                    foreach (IEnumerable<CLRSafeHashSet<int>> subspace in producedSubspaces)
                                    {
                                        Console.WriteLine(strPreferenceSet+ " (" + subspaceCount+" / " + producedSubspaces.Count+")");

                                        sw.Restart(); 
                                        var subspacesProducer = new FixedSkylineSamplingSubspacesProducer(subspace);
                                        var utility = new SkylineSamplingUtility(subspacesProducer);
                                        var skylineSample = new SkylineSampling(utility)
                                        {
                                            SubspacesCount = prefSqlModel.SkylineSampleCount,
                                            SubspaceDimension = prefSqlModel.SkylineSampleDimension,
                                            SelectedStrategy = parser.SkylineType
                                        };

                                        DataTable sampleSkylineDataTable = skylineSample.GetSkylineTable(strQuery,
                                            operators);

                                        sw.Stop();

                                        subspaceObjects.Add(sampleSkylineDataTable.Rows.Count);
                                        subspaceTime.Add(skylineSample.TimeMilliseconds);
                                        subspaceTimeElapsed.Add(sw.ElapsedMilliseconds);

                                        IReadOnlyDictionary<long, object[]> sampleSkylineDatabase =
                                            prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(
                                                sampleSkylineDataTable, 0);
                                        IReadOnlyDictionary<long, object[]> sampleSkylineNormalized =
                                            prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(
                                                sampleSkylineDataTable, 0);
                                        SkylineSamplingHelper.NormalizeColumns(sampleSkylineNormalized,
                                            skylineAttributeColumns);

                                        IReadOnlyDictionary<long, object[]> baseRandomSampleNormalized =
                                            SkylineSamplingHelper.GetRandomSample(entireSkylineNormalized,
                                                sampleSkylineDataTable.Rows.Count);

                                        IReadOnlyDictionary<long, object[]> secondRandomSampleDatabase =
                                            SkylineSamplingHelper.GetRandomSample(entireSkylineDatabase,
                                                sampleSkylineDataTable.Rows.Count);
                                        var secondRandomSampleNormalizedToBeCreated = new Dictionary<long, object[]>();
                                        foreach (KeyValuePair<long, object[]> k in secondRandomSampleDatabase)
                                        {
                                            var newValue=new object[k.Value.Length];
                                            k.Value.CopyTo(newValue, 0);
                                            secondRandomSampleNormalizedToBeCreated.Add(k.Key, newValue);
                                        }
                                        IReadOnlyDictionary<long, object[]> secondRandomSampleNormalized =
                                            new ReadOnlyDictionary<long, object[]>(secondRandomSampleNormalizedToBeCreated);
                                        SkylineSamplingHelper.NormalizeColumns(secondRandomSampleNormalized, skylineAttributeColumns);

                                        IReadOnlyDictionary<long, object[]> entireSkylineDataTableBestRankDatabase;
                                        IReadOnlyDictionary<long, object[]> entireSkylineDataTableBestRankNormalized =
                                            GetEntireSkylineDataTableRankNormalized(entireSkylineDataTable.Copy(),
                                                entireDataTableSkylineValues, skylineAttributeColumns,
                                                sampleSkylineDataTable.Rows.Count, 1, out entireSkylineDataTableBestRankDatabase);

                                        IReadOnlyDictionary<long, object[]> entireSkylineDataTableSumRankDatabase;
                                        IReadOnlyDictionary<long, object[]> entireSkylineDataTableSumRankNormalized =
                                            GetEntireSkylineDataTableRankNormalized(entireSkylineDataTable.Copy(),
                                                entireDataTableSkylineValues, skylineAttributeColumns,
                                                sampleSkylineDataTable.Rows.Count, 2, out entireSkylineDataTableSumRankDatabase);

                                        double setCoverageCoveredBySecondRandomSample = SetCoverage.GetCoverage(
                                            baseRandomSampleNormalized,
                                            secondRandomSampleNormalized, skylineAttributeColumns) * 100.0;
                                        double setCoverageCoveredBySkylineSample = SetCoverage.GetCoverage(
                                            baseRandomSampleNormalized,
                                            sampleSkylineNormalized, skylineAttributeColumns) * 100.0;
                                        double setCoverageCoveredByEntireBestRank =
                                            SetCoverage.GetCoverage(baseRandomSampleNormalized,
                                                entireSkylineDataTableBestRankNormalized, skylineAttributeColumns) *
                                            100.0;
                                        double setCoverageCoveredByEntireSumRank =
                                            SetCoverage.GetCoverage(baseRandomSampleNormalized,
                                                entireSkylineDataTableSumRankNormalized, skylineAttributeColumns) *
                                            100.0;

                                        setCoverageSecondRandom.Add(setCoverageCoveredBySecondRandomSample);
                                        setCoverageSample.Add(setCoverageCoveredBySkylineSample);
                                        setCoverageBestRank.Add(setCoverageCoveredByEntireBestRank);
                                        setCoverageSumRank.Add(setCoverageCoveredByEntireSumRank);

                                        double representationErrorSecondRandomSample = SetCoverage
                                            .GetRepresentationError(
                                                GetReducedSkyline(entireSkylineNormalized, secondRandomSampleNormalized),
                                                secondRandomSampleNormalized, skylineAttributeColumns).Max() * 100.0;
                                        double representationErrorSkylineSample = SetCoverage.GetRepresentationError(
                                            GetReducedSkyline(entireSkylineNormalized, sampleSkylineNormalized),
                                            sampleSkylineNormalized, skylineAttributeColumns).Max() * 100.0;
                                        double representationErrorEntireBestRank =
                                            SetCoverage.GetRepresentationError(
                                                GetReducedSkyline(entireSkylineNormalized,
                                                    entireSkylineDataTableBestRankNormalized),
                                                entireSkylineDataTableBestRankNormalized, skylineAttributeColumns).Max() *
                                            100.0;
                                        double representationErrorEntireSumRank =
                                            SetCoverage.GetRepresentationError(
                                                GetReducedSkyline(entireSkylineNormalized,
                                                    entireSkylineDataTableSumRankNormalized),
                                                entireSkylineDataTableSumRankNormalized, skylineAttributeColumns).Max() *
                                            100.0;

                                        representationErrorSecondRandom.Add(representationErrorSecondRandomSample);
                                        representationErrorSample.Add(representationErrorSkylineSample);
                                        representationErrorBestRank.Add(representationErrorEntireBestRank);
                                        representationErrorSumRank.Add(representationErrorEntireSumRank);

                                        double representationErrorSumSecondRandomSample = SetCoverage
                                            .GetRepresentationError(
                                                GetReducedSkyline(entireSkylineNormalized, secondRandomSampleNormalized),
                                                secondRandomSampleNormalized, skylineAttributeColumns).Sum() * 100.0;
                                        double representationErrorSumSkylineSample = SetCoverage.GetRepresentationError(
                                            GetReducedSkyline(entireSkylineNormalized, sampleSkylineNormalized),
                                            sampleSkylineNormalized, skylineAttributeColumns).Sum() * 100.0;
                                        double representationErrorSumEntireBestRank =
                                            SetCoverage.GetRepresentationError(
                                                GetReducedSkyline(entireSkylineNormalized,
                                                    entireSkylineDataTableBestRankNormalized),
                                                entireSkylineDataTableBestRankNormalized, skylineAttributeColumns).Sum() *
                                            100.0;
                                        double representationErrorSumEntireSumRank =
                                            SetCoverage.GetRepresentationError(
                                                GetReducedSkyline(entireSkylineNormalized,
                                                    entireSkylineDataTableSumRankNormalized),
                                                entireSkylineDataTableSumRankNormalized, skylineAttributeColumns)
                                                .Sum() * 100.0;

                                        representationErrorSumSecondRandom.Add(
                                            representationErrorSumSecondRandomSample);
                                        representationErrorSumSample.Add(representationErrorSumSkylineSample);
                                        representationErrorSumBestRank.Add(representationErrorSumEntireBestRank);
                                        representationErrorSumSumRank.Add(representationErrorSumEntireSumRank);

                                        var dominatedObjectsCountRandomSample =
                                            new DominatedObjects(entireDatabase,
                                                secondRandomSampleDatabase,
                                                skylineAttributeColumns);
                                        var dominatedObjectsCountSampleSkyline =
                                            new DominatedObjects(entireDatabase, sampleSkylineDatabase,
                                                skylineAttributeColumns);
                                        var dominatedObjectsCountEntireSkylineBestRank =
                                            new DominatedObjects(entireDatabase,
                                                entireSkylineDataTableBestRankDatabase, skylineAttributeColumns);
                                        var dominatedObjectsCountEntireSkylineSumRank =
                                            new DominatedObjects(entireDatabase,
                                                entireSkylineDataTableSumRankDatabase, skylineAttributeColumns);

                                        dominatedObjectsCountSamplingSecondRandom.Add(
                                            dominatedObjectsCountRandomSample.NumberOfDistinctDominatedObjects);
                                        dominatedObjectsCountSamplingSample.Add(
                                            dominatedObjectsCountSampleSkyline.NumberOfDistinctDominatedObjects);
                                        dominatedObjectsCountSamplingBestRank.Add(
                                            dominatedObjectsCountEntireSkylineBestRank.NumberOfDistinctDominatedObjects);
                                        dominatedObjectsCountSamplingSumRank.Add(
                                            dominatedObjectsCountEntireSkylineSumRank.NumberOfDistinctDominatedObjects);

                                        dominatedObjectsOfBestObjectSamplingSecondRandom.Add(
                                            dominatedObjectsCountRandomSample
                                                .NumberOfObjectsDominatedByEachObjectOrderedByDescCount.First().Value);
                                        dominatedObjectsOfBestObjectSamplingSample.Add(
                                            dominatedObjectsCountSampleSkyline
                                                .NumberOfObjectsDominatedByEachObjectOrderedByDescCount.First().Value);
                                        dominatedObjectsOfBestObjectSamplingBestRank.Add(
                                            dominatedObjectsCountEntireSkylineBestRank
                                                .NumberOfObjectsDominatedByEachObjectOrderedByDescCount.First().Value);
                                        dominatedObjectsOfBestObjectSamplingSumRank.Add(
                                            dominatedObjectsCountEntireSkylineSumRank
                                                .NumberOfObjectsDominatedByEachObjectOrderedByDescCount.First().Value);

                                        IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>> sampleBuckets =
                                                ClusterAnalysis.GetBuckets(sampleSkylineNormalized,
                                                    skylineAttributeColumns);
                                        IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                            aggregatedSampleBuckets =
                                                ClusterAnalysis.GetAggregatedBuckets(sampleBuckets);
                                         IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>> bestRankBuckets =
                                                ClusterAnalysis.GetBuckets(
                                                    entireSkylineDataTableBestRankNormalized,
                                                    skylineAttributeColumns);
                                        IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                            aggregatedBestRankBuckets =
                                                ClusterAnalysis.GetAggregatedBuckets(
                                                    bestRankBuckets);
                                        IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                                            sumRankBuckets =
                                                ClusterAnalysis.GetBuckets(
                                                    entireSkylineDataTableSumRankNormalized,
                                                    skylineAttributeColumns);
                                        IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                            aggregatedSumRankBuckets =
                                                ClusterAnalysis.GetAggregatedBuckets(
                                                    sumRankBuckets);

                                        FillTopBucketsSampling(clusterAnalysisTopBucketsSampling, ClusterAnalysisSampling.SampleSkyline, sampleBuckets, sampleSkylineNormalized.Count, entireDatabaseNormalized.Count,entireSkylineNormalized.Count);                            
                                        FillTopBucketsSampling(clusterAnalysisTopBucketsSampling, ClusterAnalysisSampling.BestRank, bestRankBuckets, entireSkylineDataTableBestRankNormalized.Count, entireDatabaseNormalized.Count,entireSkylineNormalized.Count);                            
                                        FillTopBucketsSampling(clusterAnalysisTopBucketsSampling, ClusterAnalysisSampling.SumRank, sumRankBuckets, entireSkylineDataTableSumRankNormalized.Count, entireDatabaseNormalized.Count,entireSkylineNormalized.Count);                            

                                         IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                                       sampleMedianBuckets =
                                           clusterAnalysisForMedian.GetBuckets(sampleSkylineNormalized,
                                               skylineAttributeColumns,true);
                                        IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                       aggregatedSampleMedianBuckets =
                                           ClusterAnalysis.GetAggregatedBuckets(sampleMedianBuckets);
                                        IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                                            bestRankMedianBuckets =
                                                clusterAnalysisForMedian.GetBuckets(
                                                    entireSkylineDataTableBestRankNormalized,
                                                    skylineAttributeColumns, true);
                                        IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                            aggregatedBestRankMedianBuckets =
                                                ClusterAnalysis.GetAggregatedBuckets(
                                                    bestRankMedianBuckets);
                                        IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>>
                                            sumRankMedianBuckets =
                                                clusterAnalysisForMedian.GetBuckets(
                                                    entireSkylineDataTableSumRankNormalized,
                                                    skylineAttributeColumns, true);
                                        IReadOnlyDictionary<int, List<IReadOnlyDictionary<long, object[]>>>
                                            aggregatedSumRankMedianBuckets =
                                                ClusterAnalysis.GetAggregatedBuckets(
                                                    sumRankMedianBuckets);

                                        FillTopBucketsSampling(clusterAnalysisMedianTopBucketsSampling, ClusterAnalysisSampling.SampleSkyline, sampleMedianBuckets, sampleSkylineNormalized.Count, entireDatabaseNormalized.Count,entireSkylineNormalized.Count);                            
                                        FillTopBucketsSampling(clusterAnalysisMedianTopBucketsSampling, ClusterAnalysisSampling.BestRank, bestRankMedianBuckets, entireSkylineDataTableBestRankNormalized.Count, entireDatabaseNormalized.Count,entireSkylineNormalized.Count);                            
                                        FillTopBucketsSampling(clusterAnalysisMedianTopBucketsSampling, ClusterAnalysisSampling.SumRank, sumRankMedianBuckets, entireSkylineDataTableSumRankNormalized.Count, entireDatabaseNormalized.Count,entireSkylineNormalized.Count);                            

                                        var caEntireDbNew = new List<double>();
                                        var caEntireSkylineNew = new List<double>();
                                        var caSampleSkylineNew = new List<double>();
                                        var caBestRankNew = new List<double>();
                                        var caSumRankNew = new List<double>();
                                     
                                        for (var ii = 0; ii < skylineAttributeColumns.Length; ii++)
                                        {
                                            int entireSkyline = aggregatedEntireSkylineBuckets.ContainsKey(ii)
                                                ? aggregatedEntireSkylineBuckets[ii].Count
                                                : 0;
                                            int sampleSkyline = aggregatedSampleBuckets.ContainsKey(ii)
                                                ? aggregatedSampleBuckets[ii].Count
                                                : 0;
                                            double entireSkylinePercent = (double) entireSkyline /
                                                                          entireSkylineNormalized.Count;
                                            double sampleSkylinePercent = (double) sampleSkyline /
                                                                          sampleSkylineNormalized.Count;
                                            int entireDb = aggregatedEntireDatabaseBuckets.ContainsKey(ii)
                                                ? aggregatedEntireDatabaseBuckets[ii].Count
                                                : 0;
                                            double entireDbPercent = (double) entireDb /
                                                                     entireDatabaseNormalized.Count;

                                            int bestRank = aggregatedBestRankBuckets.ContainsKey(ii)
                                                ? aggregatedBestRankBuckets[ii].Count
                                                : 0;
                                            int sumRank = aggregatedSumRankBuckets.ContainsKey(ii)
                                                ? aggregatedSumRankBuckets[ii].Count
                                                : 0;

                                            double bestRankPercent = (double) bestRank /
                                                                     entireSkylineDataTableBestRankNormalized.Count;
                                            double sumRankPercent = (double) sumRank /
                                                                    entireSkylineDataTableSumRankNormalized.Count;
                                            caEntireDbNew.Add(entireDbPercent);
                                            caEntireSkylineNew.Add(entireSkylinePercent);
                                            caSampleSkylineNew.Add(sampleSkylinePercent);
                                            caBestRankNew.Add(bestRankPercent);
                                            caSumRankNew.Add(sumRankPercent);
                                        }

                                        var caMedianEntireDbNew = new List<double>();
                                        var caMedianEntireSkylineNew = new List<double>();
                                        var caMedianSampleSkylineNew = new List<double>();
                                        var caMedianBestRankNew = new List<double>();
                                        var caMedianSumRankNew = new List<double>();

                                        for (var ii = 0; ii < skylineAttributeColumns.Length; ii++)
                                        {
                                            int entireSkyline = aggregatedEntireSkylineMedianBuckets.ContainsKey(ii)
                                                ? aggregatedEntireSkylineMedianBuckets[ii].Count
                                                : 0;
                                            int sampleSkyline = aggregatedSampleMedianBuckets.ContainsKey(ii)
                                                ? aggregatedSampleMedianBuckets[ii].Count
                                                : 0;
                                            double entireSkylinePercent = (double)entireSkyline /
                                                                          entireSkylineNormalized.Count;
                                            double sampleSkylinePercent = (double)sampleSkyline /
                                                                          sampleSkylineNormalized.Count;
                                            int entireDb = aggregatedEntireDatabaseMedianBuckets.ContainsKey(ii)
                                                ? aggregatedEntireDatabaseMedianBuckets[ii].Count
                                                : 0;
                                            double entireDbPercent = (double)entireDb /
                                                                     entireDatabaseNormalized.Count;

                                            int bestRank = aggregatedBestRankMedianBuckets.ContainsKey(ii)
                                                ? aggregatedBestRankMedianBuckets[ii].Count
                                                : 0;
                                            int sumRank = aggregatedSumRankMedianBuckets.ContainsKey(ii)
                                                ? aggregatedSumRankMedianBuckets[ii].Count
                                                : 0;
                                            
                                            double bestRankPercent = (double)bestRank /
                                                                     entireSkylineDataTableBestRankNormalized.Count;
                                            double sumRankPercent = (double)sumRank /
                                                                    entireSkylineDataTableSumRankNormalized.Count;
                                            caMedianEntireDbNew.Add(entireDbPercent);
                                            caMedianEntireSkylineNew.Add(entireSkylinePercent);
                                            caMedianSampleSkylineNew.Add(sampleSkylinePercent);
                                            caMedianBestRankNew.Add(bestRankPercent);
                                            caMedianSumRankNew.Add(sumRankPercent);
                                        }

                                        clusterAnalysisSampling[ClusterAnalysisSampling.EntireDb].Add(caEntireDbNew);
                                        clusterAnalysisSampling[ClusterAnalysisSampling.EntireSkyline].Add(
                                            caEntireSkylineNew);
                                        clusterAnalysisSampling[ClusterAnalysisSampling.SampleSkyline].Add(
                                            caSampleSkylineNew);
                                        clusterAnalysisSampling[ClusterAnalysisSampling.BestRank].Add(
                                            caBestRankNew);
                                        clusterAnalysisSampling[ClusterAnalysisSampling.SumRank].Add(
                                            caSumRankNew);

                                        clusterAnalysisMedianSampling[ClusterAnalysisSampling.EntireDb].Add(caMedianEntireDbNew);
                                        clusterAnalysisMedianSampling[ClusterAnalysisSampling.EntireSkyline].Add(
                                            caMedianEntireSkylineNew);
                                        clusterAnalysisMedianSampling[ClusterAnalysisSampling.SampleSkyline].Add(
                                            caMedianSampleSkylineNew);
                                        clusterAnalysisMedianSampling[ClusterAnalysisSampling.BestRank].Add(
                                            caMedianBestRankNew);
                                        clusterAnalysisMedianSampling[ClusterAnalysisSampling.SumRank].Add(
                                            caMedianSumRankNew);

                                        subspaceCount++;
                                    }

                                    Dictionary<ClusterAnalysisSampling, string> clusterAnalysisStrings =
                                        GetClusterAnalysisStrings(skylineAttributeColumns, clusterAnalysisSampling);
                                    Dictionary<ClusterAnalysisSampling, string> clusterAnalysisMedianStrings =
                                        GetClusterAnalysisStrings(skylineAttributeColumns, clusterAnalysisMedianSampling);
                                    Dictionary<ClusterAnalysisSampling, string> clusterAnalysisTopBucketsStrings =
                                       GetClusterAnalysisTopBucketsStrings(clusterAnalysisTopBucketsSampling);
                                    Dictionary<ClusterAnalysisSampling, string> clusterAnalysisMedianTopBucketsStrings =
                                        GetClusterAnalysisTopBucketsStrings(clusterAnalysisMedianTopBucketsSampling);

                                    var time = (long) (subspaceTime.Average() + .5);
                                    var objects = (long) (subspaceObjects.Average() + .5);
                                    var elapsed = (long) (subspaceTimeElapsed.Average() + .5);

                                    reportDimensions.Add(preferences.Count);
                                    reportSkylineSize.Add(objects);
                                    reportTimeTotal.Add(elapsed);
                                    reportTimeAlgorithm.Add(time);
                                    reportMinCorrelation.Add(minCorrelation);
                                    reportMaxCorrelation.Add(maxCorrelation);
                                    reportCardinality.Add(cardinality);

                                    var setCoverageSamplingSingle =
                                        new Dictionary<SkylineTypesSingleSampling, List<double>>
                                        {
                                            {SkylineTypesSingleSampling.Random, setCoverageSecondRandom},
                                            {SkylineTypesSingleSampling.Sample, setCoverageSample},
                                            {SkylineTypesSingleSampling.BestRank, setCoverageBestRank},
                                            {SkylineTypesSingleSampling.SumRank, setCoverageSumRank}
                                        };

                                    var representationErrorSamplingSingle =
                                        new Dictionary<SkylineTypesSingleSampling, List<double>>
                                        {
                                            {SkylineTypesSingleSampling.Random, representationErrorSecondRandom},
                                            {SkylineTypesSingleSampling.Sample, representationErrorSample},
                                            {SkylineTypesSingleSampling.BestRank, representationErrorBestRank},
                                            {SkylineTypesSingleSampling.SumRank, representationErrorSumRank}
                                        };
                                    
                                    var representationErrorSumSamplingSingle =
                                        new Dictionary<SkylineTypesSingleSampling, List<double>>
                                        {
                                            {SkylineTypesSingleSampling.Random, representationErrorSumSecondRandom},
                                            {SkylineTypesSingleSampling.Sample, representationErrorSumSample},
                                            {SkylineTypesSingleSampling.BestRank, representationErrorSumBestRank},
                                            {SkylineTypesSingleSampling.SumRank, representationErrorSumSumRank}
                                        };

                                    var dominatedObjectsCountSamplingSingle =
                                        new Dictionary<SkylineTypesSingleSampling, List<double>>()
                                        {
                                            {
                                                SkylineTypesSingleSampling.Random,
                                                dominatedObjectsCountSamplingSecondRandom
                                            },
                                            {
                                                SkylineTypesSingleSampling.Sample,
                                                dominatedObjectsCountSamplingSample
                                            },
                                            {
                                                SkylineTypesSingleSampling.BestRank,
                                                dominatedObjectsCountSamplingBestRank
                                            },
                                            {
                                                SkylineTypesSingleSampling.SumRank,
                                                dominatedObjectsCountSamplingSumRank
                                            }
                                        };                                   

                                    var dominatedObjectsOfBestObjectSamplingSingle =
                                        new Dictionary<SkylineTypesSingleSampling, List<double>>
                                        {
                                            {
                                                SkylineTypesSingleSampling.Random,
                                                dominatedObjectsOfBestObjectSamplingSecondRandom
                                            },
                                            {
                                                SkylineTypesSingleSampling.Sample,
                                                dominatedObjectsOfBestObjectSamplingSample
                                            },
                                            {
                                                SkylineTypesSingleSampling.BestRank,
                                                dominatedObjectsOfBestObjectSamplingBestRank
                                            },
                                            {
                                                SkylineTypesSingleSampling.SumRank,
                                                dominatedObjectsOfBestObjectSamplingSumRank
                                            }
                                        };

                                    AddToReportsSampling(reportsSamplingLong, subspaceObjects, subspaceTime,
                                        reportsSamplingDouble);
                                    AddToSetCoverageSampling(setCoverageSampling, setCoverageSamplingSingle);
                                    AddToSetCoverageSampling(representationErrorSampling,
                                        representationErrorSamplingSingle);
                                    AddToSetCoverageSampling(representationErrorSumSampling,
                                        representationErrorSumSamplingSingle);
                                    AddToSetCoverageSampling(dominatedObjectsCountSampling,
                                        dominatedObjectsCountSamplingSingle);
                                    AddToSetCoverageSampling(dominatedObjectsOfBestObjectSampling,
                                        dominatedObjectsOfBestObjectSamplingSingle);

                                    string strLine = FormatLineStringSample(strPreferenceSet, strTrial,
                                        preferences.Count, objects,
                                        elapsed, time, subspaceTime.Min(), subspaceTime.Max(),
                                        MyMathematic.GetSampleVariance(subspaceTime),
                                        MyMathematic.GetSampleStdDeviation(subspaceTime), Mathematic.Median(subspaceTime), Mathematic.LowerQuartile(subspaceTime), Mathematic.UpperQuartile(subspaceTime), subspaceObjects.Min(),
                                        subspaceObjects.Max(), MyMathematic.GetSampleVariance(subspaceObjects),
                                        MyMathematic.GetSampleStdDeviation(subspaceObjects), Mathematic.Median(subspaceObjects), Mathematic.LowerQuartile(subspaceObjects), Mathematic.UpperQuartile(subspaceObjects),
                                        setCoverageSamplingSingle, representationErrorSamplingSingle,
                                        representationErrorSumSamplingSingle, dominatedObjectsCountSamplingSingle,
                                        dominatedObjectsOfBestObjectSamplingSingle,
                                        clusterAnalysisStrings, clusterAnalysisMedianStrings, clusterAnalysisTopBucketsStrings, clusterAnalysisMedianTopBucketsStrings, minCorrelation,                                   
                                        maxCorrelation, cardinality);

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
                        AddSummarySample(sb, strSeparatorLine, reportDimensions, reportSkylineSize, reportTimeTotal, reportTimeAlgorithm, reportsSamplingLong, reportsSamplingDouble, setCoverageSampling, representationErrorSampling, representationErrorSumSampling, dominatedObjectsCountSampling, dominatedObjectsOfBestObjectSampling, reportMinCorrelation, reportMaxCorrelation, reportCardinality);
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

        private static void FillTopBucketsSampling(Dictionary<ClusterAnalysisSampling, Dictionary<BigInteger, List<double>>> clusterAnalysisTopBuckets, ClusterAnalysisSampling sampleSkylineType, IReadOnlyDictionary<BigInteger, List<IReadOnlyDictionary<long, object[]>>> skylineTypeBuckets, int skylineCount, int entireDbCount, int entireSkylineCount)
        {            
            List<KeyValuePair<BigInteger, List<IReadOnlyDictionary<long, object[]>>>> sortedTop5 =
                skylineTypeBuckets.OrderByDescending(l => l.Value.Count).ThenBy(l => l.Key).Take(5).ToList(); // track top 5 buckets

            foreach (KeyValuePair<BigInteger, List<IReadOnlyDictionary<long, object[]>>> skylineTypeBucket in sortedTop5)
            {
                if (!clusterAnalysisTopBuckets[sampleSkylineType].ContainsKey(skylineTypeBucket.Key))
                {
                    clusterAnalysisTopBuckets[sampleSkylineType].Add(skylineTypeBucket.Key, new List<double>());
                }
                double percent = (double)skylineTypeBucket.Value.Count / skylineCount;
                clusterAnalysisTopBuckets[sampleSkylineType][skylineTypeBucket.Key].Add(percent);
            }

            foreach (KeyValuePair<BigInteger, List<double>> entireDbBucket in clusterAnalysisTopBuckets[ClusterAnalysisSampling.EntireDb]) // additionally track top 5 buckets of entire db
            {
                if (!clusterAnalysisTopBuckets[sampleSkylineType].ContainsKey(entireDbBucket.Key))
                {
                    clusterAnalysisTopBuckets[sampleSkylineType].Add(entireDbBucket.Key, new List<double>());
                }

                if (!skylineTypeBuckets.ContainsKey(entireDbBucket.Key)) // not contained => percentage = 0
                {
                    clusterAnalysisTopBuckets[sampleSkylineType][entireDbBucket.Key].Add(0);
                }
                else if (sortedTop5.All(item => item.Key != entireDbBucket.Key)) // else: already added in previous foreach => no need to add again
                {
                    double percent = (double) skylineTypeBuckets[entireDbBucket.Key].Count / entireDbCount;
                    clusterAnalysisTopBuckets[sampleSkylineType][entireDbBucket.Key].Add(percent);
                }
            }

            foreach (KeyValuePair<BigInteger, List<double>> entireSkylineBucket in clusterAnalysisTopBuckets[ClusterAnalysisSampling.EntireSkyline]) // additionally track top 5 buckets of entire skyline
            {
                if (!clusterAnalysisTopBuckets[sampleSkylineType].ContainsKey(entireSkylineBucket.Key))
                {
                    clusterAnalysisTopBuckets[sampleSkylineType].Add(entireSkylineBucket.Key, new List<double>());
                }

                if (!skylineTypeBuckets.ContainsKey(entireSkylineBucket.Key)) // not contained => percentage = 0
                {
                    clusterAnalysisTopBuckets[sampleSkylineType][entireSkylineBucket.Key].Add(0);
                }
                else if (sortedTop5.All(item => item.Key != entireSkylineBucket.Key)) // else: already added in previous foreach => no need to add again
                {
                    double percent = (double)skylineTypeBuckets[entireSkylineBucket.Key].Count / entireSkylineCount;
                    clusterAnalysisTopBuckets[sampleSkylineType][entireSkylineBucket.Key].Add(percent);
                }
            }
        }

        private IReadOnlyDictionary<long, object[]> GetReducedSkyline(IReadOnlyDictionary<long, object[]> baseSkylineNormalized, IReadOnlyDictionary<long, object[]> subtractSkylineNormalized)
        {
            var ret = new Dictionary<long, object[]>();

            foreach (KeyValuePair<long, object[]> baseObject in baseSkylineNormalized)
            {
                if (!subtractSkylineNormalized.ContainsKey(baseObject.Key))
                {
                    ret.Add(baseObject.Key, baseObject.Value);
                }
            }

            return new ReadOnlyDictionary<long, object[]>(ret);
        }

        private static Dictionary<ClusterAnalysisSampling, string> GetClusterAnalysisTopBucketsStrings(Dictionary<ClusterAnalysisSampling, Dictionary<BigInteger, List<double>>> clusterAnalysisTopBuckets)
        {
            var clusterAnalysisStrings =
              new Dictionary<ClusterAnalysisSampling, string>()
                {
                    {ClusterAnalysisSampling.EntireDb, ""},
                    {ClusterAnalysisSampling.EntireSkyline, ""},
                    {ClusterAnalysisSampling.SampleSkyline, ""},
                    {ClusterAnalysisSampling.BestRank, ""},
                    {ClusterAnalysisSampling.SumRank, ""}
                };

            int subspacesCount = clusterAnalysisTopBuckets[ClusterAnalysisSampling.EntireDb].Values.First().Count;

            IOrderedEnumerable<KeyValuePair<BigInteger, List<double>>> allEntireDbBuckets = clusterAnalysisTopBuckets[ClusterAnalysisSampling.EntireDb].OrderByDescending(
                l => l.Value.Sum() / subspacesCount).ThenBy(l => l.Key);
            IOrderedEnumerable<KeyValuePair<BigInteger, List<double>>> allEntireSkylineBuckets = clusterAnalysisTopBuckets[ClusterAnalysisSampling.EntireSkyline].OrderByDescending(
               l => l.Value.Sum() / subspacesCount).ThenBy(l => l.Key);

            foreach (
                ClusterAnalysisSampling clusterAnalysisType in
                    Enum.GetValues(typeof (ClusterAnalysisSampling)).Cast<ClusterAnalysisSampling>())
            {
                foreach (KeyValuePair<BigInteger, List<double>> bucket in allEntireDbBuckets)
                {
                    double percent = clusterAnalysisTopBuckets[clusterAnalysisType][bucket.Key].Sum() / subspacesCount;
                    clusterAnalysisStrings[clusterAnalysisType] += "EB-" + bucket.Key + ":" +
                                                                   string.Format("{0:0.00},", percent * 100);
                }

                clusterAnalysisStrings[clusterAnalysisType] = clusterAnalysisStrings[clusterAnalysisType].TrimEnd(',');

                if (clusterAnalysisType != ClusterAnalysisSampling.EntireDb)
                {
                    clusterAnalysisStrings[clusterAnalysisType] += ";";

                    foreach (KeyValuePair<BigInteger, List<double>> bucket in allEntireSkylineBuckets.Take(5))
                    {
                        double percent = clusterAnalysisTopBuckets[clusterAnalysisType][bucket.Key].Sum() /
                                         subspacesCount;
                        clusterAnalysisStrings[clusterAnalysisType] += "ESB-" + bucket.Key + ":" +
                                                                       string.Format("{0:0.00},", percent * 100);
                    }

                    clusterAnalysisStrings[clusterAnalysisType] =
                        clusterAnalysisStrings[clusterAnalysisType].TrimEnd(',');
                    if (clusterAnalysisType != ClusterAnalysisSampling.EntireSkyline)
                    {
                        clusterAnalysisStrings[clusterAnalysisType] += ";";

                        foreach (
                            KeyValuePair<BigInteger, List<double>> bucket in
                                clusterAnalysisTopBuckets[clusterAnalysisType].OrderByDescending(
                                    l => l.Value.Sum() / subspacesCount).ThenBy(l => l.Key).Take(5))
                        {
                            double percent = clusterAnalysisTopBuckets[clusterAnalysisType][bucket.Key].Sum() /
                                             subspacesCount;
                            clusterAnalysisStrings[clusterAnalysisType] += "SB-" + bucket.Key + ":" +
                                                                           string.Format("{0:0.00},", percent * 100);
                        }

                        clusterAnalysisStrings[clusterAnalysisType] =
                            clusterAnalysisStrings[clusterAnalysisType].TrimEnd(',');
                    }
                }
            }

            return clusterAnalysisStrings;
        }

        private static Dictionary<ClusterAnalysisSampling, string> GetClusterAnalysisStrings(int[] skylineAttributeColumns, Dictionary<ClusterAnalysisSampling, List<List<double>>> clusterAnalysisSampling)
        {
            var clusterAnalysisAverages =
                new Dictionary<ClusterAnalysisSampling, List<double>>()
                {
                    {ClusterAnalysisSampling.EntireDb, new List<double>()},
                    {ClusterAnalysisSampling.EntireSkyline, new List<double>()},
                    {ClusterAnalysisSampling.SampleSkyline, new List<double>()},
                    {ClusterAnalysisSampling.BestRank, new List<double>()},
                    {ClusterAnalysisSampling.SumRank, new List<double>()}
                };

            var clusterAnalysisStrings =
                new Dictionary<ClusterAnalysisSampling, string>()
                {
                    {ClusterAnalysisSampling.EntireDb, ""},
                    {ClusterAnalysisSampling.EntireSkyline, ""},
                    {ClusterAnalysisSampling.SampleSkyline, ""},
                    {ClusterAnalysisSampling.BestRank, ""},
                    {ClusterAnalysisSampling.SumRank, ""}
                };

            for (var bucket = 0; bucket < skylineAttributeColumns.Length; bucket++)
            {
                clusterAnalysisAverages[ClusterAnalysisSampling.EntireDb].Add(0);
                clusterAnalysisAverages[ClusterAnalysisSampling.EntireSkyline].Add(0);
                clusterAnalysisAverages[ClusterAnalysisSampling.SampleSkyline].Add(0);
                clusterAnalysisAverages[ClusterAnalysisSampling.BestRank].Add(0);
                clusterAnalysisAverages[ClusterAnalysisSampling.SumRank].Add(0);
            }

            foreach (
                ClusterAnalysisSampling clusterAnalysisType in
                    Enum.GetValues(typeof (ClusterAnalysisSampling)).Cast<ClusterAnalysisSampling>())
            {
                foreach (List<double> row in clusterAnalysisSampling[clusterAnalysisType])
                {
                    for (var bucket = 0; bucket < row.Count; bucket++)
                    {
                        clusterAnalysisAverages[clusterAnalysisType][bucket] += row[bucket];
                    }
                }

                for (var bucket = 0; bucket < skylineAttributeColumns.Length; bucket++)
                {
                    clusterAnalysisAverages[clusterAnalysisType][bucket] /= clusterAnalysisSampling[clusterAnalysisType].Count;
                }

                foreach (double averageValue in clusterAnalysisAverages[clusterAnalysisType])
                {
                    clusterAnalysisStrings[clusterAnalysisType] += string.Format("{0:0.00};", averageValue * 100);
                }

                clusterAnalysisStrings[clusterAnalysisType] = clusterAnalysisStrings[clusterAnalysisType].TrimEnd(';');
            }
            return clusterAnalysisStrings;
        }

        private static IReadOnlyDictionary<long, object[]> GetEntireSkylineDataTableRankNormalized(DataTable entireSkyline, List<long[]> skylineValues, int[] skylineAttributeColumns, int numberOfRecords, int sortType, out  IReadOnlyDictionary<long, object[]> skylineDatabase)
        {
            var sortedDataTable=new DataTable();

              if (sortType == 1)
              {
                  sortedDataTable = prefSQL.SQLSkyline.Helper.SortByRank(entireSkyline,skylineValues);
              } 
                else if (sortType == 2)
                {
                     sortedDataTable = prefSQL.SQLSkyline.Helper.SortBySum(entireSkyline,skylineValues);
                }

              prefSQL.SQLSkyline.Helper.GetAmountOfTuples(sortedDataTable, numberOfRecords);

            skylineDatabase = prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(sortedDataTable, 0);      
      
              IReadOnlyDictionary<long, object[]> sortedDataTableNormalized =
                prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(sortedDataTable, 0);
            SkylineSamplingHelper.NormalizeColumns(sortedDataTableNormalized, skylineAttributeColumns);
            return sortedDataTableNormalized;
        }

        private List<IEnumerable<CLRSafeHashSet<int>>> ProduceSubspaces(ArrayList preferences)
        {
            var randomSubspacesProducer = new RandomSkylineSamplingSubspacesProducer
            {
                AllPreferencesCount = preferences.Count,
                SubspacesCount = SamplingSubspacesCount,
                SubspaceDimension = SamplingSubspaceDimension
            };

            var producedSubspaces = new List<IEnumerable<CLRSafeHashSet<int>>>();
            for (var ii = 0; ii < SamplingSamplesCount; ii++)
            {
                producedSubspaces.Add(randomSubspacesProducer.GetSubspaces());
            }
            return producedSubspaces;
        }

        private static IReadOnlyDictionary<long, object[]> GetEntireDatabaseNormalized(SQLCommon parser, string strSQL, int[] skylineAttributeColumns, out DataTable dtEntire)
        {
            DbProviderFactory factory = DbProviderFactories.GetFactory(Helper.ProviderName);

            // use the factory object to create Data access objects.
            DbConnection connection = factory.CreateConnection();
            // will return the connection object (i.e. SqlConnection ...)
            connection.ConnectionString = Helper.ConnectionString;

            dtEntire = new DataTable();

            connection.Open();

            DbDataAdapter dap = factory.CreateDataAdapter();
            DbCommand selectCommand = connection.CreateCommand();
            selectCommand.CommandTimeout = 0; //infinite timeout

            string strQueryEntire;
            string operatorsEntire;
            int numberOfRecordsEntire;
            string[] parameterEntire;

            string ansiSqlEntire =
                parser.GetAnsiSqlFromPrefSqlModel(
                    parser.GetPrefSqlModelFromPreferenceSql(strSQL));
            prefSQL.SQLParser.Helper.DetermineParameters(ansiSqlEntire, out parameterEntire,
                out strQueryEntire, out operatorsEntire,
                out numberOfRecordsEntire);

            selectCommand.CommandText = strQueryEntire;
            dap.SelectCommand = selectCommand;
            dtEntire = new DataTable();

            dap.Fill(dtEntire);

            for (var ii = 0; ii < skylineAttributeColumns.Length; ii++)
            {
                dtEntire.Columns.RemoveAt(0);
            }

            IReadOnlyDictionary<long, object[]> entireDatabaseNormalized =
                                         prefSQL.SQLSkyline.Helper.GetDatabaseAccessibleByUniqueId(dtEntire, 0);
            SkylineSamplingHelper.NormalizeColumns(entireDatabaseNormalized, skylineAttributeColumns);

            return entireDatabaseNormalized;
        }

        private static void AddToSetCoverageSampling(IReadOnlyDictionary<SkylineTypesSampling, List<double>> setCoverageSampling, IReadOnlyDictionary<SkylineTypesSingleSampling, List<double>> setCoverageSingleSampling)
        {
            setCoverageSampling[SkylineTypesSampling.RandomAvg].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.Random].Average());
            setCoverageSampling[SkylineTypesSampling.RandomMin].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.Random].Min());
            setCoverageSampling[SkylineTypesSampling.RandomMax].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.Random].Max());
            setCoverageSampling[SkylineTypesSampling.RandomVar].Add(MyMathematic.GetSampleVariance(setCoverageSingleSampling[SkylineTypesSingleSampling.Random]));
            setCoverageSampling[SkylineTypesSampling.RandomStdDev].Add(MyMathematic.GetSampleStdDeviation(setCoverageSingleSampling[SkylineTypesSingleSampling.Random]));
            setCoverageSampling[SkylineTypesSampling.RandomMed].Add(Mathematic.Median(setCoverageSingleSampling[SkylineTypesSingleSampling.Random]));
            setCoverageSampling[SkylineTypesSampling.RandomQ1].Add(Mathematic.LowerQuartile(setCoverageSingleSampling[SkylineTypesSingleSampling.Random]));
            setCoverageSampling[SkylineTypesSampling.RandomQ3].Add(Mathematic.UpperQuartile(setCoverageSingleSampling[SkylineTypesSingleSampling.Random]));
           
            setCoverageSampling[SkylineTypesSampling.SampleAvg].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.Sample].Average());
            setCoverageSampling[SkylineTypesSampling.SampleMin].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.Sample].Min());
            setCoverageSampling[SkylineTypesSampling.SampleMax].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.Sample].Max());
            setCoverageSampling[SkylineTypesSampling.SampleVar].Add(MyMathematic.GetSampleVariance(setCoverageSingleSampling[SkylineTypesSingleSampling.Sample]));
            setCoverageSampling[SkylineTypesSampling.SampleStdDev].Add(MyMathematic.GetSampleStdDeviation(setCoverageSingleSampling[SkylineTypesSingleSampling.Sample]));
            setCoverageSampling[SkylineTypesSampling.SampleMed].Add(Mathematic.Median(setCoverageSingleSampling[SkylineTypesSingleSampling.Sample]));
            setCoverageSampling[SkylineTypesSampling.SampleQ1].Add(Mathematic.LowerQuartile(setCoverageSingleSampling[SkylineTypesSingleSampling.Sample]));
            setCoverageSampling[SkylineTypesSampling.SampleQ3].Add(Mathematic.UpperQuartile(setCoverageSingleSampling[SkylineTypesSingleSampling.Sample]));

            setCoverageSampling[SkylineTypesSampling.BestRankAvg].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.BestRank].Average());
            setCoverageSampling[SkylineTypesSampling.BestRankMin].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.BestRank].Min());
            setCoverageSampling[SkylineTypesSampling.BestRankMax].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.BestRank].Max());
            setCoverageSampling[SkylineTypesSampling.BestRankVar].Add(MyMathematic.GetSampleVariance(setCoverageSingleSampling[SkylineTypesSingleSampling.BestRank]));
            setCoverageSampling[SkylineTypesSampling.BestRankStdDev].Add(MyMathematic.GetSampleStdDeviation(setCoverageSingleSampling[SkylineTypesSingleSampling.BestRank]));
            setCoverageSampling[SkylineTypesSampling.BestRankMed].Add(Mathematic.Median(setCoverageSingleSampling[SkylineTypesSingleSampling.BestRank]));
            setCoverageSampling[SkylineTypesSampling.BestRankQ1].Add(Mathematic.LowerQuartile(setCoverageSingleSampling[SkylineTypesSingleSampling.BestRank]));
            setCoverageSampling[SkylineTypesSampling.BestRankQ3].Add(Mathematic.UpperQuartile(setCoverageSingleSampling[SkylineTypesSingleSampling.BestRank]));
            
            setCoverageSampling[SkylineTypesSampling.SumRankAvg].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.SumRank].Average());
            setCoverageSampling[SkylineTypesSampling.SumRankMin].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.SumRank].Min());
            setCoverageSampling[SkylineTypesSampling.SumRankMax].Add(setCoverageSingleSampling[SkylineTypesSingleSampling.SumRank].Max());
            setCoverageSampling[SkylineTypesSampling.SumRankVar].Add(MyMathematic.GetSampleVariance(setCoverageSingleSampling[SkylineTypesSingleSampling.SumRank]));
            setCoverageSampling[SkylineTypesSampling.SumRankStdDev].Add(MyMathematic.GetSampleStdDeviation(setCoverageSingleSampling[SkylineTypesSingleSampling.SumRank]));
            setCoverageSampling[SkylineTypesSampling.SumRankMed].Add(Mathematic.Median(setCoverageSingleSampling[SkylineTypesSingleSampling.SumRank]));
            setCoverageSampling[SkylineTypesSampling.SumRankQ1].Add(Mathematic.LowerQuartile(setCoverageSingleSampling[SkylineTypesSingleSampling.SumRank]));
            setCoverageSampling[SkylineTypesSampling.SumRankQ3].Add(Mathematic.UpperQuartile(setCoverageSingleSampling[SkylineTypesSingleSampling.SumRank]));
        }

        private static void AddToReportsSampling(IReadOnlyDictionary<ReportsSampling, List<long>> reportsSamplingLong, List<long> subspaceObjects, List<long> subspaceTime,
            IReadOnlyDictionary<ReportsSampling, List<double>> reportsSamplingDouble)
        {
            reportsSamplingLong[ReportsSampling.SizeMin].Add(subspaceObjects.Min());
            reportsSamplingLong[ReportsSampling.TimeMin].Add(subspaceTime.Min());
            reportsSamplingLong[ReportsSampling.SizeMax].Add(subspaceObjects.Max());
            reportsSamplingLong[ReportsSampling.TimeMax].Add(subspaceTime.Max());
            reportsSamplingLong[ReportsSampling.SizeMed].Add(Mathematic.Median(subspaceObjects));
            reportsSamplingLong[ReportsSampling.TimeMed].Add(Mathematic.Median(subspaceTime));
            reportsSamplingLong[ReportsSampling.SizeQ1].Add(Mathematic.LowerQuartile(subspaceObjects));
            reportsSamplingLong[ReportsSampling.TimeQ1].Add(Mathematic.LowerQuartile(subspaceTime));
            reportsSamplingLong[ReportsSampling.SizeQ3].Add(Mathematic.UpperQuartile(subspaceObjects));
            reportsSamplingLong[ReportsSampling.TimeQ3].Add(Mathematic.UpperQuartile(subspaceTime));
            reportsSamplingDouble[ReportsSampling.SizeVar].Add(MyMathematic.GetSampleVariance(subspaceObjects));
            reportsSamplingDouble[ReportsSampling.TimeVar].Add(MyMathematic.GetSampleVariance(subspaceTime));
            reportsSamplingDouble[ReportsSampling.SizeStdDev].Add(MyMathematic.GetSampleStdDeviation(subspaceObjects));
            reportsSamplingDouble[ReportsSampling.TimeStdDev].Add(MyMathematic.GetSampleStdDeviation(subspaceTime));
        }

        private static void InitClusterAnalysisSamplingDataStructures(out Dictionary<ClusterAnalysisSampling, List<List<double>>> clusterAnalysisSampling)
        {
            clusterAnalysisSampling = new Dictionary<ClusterAnalysisSampling, List<List<double>>>()
            {
                {ClusterAnalysisSampling.EntireDb, new List<List<double>>()},
                {ClusterAnalysisSampling.EntireSkyline, new List<List<double>>()},
                {ClusterAnalysisSampling.SampleSkyline, new List<List<double>>()},
                {ClusterAnalysisSampling.BestRank, new List<List<double>>()},
                {ClusterAnalysisSampling.SumRank, new List<List<double>>()}
            };
        }

        private static void InitClusterAnalysisTopBucketsSamplingDataStructures(out Dictionary<ClusterAnalysisSampling, Dictionary<BigInteger, List<double>>> clusterAnalysisTopBucketsSampling)
        {
            clusterAnalysisTopBucketsSampling = new Dictionary<ClusterAnalysisSampling, Dictionary<BigInteger, List<double>>>()
            {
                {ClusterAnalysisSampling.EntireDb, new Dictionary<BigInteger, List<double>>()},
                {ClusterAnalysisSampling.EntireSkyline, new Dictionary<BigInteger, List<double>>()},
                {ClusterAnalysisSampling.SampleSkyline, new Dictionary<BigInteger, List<double>>()},
                {ClusterAnalysisSampling.BestRank, new Dictionary<BigInteger, List<double>>()},
                {ClusterAnalysisSampling.SumRank, new Dictionary<BigInteger, List<double>>()}
            };
        }

        private static void InitSamplingDataStructures(out Dictionary<ReportsSampling, List<long>> reportsSamplingLong, out Dictionary<ReportsSampling, List<double>> reportsSamplingDouble, out Dictionary<SkylineTypesSampling, List<double>> setCoverageSampling, out Dictionary<SkylineTypesSampling, List<double>> representationErrorSampling, out Dictionary<SkylineTypesSampling, List<double>> representationErrorSumSampling, out Dictionary<SkylineTypesSampling, List<double>> dominatedObjectsCountSampling, out Dictionary<SkylineTypesSampling, List<double>> dominatedObjectsByBestObjectSampling, out Dictionary<ClusterAnalysisSampling, List<List<double>>> clusterAnalysisSampling, out Dictionary<ClusterAnalysisSampling, List<List<double>>> clusterAnalysisMedianSampling, out Dictionary<ClusterAnalysisSampling, Dictionary<BigInteger, List<double>>> clusterAnalysisTopBucketsSampling, out Dictionary<ClusterAnalysisSampling, Dictionary<BigInteger, List<double>>> clusterAnalysisMedianTopBucketsSampling)
        {
            reportsSamplingLong = new Dictionary<ReportsSampling, List<long>>
            {
                {ReportsSampling.SizeMin, new List<long>()},
                {ReportsSampling.TimeMin, new List<long>()},
                {ReportsSampling.SizeMax, new List<long>()},
                {ReportsSampling.TimeMax, new List<long>()},
                {ReportsSampling.SizeMed, new List<long>()},
                {ReportsSampling.TimeMed, new List<long>()},
                {ReportsSampling.SizeQ1, new List<long>()},
                {ReportsSampling.TimeQ1, new List<long>()},
                {ReportsSampling.SizeQ3, new List<long>()},
                {ReportsSampling.TimeQ3, new List<long>()}
            };
            reportsSamplingDouble = new Dictionary<ReportsSampling, List<double>>
            {
                {ReportsSampling.SizeVar, new List<double>()},
                {ReportsSampling.TimeVar, new List<double>()},
                {ReportsSampling.SizeStdDev, new List<double>()},
                {ReportsSampling.TimeStdDev, new List<double>()}
            };

            setCoverageSampling = new Dictionary<SkylineTypesSampling, List<double>>();
            representationErrorSampling = new Dictionary<SkylineTypesSampling, List<double>>();
            representationErrorSumSampling = new Dictionary<SkylineTypesSampling, List<double>>();
            dominatedObjectsCountSampling = new Dictionary<SkylineTypesSampling, List<double>>();
            dominatedObjectsByBestObjectSampling = new Dictionary<SkylineTypesSampling, List<double>>();

            foreach (
                SkylineTypesSampling skylineTypesSamplingType in
                    Enum.GetValues(typeof (SkylineTypesSampling)).Cast<SkylineTypesSampling>())
            {
                setCoverageSampling.Add(skylineTypesSamplingType,new List<double>());
            }

            foreach (
            SkylineTypesSampling skylineTypesSamplingType in
                Enum.GetValues(typeof(SkylineTypesSampling)).Cast<SkylineTypesSampling>())
            {
                representationErrorSampling.Add(skylineTypesSamplingType, new List<double>());
            }

            foreach (
            SkylineTypesSampling skylineTypesSamplingType in
                Enum.GetValues(typeof(SkylineTypesSampling)).Cast<SkylineTypesSampling>())
            {
                representationErrorSumSampling.Add(skylineTypesSamplingType, new List<double>());
            }

            foreach (
            SkylineTypesSampling skylineTypesSamplingType in
                Enum.GetValues(typeof(SkylineTypesSampling)).Cast<SkylineTypesSampling>())
            {
                dominatedObjectsCountSampling.Add(skylineTypesSamplingType, new List<double>());
            }

            foreach (
            SkylineTypesSampling skylineTypesSamplingType in
                Enum.GetValues(typeof(SkylineTypesSampling)).Cast<SkylineTypesSampling>())
            {
                dominatedObjectsByBestObjectSampling.Add(skylineTypesSamplingType, new List<double>());
            }    

            InitClusterAnalysisSamplingDataStructures(out clusterAnalysisSampling); 
            InitClusterAnalysisSamplingDataStructures(out clusterAnalysisMedianSampling);
            InitClusterAnalysisTopBucketsSamplingDataStructures(out clusterAnalysisTopBucketsSampling);
            InitClusterAnalysisTopBucketsSamplingDataStructures(out clusterAnalysisMedianTopBucketsSampling);
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

        private void AddSummarySample(StringBuilder sb, String strSeparatorLine, List<long> reportDimensions,
            List<long> reportSkylineSize, List<long> reportTimeTotal, List<long> reportTimeAlgorithm, IDictionary<ReportsSampling, List<long>> rsl, IDictionary<ReportsSampling, List<double>> rsd, Dictionary<SkylineTypesSampling, List<double>> setCoverageSampling, Dictionary<SkylineTypesSampling, List<double>> representationErrorSampling, Dictionary<SkylineTypesSampling, List<double>> representationErrorSumSampling, Dictionary<SkylineTypesSampling, List<double>> dominatedObjectsCountSampling, Dictionary<SkylineTypesSampling, List<double>> dominatedObjectsOfBestObjectSampling, List<double> reportMinCorrelation, List<double> reportMaxCorrelation, List<double> reportCardinality)
        {
            //Separator Line
            Debug.WriteLine(strSeparatorLine);
            sb.AppendLine(strSeparatorLine);

            string[] setCoverageSamplingAverage = GetSummaryAverage(setCoverageSampling);
             string[] representationErrorSamplingAverage = GetSummaryAverage(representationErrorSampling);
             string[] representationErrorSumSamplingAverage = GetSummaryAverage(representationErrorSumSampling);
             string[] dominatedObjectsCountSamplingAverage = GetSummaryAverage(dominatedObjectsCountSampling);
             string[] dominatedObjectsOfBestObjectSamplingAverage = GetSummaryAverage(dominatedObjectsOfBestObjectSampling);

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

        private static string FormatLineStringSample(char paddingChar, string strTitle, string strTrial,
            string strDimension, string strSkyline, string strTimeTotal, string strTimeAlgo, string minTime,
            string maxTime, string varianceTime, string stdDevTime, string medTime, string q1Time, string q3Time, string minSize, string maxSize, string varianceSize,
            string stdDevSize, string medSize, string q1Size, string q3Size, string[] setCoverageSampling,
            string[] representationErrorSampling,
            string[] representationErrorSumSampling,
            string[] dominatedObjectsCountSampling,
            string[] dominatedObjectsOfBestObjectSampling, string[] clusterAnalysisStrings, string[] clusterAnalysisMedianStrings, string[] clusterAnalysisTopBucketsStrings, string[] clusterAnalysisMedianTopBucketsStrings, string strMinCorrelation,
            string strMaxCorrelation, string strCardinality)
        {
               var sb=new StringBuilder();

            sb.Append(strTitle.PadLeft(19, paddingChar));
            sb.Append("|");
            sb.Append(strTrial.PadLeft(11, paddingChar));
            sb.Append("|");
            sb.Append(strDimension.PadLeft(10, paddingChar));
            sb.Append("|");
            sb.Append(strSkyline.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strTimeTotal.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strTimeAlgo.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(minTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(maxTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(varianceTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(stdDevTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(medTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(q1Time.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(q3Time.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(minSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(maxSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(varianceSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(stdDevSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(medSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(q1Size.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(q3Size.PadLeft(20, paddingChar));
            sb.Append("|");
            foreach (string s in setCoverageSampling)
            {
                sb.Append(s.PadLeft(20, paddingChar));
                sb.Append("|");
            }
            foreach (string s in representationErrorSampling)
            {
                sb.Append(s.PadLeft(20, paddingChar));
                sb.Append("|");
            }
            foreach (string s in representationErrorSumSampling)
            {
                sb.Append(s.PadLeft(20, paddingChar));
                sb.Append("|");
            }
            foreach (string s in dominatedObjectsCountSampling)
            {
                sb.Append(s.PadLeft(20, paddingChar));
                sb.Append("|");
            }
            foreach (string s in dominatedObjectsOfBestObjectSampling)
            {
                sb.Append(s.PadLeft(20, paddingChar));
                sb.Append("|");
            }
            foreach (string s in clusterAnalysisStrings)
            {
                sb.Append(s.PadLeft(130, paddingChar));
                sb.Append("|");
            }
            foreach (string s in clusterAnalysisMedianStrings)
            {
                sb.Append(s.PadLeft(130, paddingChar));
                sb.Append("|");
            }
            foreach (string s in clusterAnalysisTopBucketsStrings)
            {
                sb.Append(s.PadLeft(250, paddingChar));
                sb.Append("|");
            }
            foreach (string s in clusterAnalysisMedianTopBucketsStrings)
            {
                sb.Append(s.PadLeft(250, paddingChar));
                sb.Append("|");
            }      
            sb.Append(strMinCorrelation.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strMaxCorrelation.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strCardinality.PadLeft(25, paddingChar));
            sb.Append("|");

            return sb.ToString();
        }

        private static string FormatLineStringSample(char paddingChar, string strTitle, string strTrial, string strDimension, string strSkyline, string strTimeTotal, string strTimeAlgo, string minTime, string maxTime, string varianceTime, string stdDevTime, string medTime, string q1Time, string q3Time, string minSize, string maxSize, string varianceSize, string stdDevSize, string medSize, string q1Size, string q3Size, Dictionary<SkylineTypesSingleSampling, List<double>> setCoverageSampling, Dictionary<SkylineTypesSingleSampling, List<double>> representationErrorSampling, Dictionary<SkylineTypesSingleSampling, List<double>> representationErrorSumSampling, Dictionary<SkylineTypesSingleSampling, List<double>> dominatedObjectsCountSampling, Dictionary<SkylineTypesSingleSampling, List<double>> dominatedObjectsOfBestObjectSampling, Dictionary<ClusterAnalysisSampling, string> clusterAnalysisStrings, Dictionary<ClusterAnalysisSampling, string> clusterAnalysisMedianStrings, Dictionary<ClusterAnalysisSampling, string> clusterAnalysisTopBucketsStrings, Dictionary<ClusterAnalysisSampling, string> clusterAnalysisMedianTopBucketsStrings, string strMinCorrelation, string strMaxCorrelation, string strCardinality)
        {
            var sb=new StringBuilder();

            sb.Append(strTitle.PadLeft(19, paddingChar));
            sb.Append("|");
            sb.Append(strTrial.PadLeft(11, paddingChar));
            sb.Append("|");
            sb.Append(strDimension.PadLeft(10, paddingChar));
            sb.Append("|");
            sb.Append(strSkyline.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strTimeTotal.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strTimeAlgo.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(minTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(maxTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(varianceTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(stdDevTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(medTime.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(q1Time.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(q3Time.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(minSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(maxSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(varianceSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(stdDevSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(medSize.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(q1Size.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(q3Size.PadLeft(20, paddingChar));
            sb.Append("|");
            AppendValues(sb, paddingChar, setCoverageSampling);
            AppendValues(sb, paddingChar, representationErrorSampling);
            AppendValues(sb, paddingChar, representationErrorSumSampling);
            AppendValues(sb, paddingChar, dominatedObjectsCountSampling);
            AppendValues(sb, paddingChar, dominatedObjectsOfBestObjectSampling);
            AppendClusterAnalysisValues(sb, paddingChar, 130, clusterAnalysisStrings);
            AppendClusterAnalysisValues(sb, paddingChar, 130, clusterAnalysisMedianStrings);
            AppendClusterAnalysisValues(sb, paddingChar, 250, clusterAnalysisTopBucketsStrings);
            AppendClusterAnalysisValues(sb, paddingChar, 250, clusterAnalysisMedianTopBucketsStrings); 
            sb.Append(strMinCorrelation.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strMaxCorrelation.PadLeft(20, paddingChar));
            sb.Append("|");
            sb.Append(strCardinality.PadLeft(25, paddingChar));
            sb.Append("|");

            return sb.ToString();
        }

        private static void AppendValues(StringBuilder sb, char paddingChar,
            Dictionary<SkylineTypesSingleSampling, List<double>> setCoverageSampling)
        {
            if (setCoverageSampling.Count == 0)
            {
                for (var i = 0; i < 32; i++)
                {
                    sb.Append("".PadLeft(20, paddingChar));
                    sb.Append("|");
                }

                return;
            }

            foreach (
                SkylineTypesSingleSampling skylineTypesSingleSamplingType in
                    Enum.GetValues(typeof (SkylineTypesSingleSampling)).Cast<SkylineTypesSingleSampling>())
            {
                sb.Append(
                    Math.Round(setCoverageSampling[skylineTypesSingleSamplingType].Average(), 2)
                        .ToString(CultureInfo.InvariantCulture)
                        .PadLeft(20, paddingChar));
                sb.Append("|");
                sb.Append(
                    Math.Round(setCoverageSampling[skylineTypesSingleSamplingType].Min(), 2)
                        .ToString(CultureInfo.InvariantCulture)
                        .PadLeft(20, paddingChar));
                sb.Append("|");
                sb.Append(
                    Math.Round(setCoverageSampling[skylineTypesSingleSamplingType].Max(), 2)
                        .ToString(CultureInfo.InvariantCulture)
                        .PadLeft(20, paddingChar));
                sb.Append("|");
                sb.Append(
                    Math.Round(MyMathematic.GetSampleVariance(setCoverageSampling[skylineTypesSingleSamplingType]), 2)
                        .ToString(CultureInfo.InvariantCulture)
                        .PadLeft(20, paddingChar));
                sb.Append("|");
                sb.Append(
                    Math.Round(MyMathematic.GetSampleStdDeviation(setCoverageSampling[skylineTypesSingleSamplingType]),
                        2)
                        .ToString(CultureInfo.InvariantCulture)
                        .PadLeft(20, paddingChar));
                sb.Append("|");
                sb.Append(
                    Math.Round(Mathematic.Median(setCoverageSampling[skylineTypesSingleSamplingType]), 2)
                        .ToString(CultureInfo.InvariantCulture)
                        .PadLeft(20, paddingChar));
                sb.Append("|");
                sb.Append(
                    Math.Round(Mathematic.LowerQuartile(setCoverageSampling[skylineTypesSingleSamplingType]), 2)
                        .ToString(CultureInfo.InvariantCulture)
                        .PadLeft(20, paddingChar));
                sb.Append("|");
                sb.Append(
                    Math.Round(Mathematic.UpperQuartile(setCoverageSampling[skylineTypesSingleSamplingType]), 2)
                        .ToString(CultureInfo.InvariantCulture)
                        .PadLeft(20, paddingChar));
                sb.Append("|");
            }
        }

        private static void AppendClusterAnalysisValues(StringBuilder sb, char paddingChar, int paddingCount, Dictionary<ClusterAnalysisSampling, string> clusterAnalysisStrings)
        {
            if (clusterAnalysisStrings.Count == 0)
            {
                for (var i = 0; i < 5; i++)
                {
                    sb.Append("".PadLeft(paddingCount, paddingChar));
                    sb.Append("|");
                }

                return;
            }

            sb.Append(clusterAnalysisStrings[ClusterAnalysisSampling.EntireDb].PadLeft(paddingCount, paddingChar));
            sb.Append("|");
            sb.Append(clusterAnalysisStrings[ClusterAnalysisSampling.EntireSkyline].PadLeft(paddingCount, paddingChar));
            sb.Append("|");
            sb.Append(clusterAnalysisStrings[ClusterAnalysisSampling.SampleSkyline].PadLeft(paddingCount, paddingChar));
            sb.Append("|");
            sb.Append(clusterAnalysisStrings[ClusterAnalysisSampling.BestRank].PadLeft(paddingCount, paddingChar));
            sb.Append("|");
            sb.Append(clusterAnalysisStrings[ClusterAnalysisSampling.SumRank].PadLeft(paddingCount, paddingChar));
            sb.Append("|");
        }

        private string FormatLineString(string strTitle, string strTrial, double dimension, double skyline, double timeTotal, double timeAlgo, double minCorrelation, double maxCorrelation, double cardinality, double numberOfMoves, double numberOfComparisons)
        {
            return FormatLineString(' ', strTitle, strTrial, Math.Round(dimension, 2).ToString(CultureInfo.InvariantCulture), Math.Round(skyline, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeTotal, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeAlgo, 2).ToString(CultureInfo.InvariantCulture), Math.Round(minCorrelation, 2).ToString(), Math.Round(maxCorrelation, 2).ToString(), ToLongString(Math.Round(cardinality, 2)), Math.Round(numberOfMoves, 2).ToString(), Math.Round(numberOfComparisons, 2).ToString());
        }

        private string FormatLineStringSample(string strTitle, string strTrial, double dimension, double skyline, double timeTotal, double timeAlgo, double minTime, double maxTime, double varianceTime, double stddeviationTime, double medTime, double q1Time, double q3Time, double minSize, double maxSize, double varianceSize, double stddeviationSize, double medSize, double q1Size, double q3Size, Dictionary<SkylineTypesSingleSampling, List<double>> setCoverageSampling, Dictionary<SkylineTypesSingleSampling, List<double>> representationErrorSampling, Dictionary<SkylineTypesSingleSampling, List<double>> representationErrorSumSampling, Dictionary<SkylineTypesSingleSampling, List<double>> dominatedObjectsCountSampling, Dictionary<SkylineTypesSingleSampling, List<double>> dominatedObjectsOfBestObjectSampling, Dictionary<ClusterAnalysisSampling, string> clusterAnalysisStrings, Dictionary<ClusterAnalysisSampling, string> clusterAnalysisMedianStrings, Dictionary<ClusterAnalysisSampling, string> clusterAnalysisTopBucketsStrings, Dictionary<ClusterAnalysisSampling, string> clusterAnalysisMedianTopBucketsStrings, double minCorrelation, double maxCorrelation, double cardinality)
        {
            return FormatLineStringSample(' ', strTitle, strTrial, Math.Round(dimension, 2).ToString(CultureInfo.InvariantCulture), Math.Round(skyline, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeTotal, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeAlgo, 2).ToString(CultureInfo.InvariantCulture), Math.Round(minTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(maxTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(varianceTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(stddeviationTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(medTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(q1Time, 2).ToString(CultureInfo.InvariantCulture), Math.Round(q3Time, 2).ToString(CultureInfo.InvariantCulture), Math.Round(minSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(maxSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(varianceSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(stddeviationSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(medSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(q1Size, 2).ToString(CultureInfo.InvariantCulture), Math.Round(q3Size, 2).ToString(CultureInfo.InvariantCulture), setCoverageSampling, representationErrorSampling, representationErrorSumSampling, dominatedObjectsCountSampling, dominatedObjectsOfBestObjectSampling, clusterAnalysisStrings, clusterAnalysisMedianStrings, clusterAnalysisTopBucketsStrings, clusterAnalysisMedianTopBucketsStrings, Math.Round(minCorrelation, 2).ToString(CultureInfo.InvariantCulture), Math.Round(maxCorrelation, 2).ToString(CultureInfo.InvariantCulture), ToLongString(Math.Round(cardinality, 2)));
        }

        private string FormatLineStringSample(string strTitle, string strTrial, double dimension, double skyline, double timeTotal, double timeAlgo, double minTime, double maxTime, double varianceTime, double stddeviationTime, double medTime, double q1Time, double q3Time, double minSize, double maxSize, double varianceSize, double stddeviationSize, double medSize, double q1Size, double q3Size, string[] setCoverageSampling, string[] representationErrorSampling, string[] representationErrorSumSampling, string[] dominatedObjectsCountSampling, string[] dominatedObjectsOfBestObjectSampling, string[] clusterAnalysisStrings, string[] clusterAnalysisMedianStrings, string[] clusterAnalysisTopBucketsStrings, string[] clusterAnalysisMedianTopBucketsStrings, double minCorrelation, double maxCorrelation, double cardinality)
        {
            return FormatLineStringSample(' ', strTitle, strTrial, Math.Round(dimension, 2).ToString(CultureInfo.InvariantCulture), Math.Round(skyline, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeTotal, 2).ToString(CultureInfo.InvariantCulture), Math.Round(timeAlgo, 2).ToString(CultureInfo.InvariantCulture), Math.Round(minTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(maxTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(varianceTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(stddeviationTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(medTime, 2).ToString(CultureInfo.InvariantCulture), Math.Round(q1Time, 2).ToString(CultureInfo.InvariantCulture), Math.Round(q3Time, 2).ToString(CultureInfo.InvariantCulture), Math.Round(minSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(maxSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(varianceSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(stddeviationSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(medSize, 2).ToString(CultureInfo.InvariantCulture), Math.Round(q1Size, 2).ToString(CultureInfo.InvariantCulture), Math.Round(q3Size, 2).ToString(CultureInfo.InvariantCulture), setCoverageSampling, representationErrorSampling, representationErrorSumSampling, dominatedObjectsCountSampling, dominatedObjectsOfBestObjectSampling, clusterAnalysisStrings, clusterAnalysisMedianStrings, clusterAnalysisTopBucketsStrings, clusterAnalysisMedianTopBucketsStrings, Math.Round(minCorrelation, 2).ToString(CultureInfo.InvariantCulture), Math.Round(maxCorrelation, 2).ToString(CultureInfo.InvariantCulture), ToLongString(Math.Round(cardinality, 2)));
        }

        
        /// <summary>
        /// Source: http://stackoverflow.com/questions/1546113/double-to-string-conversion-without-scientific-notation
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        private static string ToLongString(double input)
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
