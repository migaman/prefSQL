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
    using prefSQL.SQLParserTest;
    using prefSQL.SQLSkyline.SamplingSkyline;

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

        private const string Path = "E:\\Doc\\Studies\\PRJ_Thesis\\15 Performance\\Output\\";
        private int _trials = 5;                 //How many times each preferene query is executed
        private int _dimensions = 6;             //Up to this amount of dimension should be tested
        private int _randomDraws = 25;          //Only used for the shuffle set. How many random set will be generated
        static Random _rnd = new Random();
        private int _minDimensions = 2;

        public Performance()
        {
            UseCLR = false;
        }


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
            CombinationMinCardinality,      //Special collection of preferences which perform well on Hexagon
            Correlation,            //Take 2 best correlated preferences
            AntiCorrelation,        //Take 2 worst correlated preferences
            Independent,            //Take 2 most independent correlated preferences

        };


        #region getter/setters

        public bool UseCLR { get; set; }

        internal Size TableSize { get; set; }

        public int Dimensions
        {
            get { return _dimensions; }
            set { _dimensions = value; }
        }
        

        public SkylineStrategy Strategy { get; set; }

        public bool GenerateScript { get; set; }

        internal PreferenceSet Set { get; set; }

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

        private ArrayList GetNumericPreferences()
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

        private ArrayList GetCategoricalPreferences()
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



        private ArrayList GetAllPreferences()
        {
            ArrayList preferences = new ArrayList();
            preferences.AddRange(GetNumericPreferences());
            preferences.AddRange(GetCategoricalPreferences());
            return preferences;
        }

        #endregion


        public void GeneratePerformanceQueries()
        {
            //Open DBConnection --> Otherwise first query is slower as usual, because DBConnection is not open
            SQLCommon parser = new SQLCommon();
            DataTable dt = parser.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, "SELECT cars.id FROM cars SKYLINE OF cars.price LOW");

            //Use the correct line, depending on how incomparable items should be compared
            ArrayList listPreferences = new ArrayList();
            ArrayList correlationMatrix = new ArrayList();
            ArrayList listCardinality = new ArrayList();
            SqlConnection cnnSQL = new SqlConnection(Helper.ConnectionString); //for CLR performance tets
            if (UseCLR)
            {
                cnnSQL.Open();
            }

            if (Set == PreferenceSet.ArchiveComparable)
            {
                ArrayList preferences = GetArchiveComparablePreferences();
                correlationMatrix = GetCorrelationMatrix(preferences);
                listCardinality = GetCardinalityOfPreferences(preferences);
                //Only one set
                listPreferences.Add(preferences);
            }
            else if (Set == PreferenceSet.ArchiveIncomparable)
            {
                ArrayList preferences = GetArchiveIncomparablePreferences();
                correlationMatrix = GetCorrelationMatrix(preferences);
                listCardinality = GetCardinalityOfPreferences(preferences);
                //Only one set
                listPreferences.Add(preferences);
            }
            else if (Set == PreferenceSet.Jon)
            {
                ArrayList preferences = GetJonsPreferences();
                correlationMatrix = GetCorrelationMatrix(preferences);
                listCardinality = GetCardinalityOfPreferences(preferences);
                //Only one set
                listPreferences.Add(preferences);

            }
            else if (Set == PreferenceSet.Mya)
            {
                ArrayList preferences = GetMyasPreferences();
                correlationMatrix = GetCorrelationMatrix(preferences);
                listCardinality = GetCardinalityOfPreferences(preferences);
                //Only one set
                listPreferences.Add(preferences);
            }
            else if (Set == PreferenceSet.Barra)
            {
                ArrayList preferences = GetBarrasPreferences();
                correlationMatrix = GetCorrelationMatrix(preferences);
                listCardinality = GetCardinalityOfPreferences(preferences);
                //Only one set
                listPreferences.Add(preferences);
            }
            else if (Set == PreferenceSet.Combination)
            {
                //Tests every possible combination with y preferences from the whole set of preferences
                ArrayList preferences = GetAllPreferences();
                correlationMatrix = GetCorrelationMatrix(preferences);
                listCardinality = GetCardinalityOfPreferences(preferences);

                if (_dimensions > preferences.Count)
                {
                    Debug.WriteLine("Combination with more dimensions than preferences. Please reduce dimensions!");
                    return;
                }

                //create all possible combinations and add it to listPreferences
                GetCombinations(preferences, _dimensions, 0, new ArrayList(), ref listPreferences);
                
                //set mindimensions to maxdimension (test with fixed amount of preferences)
                _minDimensions = _dimensions;

            }
            else if (Set == PreferenceSet.CombinationNumeric)
            {
                //Tests every possible combination with y preferences from the whole set of preferences
                ArrayList preferences = GetNumericPreferences();
                correlationMatrix = GetCorrelationMatrix(preferences);
                listCardinality = GetCardinalityOfPreferences(preferences);

                if (_dimensions > preferences.Count)
                {
                    Debug.WriteLine("Combination with more dimensions than preferences. Please reduce dimensions!");
                    return;
                }

                //create all possible combinations and add it to listPreferences
                GetCombinations(preferences, _dimensions, 0, new ArrayList(), ref listPreferences);
                
                //set mindimensions to maxdimension (test with fixed amount of preferences)
                _minDimensions = _dimensions;

            }
            else if (Set == PreferenceSet.CombinationCategoric)
            {
                //Tests every possible combination with y preferences from the whole set of preferences
                ArrayList preferences = GetCategoricalPreferences();
                correlationMatrix = GetCorrelationMatrix(preferences);
                listCardinality = GetCardinalityOfPreferences(preferences);

                if (_dimensions > preferences.Count)
                {
                    Debug.WriteLine("Combination with more dimensions than preferences. Please reduce dimensions!");
                    return;
                }

                //create all possible combinations and add it to listPreferences
                GetCombinations(preferences, _dimensions, 0, new ArrayList(), ref listPreferences);
                
                //set mindimensions to maxdimension (test with fixed amount of preferences)
                _minDimensions = _dimensions;

            }
            else if (Set == PreferenceSet.CombinationMinCardinality)
            {
                //Tests every possible combination with y preferences from the whole set of preferences
                ArrayList preferences = GetSpecialHexagonPreferences();
                correlationMatrix = GetCorrelationMatrix(preferences);
                listCardinality = GetCardinalityOfPreferences(preferences);

                if (_dimensions > preferences.Count)
                {
                    Debug.WriteLine("Combination with more dimensions than preferences. Please reduce dimensions!");
                    return;
                }

                //create all possible combinations and add it to listPreferences
                GetCombinations(preferences, _dimensions, 0, new ArrayList(), ref listPreferences);

                //set mindimensions to maxdimension (test with fixed amount of preferences)
                _minDimensions = _dimensions;
            }
            
            else if (Set == PreferenceSet.Shuffle)
            {
                //Tests x times randomly y preferences

                ArrayList preferences = GetAllPreferences();
                correlationMatrix = GetCorrelationMatrix(preferences);
                listCardinality = GetCardinalityOfPreferences(preferences);

                for (int iChoose = 0; iChoose < _randomDraws; iChoose++)
                {
                    ArrayList preferencesRandom = new ArrayList();
                    ArrayList preferencesChoose = (ArrayList)preferences.Clone();

                    //Choose x preferences randomly
                    for (int i = 0; i < _dimensions; i++)
                    {
                        int r = _rnd.Next(preferencesChoose.Count);
                        preferencesRandom.Add(preferencesChoose[r]);
                        preferencesChoose.RemoveAt(r);
                    }

                    //add random preferences to listPreferences
                    listPreferences.Add(preferencesRandom);

                }

                //set mindimensions to maxdimension (test with fixed amount of preferences)
                _minDimensions = _dimensions;
            }
            else if (Set == PreferenceSet.Correlation)
            {
                //Tests every possible combination with y preferences from the whole set of preferences
                ArrayList preferences = GetAllPreferences();
                correlationMatrix = GetCorrelationMatrix(preferences);
                listCardinality = GetCardinalityOfPreferences(preferences);

                //Sort correlations to find the strongest
                correlationMatrix.Sort(new CorrelationModel());

                //Sort correlations ascending
                CorrelationModel model = (CorrelationModel)correlationMatrix[0];
                preferences.Clear();
                preferences.Add(model.ColA);
                preferences.Add(model.ColB);
                listPreferences.Add(preferences);
                
                //only the two dimensions should be tested
                _minDimensions = 2;
                _dimensions = 2;

            }
            else if (Set == PreferenceSet.AntiCorrelation)
            {
                //Tests every possible combination with y preferences from the whole set of preferences
                ArrayList preferences = GetAllPreferences();
                correlationMatrix = GetCorrelationMatrix(preferences);
                listCardinality = GetCardinalityOfPreferences(preferences);

                //Sort correlations ascending
                correlationMatrix.Sort(new CorrelationModel());
                
                //Take only the two preferences with the worst correlation
                CorrelationModel model = (CorrelationModel)correlationMatrix[correlationMatrix.Count-1];
                preferences.Clear();
                preferences.Add(model.ColA);
                preferences.Add(model.ColB);
                listPreferences.Add(preferences);

                //only the two dimensions should be tested
                _minDimensions = 2;
                _dimensions = 2;
            }
            else if (Set == PreferenceSet.Independent)
            {
                //Tests every possible combination with y preferences from the whole set of preferences
                ArrayList preferences = GetAllPreferences();
                correlationMatrix = GetCorrelationMatrix(preferences);
                listCardinality = GetCardinalityOfPreferences(preferences);

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
                _minDimensions = 2;
                _dimensions = 2;
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
                     strSeparatorLine = FormatLineStringSample('-', "", "", "", "", "", "", "", "","","","","","","","","");
                }
                else
                {
                     strSeparatorLine = FormatLineString('-', "", "", "", "", "", "", "", "");
                }
                
                if (GenerateScript == false)
                {
                    //Header
                    sb.AppendLine("               Algorithm: " + currentStrategy);
                    sb.AppendLine("          Preference Set: " + Set.ToString());
                    sb.AppendLine("                    Host: " + Environment.MachineName);
                    sb.AppendLine("      Set of Preferences: " + listPreferences.Count);
                    sb.AppendLine("                  Trials: " + Trials);
                    sb.AppendLine("              Table size: " + TableSize.ToString());
                    sb.AppendLine("          Dimension from: " + _minDimensions.ToString());
                    sb.AppendLine("            Dimension to: " + _dimensions.ToString());
                    if (Sampling)
                    {
                        sb.AppendLine("                Sampling: true");
                        sb.AppendLine("         Subspaces Count: " + SamplingSubspacesCount);
                        sb.AppendLine("      Subspace Dimension: " + SamplingSubspaceDimension);
                        sb.AppendLine("           Sampling Runs: " + SamplingSamplesCount);
                    }
                    //sb.AppendLine("Correlation Coefficients:" + string.Join(",", (string[])preferences.ToArray(Type.GetType("System.String"))));
                    //sb.AppendLine("           Cardinalities:" + string.Join(",", (string[])preferences.ToArray(Type.GetType("System.String"))));
                    sb.AppendLine("");
                    if (Sampling)
                    {
                        sb.AppendLine(FormatLineStringSample(' ', "preference set", "trial", "dimensions", "avg skyline size", "avg time total", "avg time algorithm", "min time", "max time", "variance time", "stddeviation time", "min size", "max size", "variance size", "stddeviation size", "sum correlation*", "product cardinality"));
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

                List<long> reportSkylineSampleSizeMin = new List<long>();
                List<long> reportSampleTimeAlgorithmMin = new List<long>();
                List<long> reportSkylineSampleSizeMax = new List<long>();
                List<long> reportSampleTimeAlgorithmMax = new List<long>();
                List<double> reportSkylineSampleSizeVar = new List<double>();
                List<double> reportSampleTimeAlgorithmVar = new List<double>();
                List<double> reportSkylineSampleSizeStdDev = new List<double>();
                List<double> reportSampleTimeAlgorithmStdDev = new List<double>();
                           
                //For each preference set in the preference list
                for (int iPreferenceIndex = 0; iPreferenceIndex < listPreferences.Count; iPreferenceIndex++)
                {
                    ArrayList preferences = (ArrayList)listPreferences[iPreferenceIndex];
                    //Go only down two 3 dimension (because there are special algorithms for 1 and 2 dimensional skyline)
                    for (int i = _minDimensions; i <= preferences.Count; i++)
                    {
                        //ADD Preferences to SKYLINE
                        ArrayList subPreferences = preferences.GetRange(0, i);
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
                                try
                                {
                                    Stopwatch sw = new Stopwatch();

                                    double correlation = SearchCorrelation(subPreferences, correlationMatrix);
                                    double cardinality = SearchCardinality(subPreferences, listCardinality);

                                    if (Sampling)
                                    {
                                        string strQuery;
                                        string operators;
                                        int numberOfRecords;
                                        string[] parameter;

                                        strSQL+=" SAMPLE BY RANDOM_SUBSETS COUNT "+SamplingSubspacesCount+" DIMENSION "+SamplingSubspaceDimension;

                                        PrefSQLModel prefSqlModel = parser.GetPrefSqlModelFromPreferenceSql(strSQL);
                                        string ansiSql = parser.GetAnsiSqlFromPrefSqlModel(prefSqlModel);
                                        prefSQL.SQLParser.Helper.DetermineParameters(ansiSql, out parameter, out strQuery, out operators,
                                            out numberOfRecords);

                                        var randomSubspacesesProducer = new RandomSamplingSkylineSubspacesProducer
                                        {
                                            AllPreferencesCount = preferences.Count,
                                            SubspacesCount = SamplingSubspacesCount,
                                            SubspaceDimension = SamplingSubspaceDimension
                                        };

                                        var producedSubspaces = new List<HashSet<HashSet<int>>>();
                                        for (var ii = 0; ii < SamplingSamplesCount; ii++)
                                        {
                                            producedSubspaces.Add(randomSubspacesesProducer.GetSubspaces());
                                        }

                                    var subspaceObjects = new List<long>();
                var subspaceTime = new List<long>();
                var subspaceTimeElapsed = new List<long>();

                                        foreach (HashSet<HashSet<int>> subspace in producedSubspaces)
                                        {
                                            sw.Restart();
                                            var subspacesProducer = new FixedSamplingSkylineSubspacesProducer(subspace);
                                            var utility = new SamplingSkylineUtility(subspacesProducer);
                                            var skylineSample = new SamplingSkyline(utility) { Provider = Helper.ProviderName };

                                            DataTable dataTable = skylineSample.GetSkylineTable(Helper.ConnectionString, strQuery, operators,
                                                numberOfRecords, prefSqlModel.WithIncomparable, parameter, parser.SkylineType,
                                                prefSqlModel.SkylineSampleCount, prefSqlModel.SkylineSampleDimension, 0);

                                            sw.Stop();

                                            subspaceObjects.Add(dataTable.Rows.Count);
                                            subspaceTime.Add(skylineSample.TimeMilliseconds);
                                            subspaceTimeElapsed.Add(sw.ElapsedMilliseconds);
                                        }

                                        long time = (long)(subspaceTime.Average()+.5);
                                        long objects = (long)(subspaceObjects.Average() + .5);
                                        long elapsed = (long)(subspaceTimeElapsed.Average() + .5);

                                        reportDimensions.Add(i);
                                        reportSkylineSize.Add(objects);
                                        reportTimeTotal.Add(elapsed);
                                        reportTimeAlgorithm.Add(time);
                                        reportCorrelation.Add(correlation);
                                        reportCardinality.Add(cardinality);

                                        var mathematic = new Mathematic();
                                        reportSkylineSampleSizeMin.Add(subspaceObjects.Min());
                                        reportSampleTimeAlgorithmMin.Add(subspaceTime.Min());
                                        reportSkylineSampleSizeMax.Add(subspaceObjects.Max());
                                        reportSampleTimeAlgorithmMax.Add(subspaceTime.Max());
                                        reportSkylineSampleSizeVar.Add(mathematic.GetVariance(subspaceObjects));
                                        reportSampleTimeAlgorithmVar.Add(mathematic.GetVariance(subspaceTime));
                                        reportSkylineSampleSizeStdDev.Add(mathematic.GetStdDeviation(subspaceObjects));
                                        reportSampleTimeAlgorithmStdDev.Add(mathematic.GetStdDeviation(subspaceTime));

                                        //trial|dimensions|skyline size|time total|time algorithm
                                        string strTrial = iTrial + 1 + " / " + _trials;
                                        string strPreferenceSet = iPreferenceIndex + 1 + " / " + listPreferences.Count;

                                        string strLine = FormatLineStringSample(strPreferenceSet, strTrial, i, objects, elapsed, time, subspaceTime.Min(), subspaceTime.Max(), mathematic.GetVariance(subspaceTime), mathematic.GetStdDeviation(subspaceTime), subspaceObjects.Min(), subspaceObjects.Max(), mathematic.GetVariance(subspaceObjects), mathematic.GetStdDeviation(subspaceObjects), correlation, cardinality);

                                        Debug.WriteLine(strLine);
                                        sb.AppendLine(strLine);                                        
                                    }
                                    else
                                    {
                                        sw.Start();
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
                                            dt = parser.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, strSQL);
                                        }
                                        long timeAlgorithm = parser.TimeInMilliseconds;
                                        long numberOfOperations = parser.NumberOfOperations;
                                        sw.Stop();

                                        reportDimensions.Add(i);
                                        reportSkylineSize.Add(dt.Rows.Count);
                                        reportTimeTotal.Add(sw.ElapsedMilliseconds);
                                        reportTimeAlgorithm.Add(timeAlgorithm);
                                        reportCorrelation.Add(correlation);
                                        reportCardinality.Add(cardinality);

                                        //trial|dimensions|skyline size|time total|time algorithm
                                        string strTrial = iTrial + 1 + " / " + _trials;
                                        string strPreferenceSet = iPreferenceIndex + 1 + " / " + listPreferences.Count;

                                        string strLine = FormatLineString(strPreferenceSet, strTrial, i, dt.Rows.Count, sw.ElapsedMilliseconds, timeAlgorithm, correlation, cardinality);

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
                            sb.AppendLine("PRINT '----- " + (i + 1) + " dimensions  ------'");
                            sb.AppendLine("PRINT '----- -------------------------------------------------------- ------'");
                            foreach (string size in sizes)
                            {
                                sb.AppendLine("GO"); //we need this in order the profiler shows each query in a new line
                                sb.AppendLine(strSQL.Replace("cars", "cars_" + size));

                            }

                            
                        }

                    }

                }

                ////////////////////////////////
                //Summary
                ///////////////////////////////
                if (GenerateScript == false)
                {
                    if (Sampling)
                    {
                        AddSummarySample(sb, strSeparatorLine, reportDimensions, reportSkylineSize, reportTimeTotal, reportTimeAlgorithm, reportSampleTimeAlgorithmMin, reportSampleTimeAlgorithmMax, reportSampleTimeAlgorithmVar, reportSampleTimeAlgorithmStdDev, reportSkylineSampleSizeMin, reportSkylineSampleSizeMax, reportSkylineSampleSizeVar, reportSkylineSampleSizeStdDev, reportCorrelation, reportCardinality);
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
            Mathematic mathematic = new Mathematic();
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

                        double correlation = mathematic.GetPearson(colA, colB);
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
                //Debug.WriteLine(string.Join(",", (string[])result.ToArray(Type.GetType("System.String"))));
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
            List<long> reportSkylineSize, List<long> reportTimeTotal, List<long> reportTimeAlgorithm, List<long> subspaceTimeMin, List<long> subspaceTimeMax, List<double> subspaceTimeVar, List<double> subspaceTimeStdDev, List<long> subspaceObjectsMin, List<long> subspaceObjectsMax, List<double> subspaceObjectsVar, List<double> subspaceObjectsStdDev, List<double> reportCorrelation, List<double> reportCardinality)
        {
            //Separator Line
            Debug.WriteLine(strSeparatorLine);
            sb.AppendLine(strSeparatorLine);

            var mathematic = new Mathematic();
            string strAverage = FormatLineStringSample("average", "", reportDimensions.Average(), reportSkylineSize.Average(), reportTimeTotal.Average(), reportTimeAlgorithm.Average(), subspaceTimeMin.Average(), subspaceTimeMax.Average(), subspaceTimeVar.Average(), subspaceTimeStdDev.Average(), subspaceObjectsMin.Average(), subspaceObjectsMax.Average(), subspaceObjectsVar.Average(), subspaceObjectsStdDev.Average(), reportCorrelation.Average(), reportCardinality.Average());
            string strMin = FormatLineStringSample("minimum", "", reportDimensions.Min(), reportSkylineSize.Min(), reportTimeTotal.Min(), reportTimeAlgorithm.Min(), subspaceTimeMin.Min(), subspaceTimeMax.Min(), subspaceTimeVar.Min(), subspaceTimeStdDev.Min(), subspaceObjectsMin.Min(), subspaceObjectsMax.Min(), subspaceObjectsVar.Min(), subspaceObjectsStdDev.Min(), reportCorrelation.Min(), reportCardinality.Min());
            string strMax = FormatLineStringSample("maximum", "", reportDimensions.Max(), reportSkylineSize.Max(), reportTimeTotal.Max(), reportTimeAlgorithm.Max(), subspaceTimeMin.Max(), subspaceTimeMax.Max(), subspaceTimeVar.Max(), subspaceTimeStdDev.Max(), subspaceObjectsMin.Max(), subspaceObjectsMax.Max(), subspaceObjectsVar.Max(), subspaceObjectsStdDev.Max(), reportCorrelation.Max(), reportCardinality.Max());
            string strVar = FormatLineStringSample("variance", "", mathematic.GetVariance(reportDimensions), mathematic.GetVariance(reportSkylineSize), mathematic.GetVariance(reportTimeTotal), mathematic.GetVariance(reportTimeAlgorithm), mathematic.GetVariance(subspaceTimeMin), mathematic.GetVariance(subspaceTimeMax), mathematic.GetVariance(subspaceTimeVar), mathematic.GetVariance(subspaceTimeStdDev), mathematic.GetVariance(subspaceObjectsMin), mathematic.GetVariance(subspaceObjectsMax), mathematic.GetVariance(subspaceObjectsVar), mathematic.GetVariance(subspaceObjectsStdDev), mathematic.GetVariance(reportCorrelation), mathematic.GetVariance(reportCardinality));
            string strStd = FormatLineStringSample("stddeviation", "", mathematic.GetStdDeviation(reportDimensions), mathematic.GetStdDeviation(reportSkylineSize), mathematic.GetStdDeviation(reportTimeTotal), mathematic.GetStdDeviation(reportTimeAlgorithm), mathematic.GetStdDeviation(subspaceTimeMin), mathematic.GetStdDeviation(subspaceTimeMax), mathematic.GetStdDeviation(subspaceTimeVar), mathematic.GetStdDeviation(subspaceTimeStdDev), mathematic.GetStdDeviation(subspaceObjectsMin), mathematic.GetStdDeviation(subspaceObjectsMax), mathematic.GetStdDeviation(subspaceObjectsVar), mathematic.GetStdDeviation(subspaceObjectsStdDev), mathematic.GetStdDeviation(reportCorrelation), mathematic.GetStdDeviation(reportCardinality));

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

        private void AddSummary(StringBuilder sb, String strSeparatorLine, List<long> reportDimensions, List<long> reportSkylineSize, List<long> reportTimeTotal, List<long> reportTimeAlgorithm, List<double> reportCorrelation, List<double> reportCardinality)
        {
            //Separator Line
            Debug.WriteLine(strSeparatorLine);
            sb.AppendLine(strSeparatorLine);

            Mathematic mathematic = new Mathematic();
            string strAverage = FormatLineString("average", "", reportDimensions.Average(), reportSkylineSize.Average(), reportTimeTotal.Average(), reportTimeAlgorithm.Average(), reportCorrelation.Average(), reportCardinality.Average());
            string strMin = FormatLineString("minimum", "", reportDimensions.Min(), reportSkylineSize.Min(), reportTimeTotal.Min(), reportTimeAlgorithm.Min(), reportCorrelation.Min(), reportCardinality.Min());
            string strMax = FormatLineString("maximum", "", reportDimensions.Max(), reportSkylineSize.Max(), reportTimeTotal.Max(), reportTimeAlgorithm.Max(), reportCorrelation.Max(), reportCardinality.Max());
            string strVar = FormatLineString("variance", "", mathematic.GetVariance(reportDimensions), mathematic.GetVariance(reportSkylineSize), mathematic.GetVariance(reportTimeTotal), mathematic.GetVariance(reportTimeAlgorithm), mathematic.GetVariance(reportCorrelation), mathematic.GetVariance(reportCardinality));
            string strStd = FormatLineString("stddeviation", "", mathematic.GetStdDeviation(reportDimensions), mathematic.GetStdDeviation(reportSkylineSize), mathematic.GetStdDeviation(reportTimeTotal), mathematic.GetStdDeviation(reportTimeAlgorithm), mathematic.GetStdDeviation(reportCorrelation), mathematic.GetStdDeviation(reportCardinality));

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



        private string FormatLineString(char paddingChar, string strTitle, string strTrial, string strDimension, string strSkyline, string strTimeTotal, string strTimeAlgo, string strCorrelation, string strCardinality)
        {
            //average line
            //trial|dimensions|skyline size|time total|time algorithm|correlation|
            string[] line = new string[9];
            line[0] = strTitle.PadLeft(14, paddingChar);
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

        private string FormatLineStringSample(char paddingChar, string strTitle, string strTrial, string strDimension, string strSkyline, string strTimeTotal, string strTimeAlgo, string minTime, string maxTime, string varianceTime, string sedDevTime, string minSize, string maxSize, string varianceSize, string sedDevSize, string strCorrelation, string strCardinality)
        {
            //average line
            //trial|dimensions|skyline size|time total|time algorithm|correlation|
            string[] line = new string[17];
            line[0] = strTitle.PadLeft(14, paddingChar);
            line[1] = strTrial.PadLeft(11, paddingChar);
            line[2] = strDimension.PadLeft(10, paddingChar);
            line[3] = strSkyline.PadLeft(20, paddingChar);
            line[4] = strTimeTotal.PadLeft(20, paddingChar);
            line[5] = strTimeAlgo.PadLeft(20, paddingChar);
            line[6] = minTime.PadLeft(20, paddingChar);
            line[7] = maxTime.PadLeft(20, paddingChar);
            line[8] = varianceTime.PadLeft(20, paddingChar);
            line[9] = sedDevTime.PadLeft(20, paddingChar);
            line[10] = minSize.PadLeft(20, paddingChar);
            line[11] = maxSize.PadLeft(20, paddingChar);
            line[12] = varianceSize.PadLeft(20, paddingChar);
            line[13] = sedDevSize.PadLeft(20, paddingChar);
            line[14] = strCorrelation.PadLeft(20, paddingChar);
            line[15] = strCardinality.PadLeft(25, paddingChar);
            line[16] = "";
            return string.Join("|", line);
        }

        private string FormatLineString(string strTitle, string strTrial, double dimension, double skyline, double timeTotal, double timeAlgo, double correlation, double cardinality)
        {
            return FormatLineString(' ', strTitle, strTrial, Math.Round(dimension, 2).ToString(), Math.Round(skyline, 2).ToString(), Math.Round(timeTotal, 2).ToString(), Math.Round(timeAlgo, 2).ToString(), Math.Round(correlation, 2).ToString(), ToLongString(Math.Round(cardinality, 2)));
        }

        private string FormatLineStringSample(string strTitle, string strTrial, double dimension, double skyline, double timeTotal, double timeAlgo, double minTime, double maxTime, double varianceTime, double stddeviationTime, double minSize, double maxSize, double varianceSize, double stddeviationSize, double correlation, double cardinality)
        {
            return FormatLineStringSample(' ', strTitle, strTrial, Math.Round(dimension, 2).ToString(), Math.Round(skyline, 2).ToString(), Math.Round(timeTotal, 2).ToString(), Math.Round(timeAlgo, 2).ToString(), Math.Round(minTime, 2).ToString(), Math.Round(maxTime, 2).ToString(), Math.Round(varianceTime, 2).ToString(), Math.Round(stddeviationTime, 2).ToString(), Math.Round(minSize, 2).ToString(), Math.Round(maxSize, 2).ToString(), Math.Round(varianceSize, 2).ToString(), Math.Round(stddeviationSize, 2).ToString(), Math.Round(correlation, 2).ToString(), ToLongString(Math.Round(cardinality, 2)));
        }

        
        /// <summary>
        /// Source: http://stackoverflow.com/questions/1546113/double-to-string-conversion-without-scientific-notation
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        private static string ToLongString(double input)
        {
            string str = input.ToString().ToUpper();

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
