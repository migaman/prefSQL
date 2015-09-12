
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Diagnostics;
using Microsoft.SqlServer.Server;

//Caution: Attention small changes in this code can lead to performance issues, i.e. using a startswith instead of an equal can increase by 10 times
//Important: Only use equal for comparing text (otherwise performance issues)
namespace prefSQL.SQLSkyline
{
    using System;
    using System.Linq;

    public class SPMultipleSkylineBNLLevel : TemplateStrategy
    {
        /// <summary>
        /// Calculate the skyline points from a dataset
        /// </summary>
        /// <param name="strQuery"></param>
        /// <param name="strOperators"></param>
        /// <param name="numberOfRecords"></param>
        /// <param name="sortType"></param>
        /// <param name="upToLevel"></param>
        [SqlProcedure(Name = "SP_MultipleSkylineBNLLevel")]
        public static void GetSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords, SqlInt32 sortType, SqlInt32 upToLevel)
        {
            SPMultipleSkylineBNLLevel skyline = new SPMultipleSkylineBNLLevel();
            string[] additionalParameters = new string[5];
            additionalParameters[4] = upToLevel.ToString();
            skyline.GetSkylineTable(strQuery.ToString(), strOperators.ToString(), numberOfRecords.Value, false, Helper.CnnStringSqlclr, Helper.ProviderClr, additionalParameters, sortType.Value);
        }


        protected override DataTable GetSkylineFromAlgorithm(IEnumerable<object[]> database, DataTable dataTableTemplate, string[] operatorsArray, string[] additionalParameters)
        {
           
            //load some variables from the additional paraemters
            int upToLevel = int.Parse(additionalParameters[4].Trim());

            List<long[]> window = new List<long[]>();
            ArrayList windowIncomparable = new ArrayList();
            int dimensionsCount = operatorsArray.Count(op => op != "IGNORE");
            int dimensionsTupleCount = operatorsArray.Count(op => op != "IGNORE" && op != "INCOMPARABLE");
            //int dimensions = 0; //operatorsArray.GetUpperBound(0)+1;
            int[] dimensions = new int[dimensionsCount];

            int nextDim = 0;
            for (int i = 0; i < operatorsArray.Length; i++)
            {
                if (operatorsArray[i] != "IGNORE")
                {
                    //dimensions++;
                    dimensions[nextDim] = i;
                    nextDim++;
                }
            }

            DataTable dataTableReturn = dataTableTemplate.Clone();

            //trees erstellen mit n nodes (n = anzahl tupels)
            List<int> levels = new List<int>();
            dataTableReturn.Columns.Add("level", typeof(int));
            int iMaxLevel = 0;

            //For each tuple
            foreach (object[] dbValuesObject in database)
            {
                long[] newTuple = new long[dimensionsTupleCount];
                int next = 0;
                for (int j = 0; j < operatorsArray.Length; j++)
                {
                    if (operatorsArray[j] != "IGNORE" && operatorsArray[j] != "INCOMPARABLE")
                    {
                        //Fix: For incomparable tuple the index must be the same and not the next index
                        //Otherwise function IsTupleDominated must be changed!!
                        newTuple[j] = (long)dbValuesObject[j];
                        next++;
                    }
                }


                //Insert the new record to the tree
                bool bFound = false;

                //Start wie level 0 nodes (until uptolevels or maximum levels)
                for (int iLevel = 0; iLevel <= iMaxLevel && iLevel < upToLevel; iLevel++)
                {
                    bool isDominated = false;
                    for (int i = 0; i < window.Count; i++)
                    {
                        if (levels[i] == iLevel)
                        {
                            //Dominanz
                            if (Helper.IsTupleDominated(window[i], newTuple, dimensions))
                            {
                                //Dominated in this level. Next level
                                isDominated = true;
                                break;
                            }
                        }
                    }
                    //Check if the record is dominated in this level
                    if (isDominated == false)
                    {
                        levels.Add(iLevel);
                        bFound = true;
                        break;
                    }
                }
                if (bFound == false)
                {
                    iMaxLevel++;
                    if (iMaxLevel < upToLevel)
                    {
                        levels.Add(iMaxLevel);
                        AddToWindow(dbValuesObject, window, operatorsArray.GetLength(0), dataTableReturn, levels[levels.Count - 1]);
                    }
                }
                else
                {
                    AddToWindow(dbValuesObject, window, operatorsArray.GetLength(0), dataTableReturn, levels[levels.Count - 1]);
                }


            }

            return dataTableReturn;
        }


        private void AddToWindow(object[] newTuple, List<long[]> window, int dimensions, DataTable dtResult, int level)
        {
            long[] record = new long[dimensions];
            DataRow row = dtResult.NewRow();

            for (int iCol = 0; iCol < newTuple.Length; iCol++)
            {
                //Only the real columns (skyline columns are not output fields)
                if (iCol <= dimensions - 1)
                {
                    record[iCol] = (long)newTuple[iCol];
                }
                else
                {
                    row[iCol - dimensions] = newTuple[iCol];
                }
            }
            row[record.Length + 1] = level;

            //DataTable is for the returning values
            dtResult.Rows.Add(row);
            //ResultCollection contains the skyline values (for the algorithm)
            window.Add(record);
        }




    }
}