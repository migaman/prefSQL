using System.Collections;
using System.Collections.Generic;
using System.Data;

//!!!Caution: Attention small changes in this code can lead to remarkable performance issues!!!!
namespace prefSQL.SQLSkyline
{
    using System;
    using System.Linq;

    /// <summary>
    /// BNL Algorithm implemented according to algorithm pseudocode in Börzsönyi et al. (2001)
    /// </summary>
    /// <remarks>
    /// Börzsönyi, Stephan; Kossmann, Donald; Stocker, Konrad (2001): The Skyline Operator. In : 
    /// Proceedings of the 17th International Conference on Data Engineering. Washington, DC, USA: 
    /// IEEE Computer Society, pp. 421–430. Available online at http://dl.acm.org/citation.cfm?id=645484.656550.
    /// 
    /// Profiling considersations:
    /// - Always use equal when comparins test --> i.e. using a startswith instead of an equal can decrease performance by 10 times
    /// - Write objects from DataReader into an object[] an work with the object. 
    /// - Explicity convert (i.e. (int)reader[0]) value from DataReader and don't use the given methods (i.e. reader.getInt32(0))
    /// </remarks>
    public abstract class TemplateBNL : TemplateStrategy
    {

        protected override DataTable GetSkylineFromAlgorithm(IEnumerable<object[]> database, DataTable dataTableTemplate, string[] operatorsArray, string[] additionalParameters)
        {
            List<long[]> window = new List<long[]>();
            ArrayList windowIncomparable = new ArrayList();
            int dimensionsCount = operatorsArray.Count(op => op != "IGNORE");
            int dimensionsTupleCount = operatorsArray.Count(op => op != "IGNORE" && op != "INCOMPARABLE");
            int[] dimensions = new int[dimensionsCount];
            int[] dimensionsTuple = new int[dimensionsTupleCount];
            
            int nextDim = 0;
            for(int d=0;d<operatorsArray.Length;d++)
            {
                if (operatorsArray[d] != "IGNORE")
                {
                    dimensions[nextDim] = d;
                    nextDim++;
                }
            }

            nextDim = 0;
            for (int d = 0; d < operatorsArray.Length; d++)
            {
                if (operatorsArray[d] != "IGNORE" && operatorsArray[d] != "INCOMPARABLE")
                {
                    dimensionsTuple[nextDim] = d;
                    nextDim++;
                }
            }

            DataTable dataTableReturn = dataTableTemplate;

            long[] newTuple = new long[dimensionsTupleCount];

            //For each tuple
            foreach (object[] dbValuesObject in database)
            {                
                int next = 0;
                foreach (int dimension in dimensionsTuple)
                {
                    newTuple[next] = (long)dbValuesObject[dimension];


                    //TODO: Fix: For incomparable tuple the index must be the same and not the next index
                    //Otherwise function IsTupleDominated must be changed!!
                    //newTuple[j] = (long)dbValuesObject[j];


                    next++;
                }         

                bool isDominated = false;

                //Do not move. start with last tuple in window.   
                if (WindowHandling == 0)
                {
                    //check if record is dominated (compare against the records in the window)
                    //Attention: List is used DESCENDING
                    for (int i = window.Count - 1; i >= 0; i--)
                    {

                        //long[] windowTuple = window[i];
                        //Level BNL performance drops 2 times with using the next line
                        //string[] incomparableTuple = (string[])windowIncomparable[i];

                        //Only use this for tests (Drops performance 10%)
                        //NumberOfOperations++;

                        //TODO: Using the Helper directly is sligthly faster, but than every bnl algorithm needs it own logic
                        //if(Helper.IsTupleDominated(window, newTuple, dimensions))
                        //Helper.IsTupleDominated()

                        //TODO: Comment out for high performance
                        NumberOfOperations++;

                        if (IsTupleDominated(window, newTuple, dimensions, operatorsArray, windowIncomparable, i, dataTableReturn, dbValuesObject))
                        {
                            //NumberOfMoves++;
                            isDominated = true;
                            break;
                        }
                    }
                    if (isDominated == false)
                    {
                        AddToWindow(dbValuesObject, window, windowIncomparable, operatorsArray, dimensions, dataTableReturn);
                    }
                }
                //Do not move. start with first tuple in window.   
                else if (WindowHandling == 1)
                {
                    //check if record is dominated (compare against the records in the window)
                    //Attention: List is used ASCENDING
                    for (int i = 0; i < window.Count; i++)
                    {
                        NumberOfOperations++;

                        if (IsTupleDominated(window, newTuple, dimensions, operatorsArray, windowIncomparable, i, dataTableReturn, dbValuesObject))
                        {
                            //NumberOfMoves++;
                            isDominated = true;
                            break;
                        }
                    }
                    if (isDominated == false)
                    {
                        //Work with operatorsArray length instead of dimensions (because of sampling skyline algorithms)
                        AddToWindow(dbValuesObject, window, windowIncomparable, operatorsArray, dimensions, dataTableReturn);
                    }
                }
                //Move To End   start with last tuple in window.   
                else if (WindowHandling == 2)
                {
                    //check if record is dominated (compare against the records in the window)
                    for (int i = window.Count - 1; i >= 0; i--)
                    {
                        NumberOfOperations++;

                        if (IsTupleDominated(window, newTuple, dimensions, operatorsArray, windowIncomparable, i, dataTableReturn, dbValuesObject))
                        {
                            long[] headNext = window[window.Count - 1];
                            long[] current = window[i];
                            if (current != headNext)
                            {
                                //Tupel i an position 0
                                //Tupel 0 an position 1
                                //Move to End
                                long[] newFirst = current;
                                window.RemoveAt(i);
                                //window.Insert(0, newFirst);
                                window.Add(newFirst);
                                NumberOfMoves++;
                            }

                            isDominated = true;
                            break;
                        }
                    }
                    if (isDominated == false)
                    {
                        //Work with operatorsArray length instead of dimensions (because of sampling skyline algorithms)
                        AddToWindow(dbValuesObject, window, windowIncomparable, operatorsArray, dimensions, dataTableReturn);
                    }
                }
                //Move To Beginning   start with first tuple in window.   
                else if (WindowHandling == 3)
                {
                    //check if record is dominated (compare against the records in the window)
                    for (int i = 0; i < window.Count; i++)
                    {
                        NumberOfOperations++;

                        if (IsTupleDominated(window, newTuple, dimensions, operatorsArray, windowIncomparable, i, dataTableReturn, dbValuesObject))
                        {
                            long[] headNext = window[0];
                            long[] current = window[i];
                            if (current != headNext)
                            {

                                //Tupel i an position 0
                                //Tupel 0 an position 1
                                long[] newFirst = current;
                                window.RemoveAt(i);
                                window.Insert(0, newFirst);
                                NumberOfMoves++;
                            }

                            isDominated = true;
                            break;
                        }
                    }
                    if (isDominated == false)
                    {
                        //Work with operatorsArray length instead of dimensions (because of sampling skyline algorithms)
                        AddToWindow(dbValuesObject, window, windowIncomparable, operatorsArray, dimensions, dataTableReturn);
                    }
                }
               
               

                
            }
            //Special orderings need the skyline values. Store it in a property
            SkylineValues = window;

            return dataTableReturn;
        }



        //Attention: Profiling
        //It seems to makes sense to remove the parameter listIndex and pass the string-array incomparableTuples[listIndex]
        //Unfortunately this has negative impact on the performance for algorithms that don't work with incomparables
        protected abstract bool IsTupleDominated(List<long[]> window, long[] newTuple, int[] dimensions, string[] operatorsArray, ArrayList incomparableTuples, int listIndex, DataTable dtResult, object[] newTupleAllValues);

        protected abstract void AddToWindow(object[] newTuple, List<long[]> window, ArrayList resultstringCollection, string[] operatorsArray, int[] dimensions, DataTable dtResult);

    }
}
