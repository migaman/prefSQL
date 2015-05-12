using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.SqlServer.Server;

//!!!Caution: Attention small changes in this code can lead to remarkable performance issues!!!!
namespace prefSQL.SQLSkyline
{

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

        protected override DataTable GetSkylineFromAlgorithm(List<object[]> database, DataTable dataTableTemplate, SqlDataRecord dataRecordTemplate, string[] operatorsArray, string[] additionalParameters)
        {
            //ArrayList resultCollection = new ArrayList();

            List<long[]> resultCollection = new List<long[]>();
            int dimensions = operatorsArray.GetUpperBound(0)+1;
            
           
            
            ArrayList resultstringCollection = new ArrayList();
            DataTable dataTableReturn = dataTableTemplate.Clone();
            int[] resultToTupleMapping = Helper.ResultToTupleMapping(operatorsArray);

            //For each tuple
            foreach (object[] dbValuesObject in database)
            {
                long[] newTuple = new long[dimensions];
                for (int i = 0; i < dimensions; i++)
                {
                    newTuple[i] = (long)dbValuesObject[i];
                }

                bool isDominated = false;

                //check if record is dominated (compare against the records in the window)
                for (int i = resultCollection.Count - 1; i >= 0; i--)
                {
                    long[] windowTuple = resultCollection[i];
                    //string[] incomparableTuple = (string[])resultstringCollection[i];

                    //Don't use numberOfOperations for performance reasons (slows down performance about 10%)
                    //NumberOfOperations++;

                    //2 times faster than via the Template Strategy (below)
                    if(Helper.IsTupleDominated(windowTuple, newTuple, dimensions))
                    //if (IsTupleDominated(windowTuple, newTuple, dimensions, operatorsArray, resultstringCollection, i))
                    {
                        isDominated = true;
                        break;
                    }
                }
                if (isDominated == false)
                {
                    AddToWindow(dbValuesObject, resultCollection, resultstringCollection, operatorsArray, dimensions, dataTableReturn);
                }

                
            }
            //Special orderings need the skyline values. Store it in a property
            SkylineValues = resultCollection;
            return dataTableReturn;
        }



        //Attention: Profiling
        //It seems to makes sense to remove the parameter listIndex and pass the string-array incomparableTuples[listIndex]
        //Unfortunately this has negative impact on the performance for algorithms that don't work with incomparables
        protected abstract bool IsTupleDominated(long[] windowsTuple, long[] newTuple, int dimensions, string[] operators, ArrayList incomparableTuples, int listIndex);

        protected abstract void AddToWindow(object[] dataReader, List<long[]> resultCollection, ArrayList resultstringCollection, string[] operators, int dimensions, DataTable dtResult);

    }
}
