using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;

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

        protected override DataTable GetSkylineFromAlgorithm(List<object[]> database, DataTable dataTableTemplate, string[] operatorsArray, string[] additionalParameters)
        {
            List<long[]> window = new List<long[]>();
            ArrayList windowIncomparable = new ArrayList();
            int dimensions = 0; //operatorsArray.GetUpperBound(0)+1;

            for (int i = 0; i < operatorsArray.Length; i++)
            {
                if (operatorsArray[i] != "IGNORE")
                {
                    dimensions++;
                }
            }

            DataTable dataTableReturn = dataTableTemplate.Clone();
            

            //For each tuple
            foreach (object[] dbValuesObject in database)
            {
                long[] newTuple = new long[dimensions];
                int next = 0;
                for (int j = 0; j < operatorsArray.Length; j++)
                {
                    if (operatorsArray[j] != "IGNORE")
                    {
                        newTuple[next] = (long) dbValuesObject[j];
                        next++;
                    }
                }

                /*long[] newTuple = new long[dimensions];
                for (int i = 0; i < dimensions; i++)
                {
                    newTuple[i] = (long)dbValuesObject[i];
                }*/
            

                bool isDominated = false;

                //check if record is dominated (compare against the records in the window)
                for (int i = window.Count - 1; i >= 0; i--)
                {
                    long[] windowTuple = window[i];
                    //Level BNL performance drops 2 times with using the next line
                    //string[] incomparableTuple = (string[])windowIncomparable[i];

                    //Only use this for tests (Drops performance 10%)
                    //NumberOfOperations++;

                    //TODO: Using the Helper directly is sligthly faster, but than every bnl algorithm needs it own logic
                    if(Helper.IsTupleDominated(windowTuple, newTuple, dimensions))
                    //Helper.IsTupleDominated()
                    //if (IsTupleDominated(windowTuple, newTuple, dimensions, operatorsArray, windowIncomparable, i))
                    {
                        isDominated = true;
                        break;
                    }
                }
                if (isDominated == false)
                {
                    Helper.AddToWindowSample(dbValuesObject, operatorsArray, window, dataTableReturn);
                    //AddToWindow(dbValuesObject, window, windowIncomparable, operatorsArray, dimensions, dataTableReturn);
                }

                
            }
            //Special orderings need the skyline values. Store it in a property
            SkylineValues = window;

            return dataTableReturn;
        }



        //Attention: Profiling
        //It seems to makes sense to remove the parameter listIndex and pass the string-array incomparableTuples[listIndex]
        //Unfortunately this has negative impact on the performance for algorithms that don't work with incomparables
        protected abstract bool IsTupleDominated(long[] windowsTuple, long[] newTuple, int dimensions, string[] operators, ArrayList incomparableTuples, int listIndex);

        protected abstract void AddToWindow(object[] dataReader, List<long[]> resultCollection, ArrayList resultstringCollection, string[] operators, int dimensions, DataTable dtResult);

    }
}
