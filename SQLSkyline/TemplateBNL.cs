using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
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
            ArrayList resultCollection = new ArrayList();
            ArrayList resultstringCollection = new ArrayList();
            DataTable dataTableReturn = dataTableTemplate.Clone();
            int[] resultToTupleMapping = Helper.ResultToTupleMapping(operatorsArray);

            //For each tuple
            foreach (object[] dbValuesObject in database)
            {

                //Check if window list is empty
                if (resultCollection.Count == 0)
                {
                    // Build our SqlDataRecord and start the results 
                    AddtoWindow(dbValuesObject, operatorsArray, resultCollection, resultstringCollection, dataRecordTemplate, true, dataTableReturn);
                } else
                {
                    bool isDominated = false;

                    //check if record is dominated (compare against the records in the window)
                    for (int i = resultCollection.Count - 1; i >= 0; i--)
                    {
                        if (TupleDomination(dbValuesObject, resultCollection, resultstringCollection, operatorsArray, dataTableReturn, i, resultToTupleMapping))
                        {
                            isDominated = true;
                            break;
                        }
                    }
                    if (isDominated == false)
                    {
                        AddtoWindow(dbValuesObject, operatorsArray, resultCollection, resultstringCollection, dataRecordTemplate, true, dataTableReturn);
                    }

                }
            }
            return dataTableReturn;
        }



        protected abstract bool TupleDomination(object[] dataReader, ArrayList resultCollection, ArrayList resultstringCollection, string[] operators, DataTable dtResult, int i, int[] resultToTupleMapping);

        protected abstract void AddtoWindow(object[] dataReader, string[] operators, ArrayList resultCollection, ArrayList resultstringCollection, SqlDataRecord record, bool isFrameworkMode, DataTable dtResult);

    }
}
