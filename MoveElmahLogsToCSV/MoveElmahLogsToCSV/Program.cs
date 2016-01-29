using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.Queryable;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Xml.Linq;
using CsvHelper;
using System.Linq.Expressions;

namespace MoveElmahLogsToCSV
{
    class Program
    {
        static string[] headersFromErrorAttribute = new string[] { "type", "message", "user", "time", "statuscode", "source"};
        static string[] headersFromAllXml = new string[] { "HTTP_USER_AGENT", "LOCAL_ADDR", "PATH_INFO", "QUERY_STRING"};
        static void Main(string[] args)
        {
            DateTime date = DateTimeOffset.UtcNow.AddDays(-1).Date;
            if (args.Count() > 0)
            {
                date = DateTime.Parse(args[0]);
            }
           
            MakeCSV(date, false);
            MakeCSV(date, true);
        }

        private static void MakeCSV(DateTime date, bool is404)
        {
            string filename = date.ToString("yyyy_MM_dd") + ".csv";
            if (is404)
            {
                filename = date.ToString("yyyy_MM_dd") + "-404.csv";
            }
            using (StreamWriter streamWriter = new StreamWriter(filename))
            {
                CsvWriter writer = new CsvWriter(streamWriter);
                writer.Configuration.QuoteAllFields = true;
                foreach (var item in headersFromErrorAttribute)
                {
                    writer.WriteField(item);
                }
                foreach (var item in headersFromAllXml)
                {
                    writer.WriteField(item);
                }
                writer.NextRecord();
            
                var storageAccount = CloudStorageAccount.Parse(ConfigurationManager.ConnectionStrings["AzureStorage"].ConnectionString);
                CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

                // Create the table if it doesn't exist.
                CloudTable table = tableClient.GetTableReference("Elmah");
                TableQuery<ElmahEntity> rangeQuery = new TableQuery<ElmahEntity>().Where(
                        TableQuery.CombineFilters(
                            TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.GreaterThan, date.AddDays(-1)),
                            TableOperators.And,
                            TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.LessThan, date.AddDays(1))));
                int i = 0;
                foreach (ElmahEntity entity in table.ExecuteQuery(rangeQuery))
                {
                    bool isEntity404 = false;
                   
                    XElement doc = XElement.Parse(entity.AllXml);
                    if (doc.Attribute("statusCode") != null)
                    {
                        if (doc.Attribute("statusCode").Value == "404")
                        {
                            isEntity404 = true;
                        }
                    }
                    if (is404 != isEntity404)
                    {
                        continue;
                    }
                    writer.WriteField(entity.Type);
                    writer.WriteField(entity.Message);
                    writer.WriteField(entity.User);
                    writer.WriteField(entity.Timestamp);
                    if (doc.Attribute("statusCode") == null)
                    {
                        writer.WriteField("");
                    }
                    else
                    {
                        writer.WriteField(doc.Attribute("statusCode").Value);
                    }
                    writer.WriteField(entity.Source);
                    /*
                    if (doc.Attribute("detail") == null)
                    {
                        writer.WriteField("");
                    }
                    else
                    {
                        writer.WriteField(doc.Attribute("detail").Value);
                    }
                    */

                    foreach (var node in doc.Element("serverVariables").Elements().OrderBy(a => a.Attribute("name").Value))
                    {
                        if (headersFromAllXml.Contains(node.Attribute("name").Value))
                        {
                            if (node.Element("value") != null || node.Element("value").Attribute("string") != null)
                            {
                                    writer.WriteField(node.Element("value").Attribute("string").Value);
                            }
                            else
                            {
                                writer.WriteField("");
                            }
                        }
                    }
                    writer.NextRecord();
                    i++;
                }
            }
            Console.WriteLine("done");
            Console.ReadLine();

        }
        private static void DeleteAllEntitiesInBatches(CloudTable table, Expression<Func<DynamicTableEntity, bool>> filters)
        {
            Action<IEnumerable<DynamicTableEntity>> processor = entities =>
            {
                var batches = new Dictionary<string, TableBatchOperation>();

                foreach (var entity in entities)
                {
                    TableBatchOperation batch = null;

                    if (batches.TryGetValue(entity.PartitionKey, out batch) == false)
                    {
                        batches[entity.PartitionKey] = batch = new TableBatchOperation();
                    }

                    batch.Add(TableOperation.Delete(entity));

                    if (batch.Count == 100)
                    {
                        table.ExecuteBatch(batch);
                        batches[entity.PartitionKey] = new TableBatchOperation();
                    }
                }

                foreach (var batch in batches.Values)
                {
                    if (batch.Count > 0)
                    {
                        table.ExecuteBatch(batch);
                    }
                }
            };

            ProcessEntities(table, processor, filters);
        }

        private static void ProcessEntities(CloudTable table, Action<IEnumerable<DynamicTableEntity>> processor, Expression<Func<DynamicTableEntity, bool>> filters)
        {
            TableQuerySegment<DynamicTableEntity> segment = null;

            while (segment == null || segment.ContinuationToken != null)
            {
                if (filters == null)
                {
                    segment = table.ExecuteQuerySegmented(new TableQuery().Take(100), segment == null ? null : segment.ContinuationToken);
                }
                else
                {
                    var query = table.CreateQuery<DynamicTableEntity>().Where(filters).Take(100).AsTableQuery();
                    segment = query.ExecuteSegmented(segment == null ? null : segment.ContinuationToken);
                }

                processor(segment.Results);
            }
        }

    }
}
