#CS
using Microsoft.SCOPE.Types;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;
using System.Linq;
using Newtonsoft.Json;

public class ExtractRootCategoryProcessor : Processor
{
    // This processor extracts the root category from the full category path.
    public override Schema Produces(string[] requested_columns, string[] args, Schema input_schema)
    {
        var output_schema = input_schema.Clone();
        output_schema.Add(new ColumnInfo("RootCategory", typeof(string)));
        return output_schema;
    }

    public override IEnumerable<Row> Process(RowSet input_rowset, Row output_row, string[] args)
    {
        foreach (Row input_row in input_rowset.Rows)
        {
            input_row.CopyTo(output_row);
            string fullCategoryPath = input_row["full_category_path"].String;
            string rootCategory = fullCategoryPath.Split('|')[0];
            output_row["RootCategory"].Set(rootCategory);
            yield return output_row;
        }
    }
}

public class ExtractSpecsProdProcessor : Processor
{
    // This processor is used to convert the format of the json specification of prod to a flat format as in the cluster.
    // It also generates a separate column only for the filters.
    public class ValueData
    {
        public string Value { get; set; }
        public int Rank { get; set; }
        public string ImageThumbnailId { get; set; }
        public string colorHexCode { get; set; }
    }

    public class FilterData
    {
        public string Key { get; set; }
        public string id { get; set; }
        public string rank { get; set; }
        public bool userFacing { get; set; }
        public List<ValueData> Values { get; set; }
        public int FilterAttributeType { get; set; }
    }

    public override Schema Produces(string[] requested_columns, string[] args, Schema input_schema)
    {
        var output_schema = new Schema();
        output_schema.Add(new ColumnInfo("ProductId", typeof(string)));
        output_schema.Add(new ColumnInfo("GlobalOfferId", typeof(string)));
        output_schema.Add(new ColumnInfo("Title", typeof(string)));
        output_schema.Add(new ColumnInfo("CategoryName", typeof(string)));
        output_schema.Add(new ColumnInfo("ProbableSKUs", typeof(string)));
        output_schema.Add(new ColumnInfo("Filters", typeof(string)));
        return output_schema;
    }

    public override IEnumerable<Row> Process(RowSet input_rowset, Row output_row, string[] args)
    {
        foreach (Row input_row in input_rowset.Rows)
        {
            input_row["Id"].CopyTo(output_row["ProductId"]);
            input_row["GlobalOfferId"].CopyTo(output_row["GlobalOfferId"]);
            input_row["Title"].CopyTo(output_row["Title"]);
            input_row["CategoryName"].CopyTo(output_row["CategoryName"]);
            output_row["ProbableSKUs"].Set((string)null);
            output_row["Filters"].Set((string)null);

            List<FilterData> filters = new List<FilterData>();
            if (!string.IsNullOrEmpty(input_row["Specification"].String))
            {
                filters = Newtonsoft.Json.JsonConvert.DeserializeObject<List<FilterData>>(input_row["Specification"].String);
            }
            Dictionary<string, List<string>> outputspecs = new Dictionary<string, List<string>>();
            List<string> output_filters = new List<string>();
            foreach (FilterData filter in filters)
            {
                string key = filter.Key;
                output_filters.Add(key);
                List<string> values = new List<string>();
                foreach (ValueData filter_value in filter.Values)
                {
                    string value = filter_value.Value;
                    if (value == "UNTAGGED")
                    {
                        continue;
                    }
                    values.Add(value);
                }
                outputspecs[key] = values;
            }
            output_row["Filters"].Set(String.Join(",", output_filters));
            output_row["ProbableSKUs"].Set(Newtonsoft.Json.JsonConvert.SerializeObject(outputspecs));
            yield return output_row;
        }
    }
}

public class ExtractFiltersClusterProcessor : Processor
{
    // This processor is used to generate a separate column only for the filters.
    public override Schema Produces(string[] requested_columns, string[] args, Schema input_schema)
    {
        var output_schema = input_schema.Clone();
        output_schema.Add(new ColumnInfo("Filters", typeof(string)));
        return output_schema;
    }

    public override IEnumerable<Row> Process(RowSet input_rowset, Row output_row, string[] args)
    {
        foreach (Row input_row in input_rowset.Rows)
        {
            input_row.CopyTo(output_row);
            output_row["Filters"].Set((string)null);

            Dictionary<string, List<string>> filters = new Dictionary<string, List<string>>();
            if (!string.IsNullOrEmpty(input_row["ProbableSKUs"].String))
            {
                filters = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, List<string>>>(input_row["ProbableSKUs"].String);
            }
            List<string> output_filters = new List<string>(filters.Keys);
            output_row["Filters"].Set(String.Join(",", output_filters));
            yield return output_row;
        }
    }
}
#ENDCS