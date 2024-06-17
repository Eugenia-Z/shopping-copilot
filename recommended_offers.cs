using Microsoft.SCOPE.Types;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;
using Newtonsoft.Json;

class EvalScore
{
    public string Correlation_with_user_history;
    public string Recommended_Product_Quality;
    public string Recommended_Seller_Quality;
    public string Recency_of_Correlation_with_user_history;
    public string User_interest_density;
    public string Interestingness;
    public string Exploration;
    public string Overall_recommendation_score;
    public string Explanation;
}
public class GenerateScoreProcessor : Processor
{
    private Newtonsoft.Json.JsonSerializerSettings jsonSettings;

    public GenerateScoreProcessor()
    {
        this.jsonSettings = new Newtonsoft.Json.JsonSerializerSettings()
        {
            NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore
        };
    }

    public override Schema Produces(string[] requestedColumns, string[] args, Schema input)
    {
        Schema newSchema = new Schema();
        newSchema.Add(new ColumnInfo("UserId", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("IsRelevant", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("Scenario", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("SearchQuery", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("SearchQueryRank", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("OfferId", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("OfferTitle", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("SellerName", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("OriginalPrice", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("DealPrice", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("OfferUrl", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("ImageUrl", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("FinalRank", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("UserProfile", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("OverallScore", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("HistoryCorrelationScore", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("RecencyScore", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("InterestDensityScore", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("InterestingnessScore", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("ExplorationScore", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("ProductQualityScore", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("SellerQualityScore", ColumnDataType.String));
        newSchema.Add(new ColumnInfo("EvalExplanation", ColumnDataType.String));
        return newSchema;
    }
    public override IEnumerable<Row> Process(RowSet input, Row outputRow, string[] args)
    {
        foreach (Row row in input.Rows)
        {
            outputRow["UserId"].Set(row["UserId"].String);
            outputRow["IsRelevant"].Set(row["IsRelevant"].String);
            outputRow["Scenario"].Set(row["Scenario"].String);
            outputRow["SearchQuery"].Set(row["SearchQuery"].String);
            outputRow["SearchQueryRank"].Set(row["SearchQueryRank"].String);
            outputRow["OfferId"].Set(row["OfferId"].String);
            outputRow["OfferTitle"].Set(row["OfferTitle"].String);
            outputRow["SellerName"].Set(row["SellerName"].String);
            outputRow["OriginalPrice"].Set(row["OriginalPrice"].String);
            outputRow["DealPrice"].Set(row["DealPrice"].String);
            outputRow["OfferUrl"].Set(row["OfferUrl"].String);
            outputRow["ImageUrl"].Set(row["ImageUrl"].String);
            outputRow["FinalRank"].Set(row["FinalRank"].String);
            outputRow["UserProfile"].Set(row["UserProfile"].String);
            outputRow["EvalExplanation"].Set(row["EvalExplanation"].String);
            try
            {
                Dictionary<string, string> evalScore =
                    JsonConvert.DeserializeObject<Dictionary<string, string>>(row["EvalScore"].String, this.jsonSettings);
                outputRow["OverallScore"].Set(evalScore["Overall_recommendation_score"]);
                outputRow["HistoryCorrelationScore"].Set(evalScore["Correlation_with_user_history"]);
                outputRow["RecencyScore"].Set(evalScore["Recency_of_Correlation_with_user_history"]);
                outputRow["InterestDensityScore"].Set(evalScore["User_interest_density"]);
                outputRow["InterestingnessScore"].Set(evalScore["Interestingness"]);
                outputRow["ExplorationScore"].Set(evalScore["Exploration"]);
                outputRow["ProductQualityScore"].Set(evalScore["Recommended_Product_Quality"]);
                outputRow["SellerQualityScore"].Set(evalScore["Recommended_Seller_Quality"]);
            }
            catch (Exception)
            {
                outputRow["OverallScore"].Set(String.Empty);
                outputRow["HistoryCorrelationScore"].Set(String.Empty);
                outputRow["RecencyScore"].Set(String.Empty);
                outputRow["InterestDensityScore"].Set(String.Empty);
                outputRow["InterestingnessScore"].Set(String.Empty);
                outputRow["ExplorationScore"].Set(String.Empty);
                outputRow["ProductQualityScore"].Set(String.Empty);
                outputRow["SellerQualityScore"].Set(String.Empty);
            }
            yield return outputRow;

        }
    }
}