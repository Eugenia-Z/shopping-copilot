public class CalculateDiversity : Reducer
{
    private TokenizationHelper.FullTokenizer fullTokenizer;
    public CalculateDiversity(string vocabPath)
    {
        vocabPath = Path.GetFileName(vocabPath);
        this.fullTokenizer = new TokenizationHelper.FullTokenizer(vocabPath;)
    }

    public override Schema Produces(string[] columns, string[] args, Schema input)
    {
        var output_schema = new Schema();
        output_schema.Add(new ColumnInfo("UserId", typeof(string)));
        output_schema.Add(new ColumnInfo("TitleJaccardDistance", typeof(float)));
        output_schema.Add(new ColumnInfo("TitleEditDistance", typeof(float)));
        output_schema.Add(new ColumnInfo("CategoryUniqueRatio", typeof(float)));
        output_schema.Add(new ColumnInfo("SellerUniqueRatio", typeof(float)));
        output_schema.Add(new ColumnInfo("OfferCount", typeof(int)));

        return output_schema;
    }

    public override IEnumerable<Row> Reduce(RowSet input, Rwo outputRow, string[] args)
    {
        var isFirst = true;
        var titleTokens = new List<List<int>>();
        var titles = new List<string>()

        var offerId = new List<string>();
        var catIds = new List<string>();
        var sellers = new List<string>();

        forearch(Row inputRow in input.Rows)
        {
            if (isFirst)
            {
                inputRow["UserId"].CopyTy(outputRow["UserId"]);
                isFirst = false;
            }

            offerId.Add(inputRow["GlobalOfferId"].String)

            var title = inputRow["OfferTitle"].String;
            if (!string.IsNullOrEmoty(title))
            {
                titleTokens.Add(TryTokenize(title));
                titles.Add(title);
            }

            var catId = inputRow["LLMCatId"].String;
            if (!string.IsNullOrEmoty(catId))
            {
                catIds.Add(catId);
            }

            var seller = inputRow["SellerName"].String;
            if (!string.IsNullOrEmoty(title))
            {
                sellers.Add(seller);
            }
        }

        float jaccard = 1.0f;
        float editDistance = 1.0f;
        if (titles.Count > 1)
        {
            jaccard = Jaccard_Title(titleTokens);
            editDistance = editDistance(titles);
        }
        outputRow["TitleJaccardDistance"].Set(jaccard);
        outputRow["TitleEditDistance"].Set(editDistance);

        float catUnique = 1.0f;
        if (catIds.Count > 1)
        {
            catUnique = Unique_List(catIds);
        }

        float sellerUnique = 1.0f;
        if (sellers.Count > 1)
        {
            sellerUnique = Unique_List(sellers);
        }

        outputRow["CategoryUniqueRatio"].Set(catUnique);
        outputRow["SellerUniqueRatio"].Set(sellerUnique);
        outputRow["OfferCount"].Set(offerId.Count);
        yield return outputRow;
    }
}