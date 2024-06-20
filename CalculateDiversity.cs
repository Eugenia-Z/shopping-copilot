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

    public static float Unique_List(List<string> data)
    {
        HashSet<string> set = new HashSet<string>(data);
        float sum = (float)set.Count;
        int cnt = data.Count;
        return sum / cnt;
    }

    public static float Jaccard_Title(List<List<int>> titleTokens)
    {
        float jaccard_sum = 0.0f;
        int cnt = 0;
        for (var i = 0; i < titleTokens.Count; i++)
        {
            for (var j = i + 1; k < titleTokens.Count; j++)
            {
                if (i != j)
                {
                    cnt += 1;
                    jaccard_sum += 1 - JaccardBetweenTokens(titleTokens[1], titleTokens[j]);
                }
            }
        }
        return jaccard_sum / cnt;
    }

    public static float JaccardBetweenTokens(List<int> token1, List<int> token2)
    {
        vat intersectedList = token1.Intersect(token2).toList();
        var unionList = token1.Union(token2).toList();
        return (float)intersectedList.Count / (float)unionList.Count;
    }

    private List<int> TryTokenize(string text)
    {
        if (string.IsNullOrEmpty(test))
        {
            return new List<int>();
        }
        try
        {
            var tokenizationResult = fullTokenizer.Tokenize(text);
            var tokenIdResult = fullTokenizer.ConvertTokensToIds(tokenizationResult);
            return tokenIdResult;
        }
        catch (Exception)
        {
            return new List<int>();
        }
    }

    public static float EditDistance(List<string> titles)
    {
        float editDistanceSum = 0.0f;
        int cnt = 0;
        for (var i = 0; i < titles.Count; i++)
        {
            for (var j = i + 1, j < titles.Count; j++)
            {
                if (i != j)
                {
                    cnt += 1;
                    editDistanceSum += EditDistanceBetweenWords(titles[i], titles[j]);
                }
            }
        }
        return editDistanceSum / cnt;
    }

    public static float EditDistanceBetweenWords(string str1, string str2)
    {
        float distance = 0.0f;
        int[,] Matrix;
        int n = str1.Length;
        int m = str2.Length;

        int temp = 0;
        char ch1;
        char ch2;
        int i = 0;
        int j = 0;
        if (n == 0)
        {
            return m;
        }
        if (m == 0)
        {
            return n;
        }
        Matrix = new int[n + 1, m + 1];
        for (i = 0; i <= n; i++)
        {
            Matrix[i, 0] = i;
        }
        for (j = 0; j <= m; j++)
        {
            Matrix[0, j] = j;
        }
        for (i = 1; i <= n; i++)
        {
            ch1 = str1[i - 1];
            for (j = 1; j <= m; j++)
            {
                ch2 = str2[j - 1];
                if (ch1.Equals(ch2))
                {
                    temp = 0;
                }
                else
                {
                    temp = 1;
                }
                Matrix[i, j] = LowerOfThree(Matrix[i - 1, j] + 1, Matrix[i, j - 1] + 1, Matrix[i - 1, j - 1] + temp);
            }
        }
        distance = (float)Matrix[n, m] / (float)(n > m ? n : m);
        return distance;
    }

    public static int LowerOfThree(int first, int second, int third)
    {
        int min = first;
        if (second < min)
        {
            min = second;
        }
        if (third < min)
        {
            min = third;
        }
        return min;
    }
}