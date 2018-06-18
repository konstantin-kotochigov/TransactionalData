# Load data

df = read_csv("C:\Users\konstantin\git\s7\tickets.csv")


# Set configuration

id_vars = "uid"
param_numeric_vars = ["km"]
param_cat_vars = ["destination_type"]
dt_var = "dt"


# Limit number of categories in nominal vars

max_categories = 10
for (categorical_var in param_cat_vars):
    var_top_categories = [x[0] for x in Counter(df[categorical_var]).most_common(max_categories)]
    df[categorical_var] = numpy.where(df[categorical_var].isin(top_categories),df[categorical_var], "other")



# Limit number of categories in categorical data
def groupCategoricalFeatures(df, maxFeatureCategories, otherCategory):
    
    catFeatures = df.select_dtypes("object").columns.values
    catFeaturesToGroup = [x for x in df[catFeatures].count() where x > maxFeatureCategories]
    
    print(catFeaturesToGroup)
    
    for catFeature in catFeaturesToGroup:
        featureTopCategories = [x[0] for x in Counter(df[catFeature]).most_common(maxFeatureCategories)]
        df[catFeature] = numpy.where(df[catFeature].isin(featureTopCategories), df[catFeature], otherCategory)

    return df    









df[dt_var] = pandas.to_datetime(df[dt_var].str.slice(0,7), format="%d%b%y")
df['dow'] = df[dt_var].dt.dayofweek.astype(str)

# Enumerate periods in data
min_trn_dt = df[dt_var].min()
df_raw['trx_period'] = ((df[dt_var] - min_trn_dt).dt.days / 7).map(math.floor)

df_dummies = pandas.get_dummies(df[param__cat_vars, prefix=param_cat_vars)
df_dummies['cnt'] = 1

dummy_cnt_columns = [x for x in df_dummies.columns]
dummy_sum_columns = [x + "_amount" for x in dummy_cnt_columns]
dummy_columns = dummy_cnt_columns + dummy_sum_columns
# df_dummies[dummy_sum_columns] = df_dummies[dumy_cnt_columns].apply(lambda x: if x==1: amount)
for i in range(0,len(dummy_cnt_columns)):
    df_dummies[dummy_sum_columns[i]] = numpy.where(df_dummies[dummy_cnt_columns[i]] == 1, df_dummies.amount, 0)
df_dummies.drop(['amount'], axis=1, inplace=True)


df = pandas.concat([df_raw, df_dummies], axis=1)




def processData(df, dateAttr, typeAttr, numPeriods, numericAttrs, catAttrs):
     
    # Create date base
    min_trn_dt = df_raw[dateAttr].min()
    df['period'] = ((df[dateAttr] - min_trn_dt).dt.days / 7).map(math.floor)
    dateBase = createDateBase("id","period")
    
    # Aggregate data by period
    dfPeriodAgg = aggregateData(df, numericAttrs, catAttrs)

    df = pandas.merge(dateBase, dfPeriodAgg, on=['cl_id','trx_week'], how='left', indicator=False)
    df[dummy_columns] = df[dummy_columns].fillna(0)

    reuturn df
    
def createDateBase(clientID, periodAttr):
    df_customers = DataFrame({"cl_id":df[clientID].unique(),"key":1})
    df_periods = DataFrame({"period":list(range(0,df['period'].max())), "key":1})
    df_period_base = pandas.merge(df_customers, df_weeks, on=["key"])
    df_period_base.drop(['key'],axis=1, inplace=True)
    return df_period_base