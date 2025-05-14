STQD6324 Data Management Assignment 1 - What Distinguishes Popular
Businesses Per Sector Based on Yelp Reviews
================
Ng Chin Wen
2025-05-11

## R Markdown

This is an R Markdown document. Markdown is a simple formatting syntax
for authoring HTML, PDF, and MS Word documents. For more details on
using R Markdown see <http://rmarkdown.rstudio.com>.

When you click the **Knit** button a document will be generated that
includes both content as well as the output of any embedded R code
chunks within the document. You can embed an R code chunk like this:

``` r
a<-5
print(a)
```

    ## [1] 5

## Including Plots

You can also embed plots, for example:

![](ass1_files/figure-gfm/pressure-1.png)<!-- -->

Note that the `echo = FALSE` parameter was added to the code chunk to
prevent printing of the R code that generated the plot.

## 1.0 INTRODUCTION

The dataset was sourced from Yelp. In the initial compressed file is
another .tar file which contains the dataset. The yelp_dataset.tar file
contains 5 JSON files; business.json, checkin.json, tip.json, user.json,
review.json.

The business.json contains the following information

## DATA STORAGE

After unzipping the initial zip file from Yelp, there is another file
inside which is the tar file containing all the json files.

``` r
#after setting working directory
#untar("~/a_ncw/2manage/yelp_dataset.tar")
```

After that, to read the json file into a dataframe for RStudio.

``` r
#library(jsonlite)
```

``` r
#yelp_biz <- stream_in(file("yelp_academic_dataset_business.json"), verbose = TRUE)
```

``` r
#library(jsonlite)
#yelp_tip<-stream_in(file("yelp_academic_dataset_tip.json"), verbose = TRUE)
```

``` r
#saveRDS(yelp_biz, file = "yelp_biz.RDS")
#saveRDS(yelp_tip, file = "yelp_tip.RDS")
#saveRDS(yelp_tip, file = "yelp_checkin.RDS")
```

When using reticulate, since there was a specially made environment made
to interact with Hive, the conda environment has to be set to that from
whatever the default was.

``` r
reticulate::use_condaenv(condaenv ="C:/ProgramData/anaconda3/envs/ukm_stqd6324",required = TRUE)
```

After setting up the virtual machine, and putty, connect can be made via
python.

``` python
import pandas as pd
from impala.dbapi import connect

conn = connect(
    host='127.0.0.1',
    port=10000,
    user='maria_dev',
    database='default',
    auth_mechanism = 'PLAIN'
)
cursor = conn.cursor()
```

When looking at all the databases stored in Hive, the tables we stored
are in default.

``` python
cursor.execute('SHOW DATABASES')
print(cursor.fetchall())
```

    ## [('default',), ('foodmart',)]

Since reviews and users are very large (5GB and 3GB respectively), they
cannot easily be manipulated in R or Python. So, the files were moved to
the virtualbox maria_dev folder using
[WinSCP](https://winscp.net/eng/download.php) to transfer all 5 JSON
files over. Then all the files were converted into Hive tables using
Spark.

![](images/clipboard-4142044237.png)

Another example: when turning the checkin json to a hive table.

![](images/clipboard-3799294681.png)

## Example of querying

1.  Connect and run SQL query in python and turn it into a pd dataframe.

``` python
cursor.execute('SELECT * FROM yelp_review LIMIT 10')

# Fetch column names and rows
columns = [desc[0] for desc in cursor.description]

# Store as pandas DataFrame
yelp_df = pd.DataFrame(cursor.fetchall(), columns=columns)
yelp_df
```

    ##   yelp_review.business_id  ...     yelp_review.user_id
    ## 0  XQfwVwDr-v0ZS3_CbbE5Xw  ...  mh_-eMZ6K5RLWhZyISBhwA
    ## 1  7ATYjTIgM3jUlt4UM3IypQ  ...  OyoGAe7OKpv6SyGZT5g77Q
    ## 2  YjUWPpI6HXG530lwP-fb2A  ...  8g_iMtfSiwikVnbP2etR0A
    ## 3  kxX2SOes4o-D3ZQBkiMRfA  ...  _7bHUi9Uuf5__HHc_Q8guQ
    ## 4  e4Vwtrqf-wpJfwesgvdgxQ  ...  bcjbaE6dDog4jkNY91ncLQ
    ## 5  04UD14gamNjLY0IDYVhHJg  ...  eUta8W_HdHMXPzLBBZhL1A
    ## 6  gmjsEdUsKpj9Xxu6pdjH0g  ...  r3zeYsv1XFBRA4dJpL78cw
    ## 7  LHSTtnW3YHCeUkRDGyJOyw  ...  yfFzsLmaWF2d4Sr0UNbBgg
    ## 8  B5XSoSG3SfvQGtKEGQ1tSQ  ...  wSTuiTk-sKNdcFyprzZAjg
    ## 9  gebiRewfieSdtt17PTW6Zg  ...  59MxRhNVhU9MYndMkz0wtw
    ## 
    ## [10 rows x 9 columns]

2.  After, using the reticulate package and py\$, transfer it over as an
    R object for further data manipulation

``` r
a<-as.data.frame(reticulate::py$yelp_df)
a
```

    ##    yelp_review.business_id yelp_review.cool    yelp_review.date
    ## 1   XQfwVwDr-v0ZS3_CbbE5Xw                0 2018-07-07 22:09:11
    ## 2   7ATYjTIgM3jUlt4UM3IypQ                1 2012-01-03 15:28:18
    ## 3   YjUWPpI6HXG530lwP-fb2A                0 2014-02-05 20:30:30
    ## 4   kxX2SOes4o-D3ZQBkiMRfA                1 2015-01-04 00:01:03
    ## 5   e4Vwtrqf-wpJfwesgvdgxQ                1 2017-01-14 20:54:15
    ## 6   04UD14gamNjLY0IDYVhHJg                1 2015-09-23 23:10:31
    ## 7   gmjsEdUsKpj9Xxu6pdjH0g                0 2015-01-03 23:21:18
    ## 8   LHSTtnW3YHCeUkRDGyJOyw                0 2015-08-07 02:29:16
    ## 9   B5XSoSG3SfvQGtKEGQ1tSQ                0 2016-03-30 22:46:33
    ## 10  gebiRewfieSdtt17PTW6Zg                0 2016-07-25 07:31:06
    ##    yelp_review.funny  yelp_review.review_id yelp_review.stars
    ## 1                  0 KU_O5udG6zpxOg-VcAEodg                 3
    ## 2                  0 BiTunyQ73aT9WBnpR9DZGw                 5
    ## 3                  0 saUsX_uimxRlCVr67Z4Jig                 3
    ## 4                  0 AqPFMleE6RsU23_auESxiA                 5
    ## 5                  0 Sx8TMOWLNuJBWer-0pcmoA                 4
    ## 6                  2 JrIxlS1TzJ-iCu79ul40cQ                 1
    ## 7                  2 6AxgBCNX_PNTOxmbRSwcKQ                 5
    ## 8                  0 _ZeMknuYdlQcUqng_Im3yg                 5
    ## 9                  1 ZKvDG2sBvHVdF5oBNUOpAQ                 3
    ## 10                 0 pUycOfUwM8vqX7KjRRhUEA                 3
    ##                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     yelp_review.text
    ## 1                                                                                                                                                                                                                                                                                                                                If you decide to eat here, just be aware it is going to take about 2 hours from beginning to end. We have tried it multiple times, because I want to like it! I have been to it's other locations in NJ and never had a bad experience. \n\nThe food is good, but it takes a very long time to come out. The waitstaff is very young, but usually pleasant. We have just had too many experiences where we spent way too long waiting. We usually opt for another diner or restaurant on the weekends, in order to be done quicker.
    ## 2  I've taken a lot of spin classes over the years, and nothing compares to the classes at Body Cycle. From the nice, clean space and amazing bikes, to the welcoming and motivating instructors, every class is a top notch work out.\n\nFor anyone who struggles to fit workouts in, the online scheduling system makes it easy to plan ahead (and there's no need to line up way in advanced like many gyms make you do).\n\nThere is no way I can write this review without giving Russell, the owner of Body Cycle, a shout out. Russell's passion for fitness and cycling is so evident, as is his desire for all of his clients to succeed. He is always dropping in to classes to check in/provide encouragement, and is open to ideas and recommendations from anyone. Russell always wears a smile on his face, even when he's kicking your butt in class!
    ## 3                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                Family diner. Had the buffet. Eclectic assortment: a large chicken leg, fried jalapeño, tamale, two rolled grape leaves, fresh melon. All good. Lots of Mexican choices there. Also has a menu with breakfast served all day long. Friendly, attentive staff. Good place for a casual relaxed meal with no expectations. Next to the Clarion Hotel.
    ## 4                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                Wow!  Yummy, different,  delicious.   Our favorite is the lamb curry and korma.  With 10 different kinds of naan!!!  Don't let the outside deter you (because we almost changed our minds)...go in and try something new!   You'll be glad you did!
    ## 5                                                                                                                                                                                                                                                                                                             Cute interior and owner (?) gave us tour of upcoming patio/rooftop area which will be great on beautiful days like today. Cheese curds were very good and very filling. Really like that sandwiches come w salad, esp after eating too many curds! Had the onion, gruyere, tomato sandwich. Wasn't too much cheese which I liked. Needed something else...pepper jelly maybe. Would like to see more menu options added such as salads w fun cheeses. Lots of beer and wine as well as limited cocktails. Next time I will try one of the draft wines.
    ## 6                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              I am a long term frequent customer of this establishment. I just went in to order take out (3 apps) and was told they're too busy to do it. Really? The place is maybe half full at best. Does your dick reach your ass? Yes? Go fuck yourself! I'm a frequent customer AND great tipper. Glad that Kanella just opened. NEVER going back to dmitris!
    ## 7                               Loved this tour! I grabbed a groupon and the price was great. It was the perfect way to explore New Orleans for someone who'd never been there before and didn't know a lot about the history of the city. Our tour guide had tons of interesting tidbits about the city, and I really enjoyed the experience. Highly recommended tour. I actually thought we were just going to tour through the cemetery, but she took us around the French Quarter for the first hour, and the cemetery for the second half of the tour. You'll meet up in front of a grocery store (seems strange at first, but it's not terribly hard to find, and it'll give you a chance to get some water), and you'll stop at a visitor center part way through the tour for a bathroom break if needed. This tour was one of my favorite parts of my trip!
    ## 8                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   Amazingly amazing wings and homemade bleu cheese. Had the ribeye: tender, perfectly prepared, delicious. Nice selection of craft beers. Would DEFINITELY recommend checking out this hidden gem.
    ## 9                                                                                                                                                                                                                                                                                                                     This easter instead of going to Lopez Lake we went to Los Padres National Forest which is really pretty but if you go to white rock the staff needs to cut down all the dead grass that invades the rock and the water. I would wish the staff would also clean or get rid of the dead grass that's also living by the water. The water is really green and dirty. Los padres national forest staff need to work hard to maintain this forest looking pretty and not like a dumpster. Even Cachuma lake looks like they put a bit more effort.
    ## 10                                                                                                                                                                                                                                                                                                                      Had a party of 6 here for hibachi. Our waitress brought our separate sushi orders on one plate so we couldn't really tell who's was who's and forgot several items on an order. I understand making mistakes but the restaraunt was really quiet so we were kind of surprised. Usually hibachi is a fun lively experience and our  cook  said maybe three words, but he cooked very well his name was Francisco. Service was fishy, food was pretty good, and im hoping it was just an off night here. But for the money I wouldn't go back.
    ##    yelp_review.useful    yelp_review.user_id
    ## 1                   0 mh_-eMZ6K5RLWhZyISBhwA
    ## 2                   1 OyoGAe7OKpv6SyGZT5g77Q
    ## 3                   0 8g_iMtfSiwikVnbP2etR0A
    ## 4                   1 _7bHUi9Uuf5__HHc_Q8guQ
    ## 5                   1 bcjbaE6dDog4jkNY91ncLQ
    ## 6                   1 eUta8W_HdHMXPzLBBZhL1A
    ## 7                   0 r3zeYsv1XFBRA4dJpL78cw
    ## 8                   2 yfFzsLmaWF2d4Sr0UNbBgg
    ## 9                   1 wSTuiTk-sKNdcFyprzZAjg
    ## 10                  0 59MxRhNVhU9MYndMkz0wtw

This is the columns

``` python
cursor.execute('DESCRIBE yelp_business')
print(cursor.fetchall())
```

    ## [('address', 'string', ''), ('attributes', 'struct<AcceptsInsurance:string,AgesAllowed:string,Alcohol:string,Ambience:string,BYOB:string,BYOBCorkage:string,BestNights:string,BikeParking:string,BusinessAcceptsBitcoin:string,BusinessAcceptsCreditCards:string,BusinessParking:string,ByAppointmentOnly:string,Caters:string,CoatCheck:string,Corkage:string,DietaryRestrictions:string,DogsAllowed:string,DriveThru:string,GoodForDancing:string,GoodForKids:string,GoodForMeal:string,HairSpecializesIn:string,HappyHour:string,HasTV:string,Music:string,NoiseLevel:string,Open24Hours:string,OutdoorSeating:string,RestaurantsAttire:string,RestaurantsCounterService:string,RestaurantsDelivery:string,RestaurantsGoodForGroups:string,RestaurantsPriceRange2:string,RestaurantsReservations:string,RestaurantsTableService:string,RestaurantsTakeOut:string,Smoking:string,WheelchairAccessible:string,WiFi:string>', ''), ('business_id', 'string', ''), ('categories', 'string', ''), ('city', 'string', ''), ('hours', 'struct<Friday:string,Monday:string,Saturday:string,Sunday:string,Thursday:string,Tuesday:string,Wednesday:string>', ''), ('is_open', 'bigint', ''), ('latitude', 'double', ''), ('longitude', 'double', ''), ('name', 'string', ''), ('postal_code', 'string', ''), ('review_count', 'bigint', ''), ('stars', 'double', ''), ('state', 'string', '')]

Exploratory Data Analysis, when selecting for name of business and id,
we ordered by stars in descending order followed by review_count in
descending order, ensuring only businesses with the highest rating from
the highest number of reviews is displayed.

``` python
cursor.execute('SELECT name, business_id, stars, review_count FROM yelp_business ORDER BY stars DESC, review_count DESC LIMIT 10')
print(cursor.fetchall())
```

    ## [('Blues City Deli', '_aKr7POnacW_VizRKBpCiA', 5.0, 991), ('Carlillos Cocina', '8QqnRpM-QxGsjDNuu0E57A', 5.0, 799), ('Free Tours By Foot', 'zxIF-bnaJ-eKIsznB7yu7A', 5.0, 769), ('Tumerico', 'DVBJRvnCpkqaYl6nHroaMg', 5.0, 705), ('Yats', 'gP_oWJykA2RocIs_GurKWQ', 5.0, 623), ("Nelson's Green Brier Distillery", 'JbzvJJolDBT1614qo2Yiaw', 5.0, 545), ('Smiling With Hope Pizza', 'OR7VJQ3Nk1wCcIbPN4TCQQ', 5.0, 526), ('Barracuda Deli Cafe St. Pete Beach', 'FHDuu5Mv1bEkusxEuhptZQ', 5.0, 521), ('SUGARED + BRONZED', 'l_7TW_Ix58-QvhQgpJi_Xw', 5.0, 513), ('Cafe Soleil', 'D9p7-HsY9llYP3BaCVg4DA', 5.0, 468)]

After that, when we look at the columns for yelp_review:

``` python
cursor.execute('DESCRIBE yelp_review')
print(cursor.fetchall())
```

    ## [('business_id', 'string', ''), ('cool', 'bigint', ''), ('date', 'string', ''), ('funny', 'bigint', ''), ('review_id', 'string', ''), ('stars', 'double', ''), ('text', 'string', ''), ('useful', 'bigint', ''), ('user_id', 'string', '')]

When running `ANALYZE TABLE yelp_review COMPUTE STATISTICS;` in Ambari’s
Hive’s Query Editor and clicking Explain, we see that the number of
reviews is 6990280, matching the number of reviews as mentioned on the
Yelp website.

![](images/clipboard-3413348418.png)

``` python
cursor.execute('SELECT b.name, b.business_id, AVG(r.stars) AS avg_rating, COUNT(*) AS num_reviews FROM yelp_business b JOIN yelp_review r ON b.business_id = r.business_id GROUP BY b.name, b.business_id ORDER BY avg_rating DESC, num_reviews DESC LIMIT 10')
print(cursor.fetchall())
```

    ## [('Walls Jewelry Repairing', '1RqfozJoosHAsKZhc5PY7w', 5.0, 114), ('ella & louie flowers', '-siOxQQcGKtb-04dX0Cxnw', 5.0, 104), ('Drink & Learn', '4-P4Bzqd01YvKX9tp7IGfQ', 5.0, 90), ('Steves iPhone Repair', 'dhLARBhUnJloLa8xZ1Stpw', 5.0, 78), ('Jeramie Lu Photography', 'LTqm4uY4GIYHfzuh5pVZNQ', 5.0, 77), ('Twisted Twig Fine Florals', 'x-SCuOwghy4GlZdVOKjt4g', 5.0, 74), ('ByCherry Photography', '2FQoAp9w0G_NhuZMqo9bfA', 5.0, 73), ('Carpinteria Lock & Key', 'DnkpXhc5DKdeBT-0jG13JQ', 5.0, 70), ('Jack & Melody Cote - Chase International, Reno Real Estate', 'NLx84Im37HcZkcFBO2cxlw', 5.0, 69), ('Dj Hecktik', '_LHkm-m9a_IDqfqfdWggHQ', 5.0, 68)]

``` python
cursor.execute("describe extended yelp_review")
print(cursor.fetchall())
```

    ## [('business_id', 'string', ''), ('cool', 'bigint', ''), ('date', 'string', ''), ('funny', 'bigint', ''), ('review_id', 'string', ''), ('stars', 'double', ''), ('text', 'string', ''), ('useful', 'bigint', ''), ('user_id', 'string', ''), ('', None, None), ('Detailed Table Information', 'Table(tableName:yelp_review, dbName:default, owner:maria_dev, createTime:1747054806, lastAccessTime:0, retention:0, sd:StorageDescriptor(cols:[FieldSchema(name:business_id, type:string, comment:null), FieldSchema(name:cool, type:bigint, comment:null), FieldSchema(name:date, type:string, comment:null), FieldSchema(name:funny, type:bigint, comment:null), FieldSchema(name:review_id, type:string, comment:null), FieldSchema(name:stars, type:double, comment:null), FieldSchema(name:text, type:string, comment:null), FieldSchema(name:useful, type:bigint, comment:null), FieldSchema(name:user_id, type:string, comment:null)], location:hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/yelp_review, inputFormat:org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat, outputFormat:org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat, compressed:false, numBuckets:-1, serdeInfo:SerDeInfo(name:null, serializationLib:org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe, parameters:{serialization.format=1, path=hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/yelp_review}), bucketCols:[], sortCols:[], parameters:{}, skewedInfo:SkewedInfo(skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{}), storedAsSubDirectories:false), partitionKeys:[], parameters:{totalSize=2988224046, numRows=6990280, rawDataSize=62912520, COLUMN_STATS_ACCURATE={"BASIC_STATS":"true","COLUMN_STATS":{"business_id":"true"}}, spark.sql.sources.schema.part.0={"type":"struct","fields":[{"name":"business_id","type":"string","nullable":true,"metadata":{}},{"name":"cool","type":"long","nullable":true,"metadata":{}},{"name":"date","type":"string","nullable":true,"metadata":{}},{"name":"funny","type":"long","nullable":true,"metadata":{}},{"name":"review_id","type":"string","nullable":true,"metadata":{}},{"name":"stars","type":"double","nullable":true,"metadata":{}},{"name":"text","type":"string","nullable":true,"metadata":{}},{"name":"useful","type":"long","nullable":true,"metadata":{}},{"name":"user_id","type":"string","nullable":true,"metadata":{}}]}, numFiles=40, transient_lastDdlTime=1747191062, spark.sql.sources.schema.numParts=1, spark.sql.sources.provider=parquet, spark.sql.create.version=2.3.0.2.6.5.0-292}, viewOriginalText:null, viewExpandedText:null, tableType:MANAGED_TABLE)', '')]

``` python
cursor.execute('DESCRIBE extended yelp_user')
print(cursor.fetchall())
```

    ## [('average_stars', 'double', ''), ('compliment_cool', 'bigint', ''), ('compliment_cute', 'bigint', ''), ('compliment_funny', 'bigint', ''), ('compliment_hot', 'bigint', ''), ('compliment_list', 'bigint', ''), ('compliment_more', 'bigint', ''), ('compliment_note', 'bigint', ''), ('compliment_photos', 'bigint', ''), ('compliment_plain', 'bigint', ''), ('compliment_profile', 'bigint', ''), ('compliment_writer', 'bigint', ''), ('cool', 'bigint', ''), ('elite', 'string', ''), ('fans', 'bigint', ''), ('friends', 'string', ''), ('funny', 'bigint', ''), ('name', 'string', ''), ('review_count', 'bigint', ''), ('useful', 'bigint', ''), ('user_id', 'string', ''), ('yelping_since', 'string', ''), ('', None, None), ('Detailed Table Information', 'Table(tableName:yelp_user, dbName:default, owner:maria_dev, createTime:1747120024, lastAccessTime:0, retention:0, sd:StorageDescriptor(cols:[FieldSchema(name:average_stars, type:double, comment:null), FieldSchema(name:compliment_cool, type:bigint, comment:null), FieldSchema(name:compliment_cute, type:bigint, comment:null), FieldSchema(name:compliment_funny, type:bigint, comment:null), FieldSchema(name:compliment_hot, type:bigint, comment:null), FieldSchema(name:compliment_list, type:bigint, comment:null), FieldSchema(name:compliment_more, type:bigint, comment:null), FieldSchema(name:compliment_note, type:bigint, comment:null), FieldSchema(name:compliment_photos, type:bigint, comment:null), FieldSchema(name:compliment_plain, type:bigint, comment:null), FieldSchema(name:compliment_profile, type:bigint, comment:null), FieldSchema(name:compliment_writer, type:bigint, comment:null), FieldSchema(name:cool, type:bigint, comment:null), FieldSchema(name:elite, type:string, comment:null), FieldSchema(name:fans, type:bigint, comment:null), FieldSchema(name:friends, type:string, comment:null), FieldSchema(name:funny, type:bigint, comment:null), FieldSchema(name:name, type:string, comment:null), FieldSchema(name:review_count, type:bigint, comment:null), FieldSchema(name:useful, type:bigint, comment:null), FieldSchema(name:user_id, type:string, comment:null), FieldSchema(name:yelping_since, type:string, comment:null)], location:hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/yelp_user, inputFormat:org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat, outputFormat:org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat, compressed:false, numBuckets:-1, serdeInfo:SerDeInfo(name:null, serializationLib:org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe, parameters:{serialization.format=1, path=hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/yelp_user}), bucketCols:[], sortCols:[], parameters:{}, skewedInfo:SkewedInfo(skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{}), storedAsSubDirectories:false), partitionKeys:[], parameters:{totalSize=2598945818, spark.sql.sources.schema.part.0={"type":"struct","fields":[{"name":"average_stars","type":"double","nullable":true,"metadata":{}},{"name":"compliment_cool","type":"long","nullable":true,"metadata":{}},{"name":"compliment_cute","type":"long","nullable":true,"metadata":{}},{"name":"compliment_funny","type":"long","nullable":true,"metadata":{}},{"name":"compliment_hot","type":"long","nullable":true,"metadata":{}},{"name":"compliment_list","type":"long","nullable":true,"metadata":{}},{"name":"compliment_more","type":"long","nullable":true,"metadata":{}},{"name":"compliment_note","type":"long","nullable":true,"metadata":{}},{"name":"compliment_photos","type":"long","nullable":true,"metadata":{}},{"name":"compliment_plain","type":"long","nullable":true,"metadata":{}},{"name":"compliment_profile","type":"long","nullable":true,"metadata":{}},{"name":"compliment_writer","type":"long","nullable":true,"metadata":{}},{"name":"cool","type":"long","nullable":true,"metadata":{}},{"name":"elite","type":"string","nullable":true,"metadata":{}},{"name":"fans","type":"long","nullable":true,"metadata":{}},{"name":"friends","type":"string","nullable":true,"metadata":{}},{"name":"funny","type":"long","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"review_count","type":"long","nullable":true,"metadata":{}},{"name":"useful","type":"long","nullable":true,"metadata":{}},{"name":"user_id","type":"string","nullable":true,"metadata":{}},{"name":"yelping_since","type":"string","nullable":true,"metadata":{}}]}, numFiles=26, transient_lastDdlTime=1747120024, spark.sql.sources.schema.numParts=1, spark.sql.sources.provider=parquet, spark.sql.create.version=2.3.0.2.6.5.0-292}, viewOriginalText:null, viewExpandedText:null, tableType:MANAGED_TABLE)', '')]

I;m not sure
