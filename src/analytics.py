from pyspark.sql.functions import col, count, countDistinct, desc, rank
from pyspark.sql.window import Window


# Analysis 1: Find the number of crashes (accidents) in which number of males killed are greater than 2.
def analysis_1(df):
    result = df.filter((col("PRSN_GNDR_ID") == "MALE") & (col("DEATH_CNT") > 2)) \
               .agg(countDistinct("CRASH_ID").alias("num_crashes"))
    return result

# Analysis 2: How many two-wheelers are booked for crashes?
def analysis_2(df):
    result = df.filter(col("VEH_BODY_STYL_ID").like("%MOTORCYCLE%")) \
               .agg(countDistinct("CRASH_ID").alias("num_two_wheeler_crashes"))
    return result

# Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
def analysis_3(df1, df2):
    result = df1.filter((col("PRSN_TYPE_ID") == "DRIVER") & (col("DEATH_CNT") > 0)) \
                .join(df2.filter(col("VEH_BODY_STYL_ID") == "PASSENGER CAR"), "CRASH_ID") \
                .filter(col("PRSN_AIRBAG_ID") == "NO AIR BAG") \
                .groupBy("VEH_MAKE_ID") \
                .agg(count("CRASH_ID").alias("num_crashes")) \
                .orderBy(desc("num_crashes")) \
                .limit(5)
    return result

# Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run.
def analysis_4(df1, df2):
    result = df1.filter(col("DRVR_LIC_TYPE_ID").isNotNull()) \
                .join(df2.filter(col("VEH_HNR_FL") == "Y"), "CRASH_ID") \
                .agg(countDistinct("CRASH_ID").alias("num_hit_and_run"))
    return result

# Analysis 5: Which state has highest number of accidents in which females are not involved?
def analysis_5(df):
    result = df.filter(col("PRSN_GNDR_ID") != "FEMALE") \
               .groupBy("DRVR_LIC_STATE_ID") \
               .agg(countDistinct("CRASH_ID").alias("num_crashes")) \
               .orderBy(desc("num_crashes")) \
               .limit(1)
    return result

# Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death?
def analysis_6(df1, df2):
    result = df1.join(df2, "CRASH_ID") \
                .groupBy("VEH_MAKE_ID") \
                .agg(count("*").alias("injury_death_count")) \
                .orderBy(desc("injury_death_count")) \
                .withColumn("rank", rank().over(Window.orderBy(desc("injury_death_count")))) \
                .filter((col("rank") >= 3) & (col("rank") <= 5)) \
                .select("VEH_MAKE_ID")
    return result

# Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style.
def analysis_7(df1, df2):
    result = df1.join(df2, "CRASH_ID") \
                .groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID") \
                .agg(count("*").alias("ethnic_group_count")) \
                .withColumn("rank", rank().over(Window.partitionBy("VEH_BODY_STYL_ID").orderBy(desc("ethnic_group_count")))) \
                .filter(col("rank") == 1) \
                .select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
    return result

# Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)?
def analysis_8(df1, df2):
    result = df1.filter(col("DRVR_ZIP").isNotNull()) \
                .filter(col("PRSN_ALC_RSLT_ID") == "Positive") \
                .join(df2, "CRASH_ID") \
                .groupBy("DRVR_ZIP") \
                .agg(count("*").alias("num_crashes")) \
                .orderBy(desc("num_crashes")) \
                .limit(5)
    return result

# Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance.
def analysis_9(df1, df2):
    result = df1.filter(col("DAMAGED_PROPERTY").isNull()) \
                .join(df2.filter((col("VEH_DMAG_SCL_1_ID") > 4) & (col("FIN_RESP_TYPE_ID").isNotNull())), "CRASH_ID") \
                .agg(countDistinct("CRASH_ID").alias("num_crashes"))
    return result

# Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences.
def analysis_10(df1, df2, df3):
    top_25_states = df2.groupBy("DRVR_LIC_STATE_ID") \
                       .agg(count("*").alias("offense_count")) \
                       .orderBy(desc("offense_count")) \
                       .limit(25) \
                       .select("DRVR_LIC_STATE_ID")

    top_10_colors = df3.groupBy("VEH_COLOR_ID") \
                       .agg(count("*").alias("color_count")) \
                       .orderBy(desc("color_count")) \
                       .limit(10) \
                       .select("VEH_COLOR_ID")

    result = df1.filter(col("CHARGE").like("%SPEED%")) \
                .join(df2.filter(col("DRVR_LIC_STATE_ID").isin([row["DRVR_LIC_STATE_ID"] for row in top_25_states.collect()])), "CRASH_ID") \
                .join(df3.filter((col("VEH_COLOR_ID").isin([row["VEH_COLOR_ID"] for row in top_10_colors.collect()]))), "CRASH_ID") \
                .groupBy("VEH_MAKE_ID") \
                .agg(count("*").alias("speeding_offense_count")) \
                .orderBy(desc("speeding_offense_count")) \
                .limit(5)
    return result
