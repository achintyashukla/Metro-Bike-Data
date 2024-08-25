import os
import configparser
from datetime import datetime
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import broadcast


class SparkAnalysis:

    def __init__(self, conf_file='app.conf'):
        try:
            dir_path = os.path.dirname(os.path.realpath(__file__))
            options = self.get_config(os.path.join(dir_path, conf_file))
            if options:
                hdfs_read = bool(options.get("hdfs_read"))
                if hdfs_read:
                    self.file_url = options.get("hdfs_file_url")
                else:
                    self.file_url = options.get("local_data_dir")
                print(self.file_url)
                self.db_host = options.get("db_host")
                self.db_user = options.get("db_user")
                self.db_password = options.get("db_password")
                self.db_database = options.get("db_database")
                sc = SparkContext.getOrCreate()
                self.spark = SparkSession(sc)
        except Exception as e:
            print(e)

    def get_config(self, conf_file):
        config = configparser.ConfigParser()
        config.read(conf_file)
        return config['options']

    def read_csv_file_create_schema(self):
        table_name = "metro_bike_data"
        metro_bike_df = None
        try:
            ss_schema = StructType([StructField('trip_id', IntegerType(), True),
                                    StructField('duration', IntegerType(), True),
                                    StructField('start_time', StringType(), True),
                                    StructField('end_time', StringType(), True),
                                    StructField('start_station', IntegerType(), True),
                                    StructField('start_lat', DoubleType(), True),
                                    StructField('start_lon', DoubleType(), True),
                                    StructField('end_station', IntegerType(), True),
                                    StructField('end_lat', DoubleType(), True),
                                    StructField('end_lon', DoubleType(), True),
                                    StructField('bike_id', StringType(), True),
                                    StructField('plan_duration', IntegerType(), True),
                                    StructField('trip_route_category', StringType(), True),
                                    StructField('passholder_type', StringType(), True),
                                    StructField('bike_type', StringType(), True),
                                    ])
            path = self.file_url
            os.chdir(path)
            li = []
            li2 = []
            li3 = [path+"/metro-trips-2020-q4.csv", path+"/metro-trips-2021-q2.csv", path+"/metro-trips-2021-q1.csv"]
            match = 'q'
            match2 = '.csv'
            for file in os.listdir():
                if match in file.lower():
                    if match2 in file.lower():
                        file = path + file
                        li.append(file)
                else:
                    if match2 in file:
                        file = path + file
                        li2.append(file)

            metro_bike_preprocessed_df = self.spark.read.format("csv"). \
                option("header", "true").options(schema=ss_schema).load(li)
            # udf_f1 = udf(lambda x: datetime.strptime(x, '%m/%d/%Y %H:%M'), TimestampType())
            # metro_bike_df = metro_bike_preprocessed_df.withColumn("start_time", udf_f1(col('start_time')))
            # metro_bike_df = metro_bike_df.withColumn("end_time", udf_f1(col('end_time')))
            dff = metro_bike_preprocessed_df.select(col("*"),
                                                    when(to_date(col("start_time"), "yyyy-MM-dd HH:mm:ss").isNotNull(),
                                                         to_date(col("start_time"), "yyyy-MM-dd HH:mm:ss"))

                                                    .when(to_date(col("start_time"), "M/d/yyyy H:mm").isNotNull(),
                                                          to_date(col("start_time"), "M/d/yyyy H:mm"))
                                                    .alias("start_station_time"))
            df = dff.drop("start_time")
            df1 = df.select(col("*"),
                            when(to_date(col("end_time"), "yyyy-MM-dd HH:mm:ss").isNotNull(),
                                 to_date(col("end_time"), "yyyy-MM-dd HH:mm:ss"))

                            .when(to_date(col("end_time"), "M/d/yyyy H:mm").isNotNull(),
                                  to_date(col("end_time"), "M/d/yyyy H:mm"))
                            .alias("end_station_time"))
            metro_df1 = df1.drop("end_time")
            # -----------------------------------------------------------------------------------------------
            df_3 = self.spark.read.format("csv").option("header", "true").options(schema=ss_schema).load(li3)
            df_3.createOrReplaceTempView("table_1")
            df_bikeid = self.spark.sql("select distinct(bike_id) as bike_id,bike_type from table_1 order by bike_id ")
            metro_bike_df1 = metro_df1 .join(broadcast(df_bikeid), ['bike_id']).\
                select(metro_df1['*'], df_bikeid['bike_type'].alias("bike_type"))
            # --------------------------------------------------------------------------------------------------------
            df3 = self.spark.read.option("inferSchema", "true").csv(li2).toDF(
                "station_id", "station_name", "station_date", "station_type", "current_status")

            df_new = metro_bike_df1.join(broadcast(df3), [metro_bike_df1.start_station == df3.station_id]) \
                .select(metro_bike_df1['*'], df3['station_name'].alias("start_station_name"))

            metro_bike_df = df_new.join(broadcast(df3), [df_new.end_station == df3.station_id]).select(df_new['*'], df3[
                'station_name'].alias("end_station_name"))

            if metro_bike_df:
                metro_bike_df.createOrReplaceTempView(table_name)
        except Exception as e:
            print(e)
        return metro_bike_df, table_name

    def store_data_in_database(self, df, table_name):
        try:
            df.write.format('jdbc').options(
                url='jdbc:mysql://{0}:3306/{1}'.format(self.db_host, self.db_database),
                driver='com.mysql.cj.jdbc.Driver',
                dbtable='{0}'.format(table_name),
                user='{0}'.format(self.db_user),
                password='{0}'.format(self.db_password)).mode('overwrite').save()
            print("Data stored in table {0}".format(table_name))
            print("#" * 40)
            print("\n" * 4)
        except Exception as e:
            print(e)

    def get_store_total_users(self, table_name):
        try:
            """1.--------------To calculate the total TRIPS per month/year-------------"""
            _sql = """select to_date(date_format(start_station_time, '01 MMM y'),'dd MMM y')as month_year,
             count(trip_id)as total_trip from {0} group by month_year order by month_year""".format(
                table_name)
            df_total_trip = self.spark.sql(_sql)
            df_total_trip.show()
            if df_total_trip:
                self.store_data_in_database(df_total_trip, table_name="total_users_per_month")
        except Exception as e:
            print(e)

    def get_passholder_calc_monthly(self, table_name):
        try:
            """2.-----To find the how many users use which type of pass and filtering with month -------"""
            _sql = """select to_date(date_format(start_station_time, '01 MMM y'),'dd MMM y')as month_year,
            passholder_type as pass_type,
            count(trip_id)as trip_count from {0} group by pass_type, month_year order by month_year""".format(
                table_name)
            df_passholder_calc_monthly = self.spark.sql(_sql)
            df_passholder_calc_monthly.show()
            if df_passholder_calc_monthly:
                self.store_data_in_database(df_passholder_calc_monthly, table_name="passes_for_trip_months")
        except Exception as e:
            print(e)

    def get_most_monthly_pass(self, table_name):
        try:

            """3.------------------Which station has most monthly/flex pass?------------------"""
            _sql = """select to_date(date_format(start_station_time, '01 MMM y'),'dd MMM y')as month_year,
            start_station,start_station_name,
            count(trip_id)as monthly_pass_total from {0} where passholder_type='Monthly Pass' group by month_year,
            start_station, start_station_name order by monthly_pass_total DESC """.format(
                table_name)
            df_most_monthly_pass = self.spark.sql(_sql)
            df_most_monthly_pass.show()
            if df_most_monthly_pass:
                self.store_data_in_database(df_most_monthly_pass, table_name="station_has_most_monthly_pass")
        except Exception as e:
            print(e)

    def get_long_duration_most(self, table_name):
        try:
            """4.------------ Which station(station id) has booked long duration ride most-----------"""
            _sql = """select start_station, start_station_name, count(trip_id)as No_of_LongDuration_ride from {0} 
            where duration =(select max(duration) as duration from {0})group by start_station, start_station_name 
            order by No_of_LongDuration_ride desc limit 5 """.format(
                table_name)
            df_long_duration_most = self.spark.sql(_sql)
            df_long_duration_most.show()
            if df_long_duration_most:
                self.store_data_in_database(df_long_duration_most, table_name="station_has_booked_long_duration_ride")
        except Exception as e:
            print(e)

    def get_bike_type_booked_mostly(self, table_name):
        try:
            """5.---------To calculate which bike type mostly used in which month-------------"""
            _sql = """Select to_date(date_format(start_station_time, '01 MMM y'),'dd MMM y')as month_year,bike_type,
            count(distinct(bike_id)) as total_bike from {0} group by bike_type,month_year 
            order by month_year,total_bike desc """.format(
                table_name)
            df_bike_type_booked_mostly = self.spark.sql(_sql)
            df_bike_type_booked_mostly.show()
            if df_bike_type_booked_mostly:
                self.store_data_in_database(df_bike_type_booked_mostly, table_name="bikes_mostly_used_in_months")
        except Exception as e:
            print(e)

    def get_trip_mostly_used(self, table_name):
        try:

            """6.---------- to know how many bike use which trip_route_category on the basis of month-----------"""
            _sql = """Select trip_route_category,count(distinct (bike_id)) as total_bike,
            to_date(date_format(start_station_time, '01 MMM y'),'dd MMM y')as month_year from {0} 
            where trip_route_category is not null group by month_year,trip_route_category 
            order by total_bike desc """.format(table_name)
            df_trip_mostly_used = self.spark.sql(_sql)
            df_trip_mostly_used.show()
            if df_trip_mostly_used:
                self.store_data_in_database(df_trip_mostly_used, table_name="bike_trip_route_category_in_months")
        except Exception as e:
            print(e)

    def get_biketype_used(self, table_name):
        try:

            """7.----------------------which bike type uses which trip category----------------------"""
            _sql = """Select bike_type,trip_route_category,count(trip_id) as total_trip from {0} 
            where trip_route_category is not null group by 
            trip_route_category,bike_type order by bike_type,total_trip desc """.format(
                table_name)
            df_biketype_used = self.spark.sql(_sql)
            df_biketype_used.show()
            if df_biketype_used:
                self.store_data_in_database(df_biketype_used, table_name="bike_trip_category_mapping")
        except Exception as e:
            print(e)

    def get_bike_travel(self, table_name):
        try:

            """8.----which bike_type covered max duration-------------------------"""
            _sql = """select bike_type,sum(duration) as total_duration from {0} group by bike_type order by 
            total_duration desc """.format(
                table_name)
            df_bike_travel = self.spark.sql(_sql)
            df_bike_travel.show()
            if df_bike_travel:
                self.store_data_in_database(df_bike_travel, table_name="bike_trip_covered_max_duration")
        except Exception as e:
            print(e)

    # def get_shortest_duration(self, table_name):
    #     try:
    #
    #       """9.----Which start to destination station quickest way to reach(min duration)-------------------------"""
    #         _sql = """select start_station,end_station,duration from {0}
    #          order by start_station,end_station,duration """.format(table_name)
    #         df_shortest_duration = self.spark.sql(_sql)
    #         df_shortest_duration.show()
    #         if df_shortest_duration:
    #             self.store_data_in_database(df_shortest_duration, table_name="shortest_duration_between_two_station")
    #     except Exception as e:
    #         print(e)

    def get_detail_max_duration(self, table_name):
        try:

            """9.----complete details of a ride which have complete max duration ------------------------"""
            _sql = """select start_station,start_station_name,start_station_time,end_station,end_station_name,
            end_station_time,duration,passholder_type,bike_type from {0} 
            where duration ==(select max(duration) as duration from {0})
            group by passholder_type,bike_type, start_station_name,duration,
             start_station_time,end_station_time,start_station,
            end_station,end_station_name order by start_station limit 5 """.format(table_name)
            df_detail_max_duration = self.spark.sql(_sql)
            df_detail_max_duration.show()
            if df_detail_max_duration:
                self.store_data_in_database(df_detail_max_duration, table_name="detail_max_duration")
        except Exception as e:
            print(e)

    def get_detail_min_duration(self, table_name):
        try:

            """10.----complete details of a ride which have complete min duration ------------------------"""
            _sql = """select start_station,start_station_name,start_station_time,end_station,end_station_name,
            end_station_time,duration,passholder_type,bike_type from {0} 
            where duration ==(select min(duration) as duration from {0})
            group by passholder_type,bike_type, start_station_name,duration,
             start_station_time,end_station_time,start_station,
            end_station,end_station_name order by start_station limit 5  """.format(table_name)
            df_detail_min_duration = self.spark.sql(_sql)
            df_detail_min_duration.show()
            if df_detail_min_duration:
                self.store_data_in_database(df_detail_min_duration, table_name="detail_min_duration")
        except Exception as e:
            print(e)

    def get_station_bike_type(self, table_name):
        try:

            """11.----which station has uses which bike_type ------------------------"""
            _sql = """select start_station_name,start_station,bike_type ,count(bike_id)as total_bike from {0} group 
            by start_station_name ,start_station,bike_type order by total_bike desc limit 10 """.format(
                table_name)
            df_station_bike_type = self.spark.sql(_sql)
            df_station_bike_type.show()
            if df_station_bike_type:
                self.store_data_in_database(df_station_bike_type, table_name="station_bike_type")
        except Exception as e:
            print(e)

    def get_bike_type_used_between_station(self, table_name):
        try:
            """12.----Bike type between stations ------------------------"""
            _sql = """select start_station_name,end_station_name,bike_type from {0} group by start_station_name,
            end_station_name,bike_type""".format(
                table_name)
            df_bike_type_used_between_station = self.spark.sql(_sql)
            df_bike_type_used_between_station.show()
            if df_bike_type_used_between_station:
                self.store_data_in_database(df_bike_type_used_between_station,
                                            table_name="bike_type_used_between_station")
        except Exception as e:
            print(e)

    def get_avg_duration_between_station(self, table_name, metro_bike_df):
        df_avg_duration_between_station = None
        try:
            table_avg = "table_dff"
            """13.-----get average duration between station"""
            _sql = """select start_station_name,end_station_name,avg(duration) as avg from {0} 
            group by start_station_name,end_station_name order by start_station_name,end_station_name""".format(
                table_name)
            df_avg = self.spark.sql(_sql)
            dff = df_avg.join(broadcast(metro_bike_df), ['start_station_name', 'end_station_name'])
            dff.createOrReplaceTempView(table_avg)
            _sql1 = """select start_station_name,end_station_name,avg,duration,bike_type from {0} 
            order by 1,2""".format(table_avg)
            df_avg_duration_between_station = self.spark.sql(_sql1)
            df_avg_duration_between_station.show()
            if df_avg_duration_between_station:
                self.store_data_in_database(df_avg_duration_between_station, table_name="avg_duration_between_station")
        except Exception as e:
            print(e)
        return df_avg_duration_between_station

    def get_avg_delay_percentage_bike_type(self, table_name, metro_bike_df, df_avj):
        try:
            table_avg = "table_avg"
            table_delay = "table_delay"
            """14.----avg delay of bike_tpe ------------------------"""
            df_avj.createOrReplaceTempView(table_avg)
            _sql = """select bike_type,count(*) as avg from {0}  where (duration<avg) group by 
            bike_type order by avg""".format(table_avg)

            df_bike_delay_detail = self.spark.sql(_sql)
            _sql1 = """select bike_type, count(*) as total from {0} group by bike_type""".format(table_name)
            df_bike_total_detail = self.spark.sql(_sql1)

            df_delay = df_bike_delay_detail.join(broadcast(df_bike_total_detail), ['bike_type'])
            df_delay.createOrReplaceTempView("table_delay")
            sql_per = """select bike_type,(avg*100/total)as delay_percentage from {0} """.format(table_delay)
            df_avg_bikedelay_percentage = self.spark.sql(sql_per)
            df_avg_bikedelay_percentage.show()
            if df_avg_bikedelay_percentage:
                self.store_data_in_database(df_avg_bikedelay_percentage, table_name="avg_bikedelay_percentage")
        except Exception as e:
            print(e)

    def get_avg_best_percentage_bike_type(self, table_name, metro_bike_df, df_avj):
        try:
            table_avg = "table_avg"
            table_delay = "table_delay"
            """15.----avg perfomance of bike_tpe ------------------------"""
            df_avj.createOrReplaceTempView(table_avg)
            _sql = """select bike_type,count(*) as avg from {0}  where (duration>=avg) group by 
            bike_type order by avg""".format(table_avg)
            df_bike_delay_detail = self.spark.sql(_sql)
            _sql1 = """select bike_type, count(*) as total from {0} group by bike_type""".format(table_name)
            df_bike_total_detail = self.spark.sql(_sql1)

            df_delay = df_bike_delay_detail.join(broadcast(df_bike_total_detail), ['bike_type'])
            df_delay.createOrReplaceTempView("table_delay")
            sql_per = """select bike_type,(avg*100/total)as good_percentage from {0} """.format(table_delay)
            df_avg_bikebest_percentage = self.spark.sql(sql_per)
            df_avg_bikebest_percentage.show()
            if df_avg_bikebest_percentage:
                self.store_data_in_database(df_avg_bikebest_percentage, table_name="avg_bikebest_percentage")
        except Exception as e:
            print(e)

    def analysis_all(self):
        try:
            metro_bike_df, table_name = self.read_csv_file_create_schema()
            if metro_bike_df and table_name:
                # print("1.Total trips in a months...........")
                # self.get_store_total_users(table_name)
                # print("2.Passholder calculation on the basis of month ...........")
                # self.get_passholder_calc_monthly(table_name)
                # print(" 3.station has most monthly/flex pass? ...........")
                # self.get_most_monthly_pass(table_name)
                # print(" 4.station(station id) has booked long duration ride most............")
                # self.get_long_duration_most(table_name)
                # print(" 5.bike_type booked mostly ............")
                # self.get_bike_type_booked_mostly(table_name)
                # print(" 6.trip is mostly used ............")
                # self.get_trip_mostly_used(table_name)
                #print("7.person used which bike type filtering with trip route category............")
                #self.get_biketype_used(table_name)
                # print("8.travels from which bikeid (for bike services)...........")
                # self.get_bike_travel(table_name)
                # print("9.complete details of a ride which have complete max duration ...........")
                # self.get_detail_max_duration(table_name)
                # print("10.complete details of a ride which have complete min duration ...........")
                # self.get_detail_min_duration(table_name)
                #print("11.complete details of which station has uses which bike_type  ...........")
                #self.get_station_bike_type(table_name)
                #print("12.Bike type  used between stations ...........")
                #self.get_bike_type_used_between_station(table_name)
                print("13.AVERAGE DURATION BETWEEN STATIONS...........")
                df_avj = self.get_avg_duration_between_station(table_name, metro_bike_df)
                print("14.AVERAGE delay percentage of biketype...........")
                self.get_avg_delay_percentage_bike_type(table_name, metro_bike_df, df_avj)
                print("15.AVERAGE best percentage of biketype...........")
                self.get_avg_best_percentage_bike_type(table_name, metro_bike_df, df_avj)

        except Exception as e:
            print(e)


if __name__ == "__main__":
    cls_obj = SparkAnalysis()
    cls_obj.analysis_all()
