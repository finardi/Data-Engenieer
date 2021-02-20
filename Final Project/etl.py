# ------- #
# imports 
# ------- #
import psycopg2
import pandas as pd
from dataclasses import dataclass

from pyspark.sql import SparkSession
spark = SparkSession.builder.\
config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
.enableHiveSupport().getOrCreate()

# ------- #
# queries
# ------- #
drop_airports   = "DROP TABLE IF EXISTS airports;"
create_airports = """
        CREATE TABLE IF NOT EXISTS public.airports (
            iata_code    VARCHAR PRIMARY KEY,
            name         VARCHAR,
            type         VARCHAR,
            local_code   VARCHAR,
            coordinates  VARCHAR,
            city         VARCHAR,
            elevation_ft FLOAT,
            continent    VARCHAR,
            iso_country  VARCHAR,
            iso_region   VARCHAR,
            municipality VARCHAR,
            gps_code     VARCHAR
        );
        """

airport_insert = """
        INSERT INTO airports (iata_code, name, type, local_code, coordinates, city, elevation_ft, continent, \
                              iso_country, iso_region, municipality, gps_code) \
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

drop_demographics   = "DROP TABLE IF EXISTS demographics;"
create_demographics = """
        CREATE TABLE IF NOT EXISTS public.demographics (
            city                   VARCHAR,
            state                  VARCHAR,
            media_age              FLOAT,
            male_population        INT,
            female_population      INT,
            total_population       INT,
            num_veterans           INT,
            foreign_born           INT,
            average_household_size FLOAT,
            state_code             VARCHAR(2),
            race                   VARCHAR,
            count                  INT
        );
        """

demographic_insert = """
        INSERT INTO demographics (city, state, media_age, male_population, female_population, total_population, \
                                  num_veterans, foreign_born, average_household_size, state_code, race, count) \
                                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                     """

drop_temperature   = "DROP TABLE IF EXISTS weather;"
create_temperature = """
        CREATE TABLE IF NOT EXISTS temperature (
            timestamp                      DATE,
            average_temperature            FLOAT,
                average_temperature_uncertainty FLOAT,
            city                           VARCHAR,
            country                        VARCHAR,
            latitude                       VARCHAR,
            longitude                      VARCHAR
        );
        """
temperature_insert = """
        INSERT INTO temperature (timestamp, average_temperature, average_temperature_uncertainty, city, country, \
                                 latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s, %s)
                     """

drop_immigrations   = "DROP TABLE IF EXISTS immigrations;"
create_immigrations = """
        CREATE TABLE IF NOT EXISTS public.immigrations (
            cicid    FLOAT PRIMARY KEY,
            year     INTEGER,
            month    INTEGER,
            cit      FLOAT,
            res      FLOAT,
            iata     VARCHAR(3),
            arrdate  INTEGER,
            mode     INTEGER,
            addr     VARCHAR,
            depdate  INTEGER,
            bir      INTEGER,
            visa     INTEGER,
            count    INTEGER,
            dtadfile VARCHAR,
            entdepa  VARCHAR(1),
            entdepd  VARCHAR(1),
            matflag  VARCHAR(1),
            biryear  INTEGER,
            dtaddto  VARCHAR,
            gender   VARCHAR(1),
            airline  VARCHAR,
            admnum   FLOAT,
            fltno    VARCHAR,
            visatype VARCHAR
        );
        """

immigration_insert = """
        INSERT INTO immigrations (cicid, year, month, cit, res, iata, arrdate, mode, addr, depdate, bir, visa, count, dtadfile, \
                                  entdepa, entdepd, matflag, biryear, dtaddto, gender, airline, admnum, fltno, visatype) VALUES \
                                 (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                     """

drop_table_queries = [drop_airports, drop_demographics, drop_immigrations, drop_temperature]
create_table_queries = [create_airports, create_demographics, create_immigrations, create_temperature]

# ------------- #
# data cleaning  
# ------------- #
@dataclass
class PrepFactTable:
    """
    A class used to prepare immigration fact table
    ...

    Attributes
    ----------
    path : str
        a path to port locations 
    df_i94 : object
        spark raw immigration dataframe

    Methods
    -------
    _get_port_locations
        read port locations labels and construct a dataframe with iata_code, city and state
    cleaning_i94
        filter the ports with issue and filter df_i94 without these ports. Remove columns
        and drop missing values
    """
    path: str
    df_i94: object

    def _get_port_locations(self):
        with open(self.path) as handle:
            lines = handle.readlines()
        lines = [x.strip() for x in lines]
        ports = lines[302:962]

        splt_ports, locations = [], []
        for x in ports:
            splt_ports.append(x.split("="))
        for port in splt_ports:
            locations.append(port[1].replace("'","").strip())
        
        data_dict = {
            'iata_code': [port[0].replace("'","").strip() for port in splt_ports], 
            'city': [port.split(",")[0] for port in locations], 
            'state': [port.split(",")[-1] for port in locations]
            }
        
        return spark.createDataFrame(pd.DataFrame(data_dict))

    def cleaning_i94(self):
        df = self._get_port_locations()

        ports_with_issues = df[df["city"] == df["state"]]
        list_ports_with_issues = list(set())
        df_processed = self.df_i94[~self.df_i94["i94port"].isin(list_ports_with_issues)]

        columns_to_drop = ["insnum", "entdepu", "occup", "visapost"]
        df_processed = df_processed.drop(*columns_to_drop)
        df_processed = df_processed.dropna(how='any')

        return df_processed, ports_with_issues

@dataclass
class PrepDimensionTables:
    """
    A class used to prepare dimension tables
    ...

    Attributes
    ----------
    _df_airport : object
        spark raw airtport dataframe
    _df_demographics : object
        spark raw demographics dataframe
    _df_temperature : object
        spark raw temperature dataframe

    Methods
    -------
    dropnan
        drop missing values in the three dataframes
    """
    _df_airport: object
    _df_demographics: object
    _df_temperature: object

    def dropnan(self):
        _df_airport = self._df_airport.dropna(how='any', subset=['iata_code'])
        
        _df_demographics = self._df_demographics.dropna(how='any')
        
        _df_temperature  = self._df_temperature.dropna(how='any')
        _df_temperature  = self._df_temperature[
            self._df_temperature["Country"] == "United States"]

        return _df_airport, _df_demographics, _df_temperature
    
@dataclass
class PrepAirportCodes:
    """
    A class used to prepare airport codes
    ...

    Attributes
    ----------
    df_airport_processed : object
        spark df_airport_processed dataframe
    airport_ports : object
        spark airport_ports dataframe

    Methods
    -------
    prep_airport_codes
        perform a join on iata_code feature and filter columns
    """
    df_airport_processed: object
    airport_ports: object

    def prep_airport_codes(self):
        df_airport_codes = self.df_airport_processed.join(self.airport_ports, on=['iata_code'], how='inner')
        df_airport_codes = df_airport_codes.select(
            ["iata_code", "name", "type", "local_code", 
             "coordinates", "city", "elevation_ft", "continent", 
             "iso_country", "iso_region", "municipality", "gps_code"]
        )
        
        return df_airport_codes

# ------------ #
# create table 
# ------------ #
@dataclass
class CreateTables:
    """
    A class used to creata SQL tables
    ...

    Methods
    -------
    _create_database
        creates and connects postgres in the database
    _drop_tables
        drop table if exist using 
    create_tables
        creates fact and dimensions tables
    """
    def _create_database(self):

        # connect to default database
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
        conn.set_session(autocommit=True)
        cur = conn.cursor()

        # create sparkify database with UTF8 encoding
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

        # close connection to default database
        conn.close()    

        # connect to sparkify database
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()

        return cur, conn

    def _drop_tables(self, cur, conn):
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()

    def create_tables(self):
        cur, conn = self._create_database()
        self._drop_tables(cur, conn)
        
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
        
        conn.close()

# --------------------------------------- #
# insert data and perform quality checks 
# --------------------------------------- #
@dataclass
class InsertWithQualityTest:
    """
    A class used to prepare dimension tables
    ...

    Attributes
    ----------
    cus : object
        postgres connection cursor

    Methods
    -------
    _iter_spark_row
        create a data tuple of tuple 
        to be insert with psycopg2.extras.execute_batch format
    insert_table
        perform the insert table
    quality_tests
        perform quality check to check the insertion data
    
    """

    cur: object

    def _iter_spark_row(self, df):
        row_values = []
        for idx, row in enumerate(df.collect()):
            row_list = [item for item in row]
            row_values.append(row_list)

        data_tup = tuple(tuple(elem) for elem in row_values)

        return data_tup

    def insert_table(self, df, table, chunk_insert=2_000, verbose=True): 
        len_df = df.shape()[0]
        data_tup = self._iter_spark_row(df)
        percentils = [int(len_df*.25), int(len_df*.50), int(len_df*.75)]

        already25, already50, already75,= False, False, False
        for idx in range(0, len_df, chunk_insert):
            psycopg2.extras.execute_batch(self.cur, table, data_tup[idx:idx + chunk_insert])

            if verbose:
                if idx >= percentils[0] and idx <= percentils[1] and already25 is False:
                    print(f'Inserting 25%...')
                    already25 = True
                if idx >= percentils[1] and idx <= percentils[2] and already50 is False:
                    print(f'Inserting 50%...')
                    already50 = True
                if idx >= percentils[2] and already75 is False:
                    print(f'Inserting 75%...')
                    already75 = True                        
    
    def quality_tests(self, df_name):

        if df_name=='airports':
            string = "SELECT COUNT (*) FROM " + df_name
            self.cur.execute(string)
            if self.cur.rowcount < 1:
                print("No data found in airports dimension table")
            print('Quality airport insert table test OK')

        elif df_name=='demographics':
            string = "SELECT COUNT (*) FROM " + df_name
            self.cur.execute(string)
            if self.cur.rowcount < 1:
                print("No data found in demographics dimension table")
            print('Quality demographics insert table test OK')

        elif df_name=='temperature':
            string = "SELECT COUNT (*) FROM " + df_name
            self.cur.execute(string)
            if self.cur.rowcount < 1:
                print("No data found in temperature dimension table")
            print('Quality temperature insert table test OK')

        elif df_name=='immigrations':
            string = "SELECT COUNT (*) FROM " + df_name
            self.cur.execute(string)
            if self.cur.rowcount < 1:
                print("No data found in immigrations fact table")
            print('Quality immigrations insert table test OK')

        else:
            print(f'There is no table with name {df_name} the options are:')
            print('\tairports, demographics, temperature and immigrations')    