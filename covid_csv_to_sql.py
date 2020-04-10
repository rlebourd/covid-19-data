import pymysql

class CovidDBManager:
  COUNTY_TABLE_NAME  = "county"
  RECORD_TABLE_NAME  = "covid_record"

  COUNTY_PRIMARY_KEY = "county_id"
  RECORD_PRIMARY_KEY = "covid_record_id"
  
  @classmethod
  def Recreate(cls, database, file):
    counties = dict()
    with database.cursor() as cursor:
      cursor.execute(f"DROP TABLE IF EXISTS {cls.RECORD_TABLE_NAME}")
      cursor.execute(f"DROP TABLE IF EXISTS {cls.COUNTY_TABLE_NAME}")
      cursor.execute(f"""
      CREATE TABLE {cls.COUNTY_TABLE_NAME} (
        {cls.COUNTY_PRIMARY_KEY} INTEGER NOT NULL AUTO_INCREMENT,
        county_fips INTEGER,
        county_name TEXT,
        state TEXT,
        file_ptr INTEGER NOT NULL,
        
        PRIMARY KEY({cls.COUNTY_PRIMARY_KEY})
      )""")
      cursor.execute(f"""
      CREATE TABLE {cls.RECORD_TABLE_NAME} (
        {cls.RECORD_PRIMARY_KEY} INTEGER NOT NULL AUTO_INCREMENT,
        recorded_on DATE NOT NULL,
        total_num_cases INTEGER NOT NULL,
        num_deaths INTEGER NOT NULL,
        {cls.COUNTY_PRIMARY_KEY} INTEGER NOT NULL,
        
        PRIMARY KEY({cls.RECORD_PRIMARY_KEY}),
        FOREIGN KEY({cls.COUNTY_PRIMARY_KEY}) REFERENCES {cls.COUNTY_TABLE_NAME}({cls.COUNTY_PRIMARY_KEY})
      )""")
    database.commit()
    with database.cursor() as cursor:
      line = file.readline() # discard header line in csv file
      line = file.readline()
      while line:
        file_ptr = file.tell()

        fields   = line.strip().split(",")
        date     = fields[0].strip().upper()
        name     = fields[1].strip().upper()
        state    = fields[2].strip().upper()
        fips     = fields[3].strip().upper()
        cases    = fields[4].strip().upper()
        deaths   = fields[5].strip().upper()
        
        if fips == '':
          fips = "NULL"
        
        if (name,state) not in counties.keys():
          counties[(name,state)] = [fips, file_ptr]
        
        if fips != "NULL" and counties[(name,state)] == "NULL":
          counties[(name,state)][0] = fips

        counties[(name,state)][1] = file_ptr
        counties[(name,state)].append([date, cases, deaths])
        
        line = file.readline()

      for (name, state) in counties:
        county_data = counties[(name,state)]
        fips = county_data[0]
        file_ptr = county_data[1]
        covid_records = county_data[2:-1]

        sql = f"""
        INSERT INTO county (county_fips, county_name, state, file_ptr)
                    VALUES ({fips}, "{name}", "{state}", "{file_ptr}")"""
        cursor.execute(sql)
            
      for (name, state) in counties:
        county_data = counties[(name,state)]
        fips = county_data[0]
        file_ptr = county_data[1]
        covid_records = county_data[2:-1]
        
        # get county_id
        cursor.execute(f"""SELECT county_id FROM county WHERE county_fips LIKE {fips} OR county_name LIKE "{name}" AND state LIKE "{state}" """)
        county_id = cursor.fetchone()[0]
        
        for record in covid_records:
          date = record[0]
          cases = record[1]
          deaths = record[2]
          sql = f"""
          INSERT INTO covid_record (county_id, recorded_on, total_num_cases, num_deaths)
                      VALUES ({county_id}, "{date}", {cases}, {deaths})"""
          cursor.execute(sql)
      database.commit()
    
  @staticmethod
  def LoadIncrementalUpdates(database, file):
    counties = dict()
    with database.cursor() as cursor:
      cursor.execute("SELECT MAX(file_ptr) FROM county")
      file_ptr = int(cursor.fetchone()[0])
      
      file.seek(file_ptr) # discard header line in csv file
      line = file.readline()
      line = file.readline()
      while line:
        file_ptr = file.tell()

        fields   = line.strip().split(",")
        date     = fields[0].strip().upper()
        name     = fields[1].strip().upper()
        state    = fields[2].strip().upper()
        fips     = fields[3].strip().upper()
        cases    = fields[4].strip().upper()
        deaths   = fields[5].strip().upper()
        if fips == '':
          fips = "NULL"
        
        if (name,state) not in counties.keys():
          counties[(name,state)] = [fips, file_ptr]
        
        if fips != "NULL" and counties[(name,state)] == "NULL":
          counties[(name,state)][0] = fips

        counties[(name,state)][1] = file_ptr
        counties[(name,state)].append([date, cases, deaths])
        
        line = file.readline()

      for (name, state) in counties:
        fips = counties[(name,state)][0]
        file_ptr = counties[(name,state)][1]
      
        # check if this county is in the counties table
        results = cursor.execute(f"""SELECT county_id, county_fips FROM county WHERE state LIKE "{state}" AND county_name LIKE "{name}" """)
        if results > 0:
          # check if the fips is null or not
          id_fips = cursor.fetchone()
          id = id_fips[0]
          fips = id_fips[1]
          if fips == "NULL":
            cursor.execute(f"""UPDATE county SET county_fips = {fips}, file_ptr = {file_ptr} WHERE county_id = {id} """)
          else:
            cursor.execute(f"""UPDATE county SET file_ptr = {file_ptr} WHERE county_id = {id} """)
        else:
          sql = f"""
                  INSERT INTO county (county_fips, county_name, state, file_ptr)
                  VALUES ({fips}, "{name}", "{state}", "{file_ptr}")"""
          cursor.execute(sql)
            
      for (name, state) in counties:
        county_data = counties[(name,state)]
        fips = county_data[0]
        file_ptr = county_data[1]
        covid_records = county_data[2:-1]
        
        # get county_id
        cursor.execute(f"""SELECT county_id FROM county WHERE county_fips LIKE {fips} OR county_name LIKE "{name}" AND state LIKE "{state}" """)
        county_id = cursor.fetchone()[0]
        
        for record in covid_records:
          print(name, state, fips, record)
          date = record[0]
          cases = record[1]
          deaths = record[2]
          sql = f"""
          INSERT INTO covid_record (county_id, recorded_on, total_num_cases, num_deaths)
                      VALUES ({county_id}, "{date}", {cases}, {deaths})"""
          cursor.execute(sql)
      database.commit()

dbase = pymysql.connect(host='localhost',
                        user='dummy',
                        password='password',
                        db='covid19')

input_file = open('us-counties.csv', 'r')
CovidDBManager.LoadIncrementalUpdates(dbase, input_file)
#CovidDBManager.Recreate(dbase, input_file)
