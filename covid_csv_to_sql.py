import pymysql

dbase = pymysql.connect(host='localhost',
                        user='dummy',
                        password='password',
                        db='covid19')

#add to the counties table
counties = {}
with dbase.cursor() as cursor:
  input_file = open('us-counties.csv', 'r')
  for (line_idx, line) in enumerate(input_file):
    fields = line.split(",")
    date   = fields[0]
    county = fields[1]
    state  = fields[2]
    fips   = fields[3]
    cases  = fields[4]
    deaths = fields[5]

    print(date, county, state, fips, cases, deaths)
    if line_idx > 0:
      if fips == '':
        fips = None
      counties[(county, state)] = fips

  for (county_name, state) in counties.keys():
    county_fips  = counties[(county_name, state)]
    if county_fips is None:
      county_fips = "NULL"
    print(state, county_fips)
    sql = f"INSERT INTO county (county_fips, county_name, state) VALUES ({county_fips}, \"{county_name}\", \"{state}\")"
    print(sql)
    cursor.execute(sql)
  dbase.commit()
  
# add to the cases table
counties = {}
with dbase.cursor() as cursor:
  input_file = open('us-counties.csv', 'r')
  for (line_idx, line) in enumerate(input_file):
    fields = line.split(",")
    date   = fields[0]
    county = fields[1]
    state  = fields[2]
    fips   = fields[3]
    cases  = fields[4]
    deaths = fields[5]

    if line_idx > 0:
      if fips == '':
        fips = None
      counties[(county,state,date)] = (cases, deaths)

  for (county,state,date) in counties:
    cases  = counties[(county,state,date)][0]
    deaths = counties[(county,state,date)][1].strip()

    cursor.execute(f"SELECT county_id FROM county WHERE county_name = \"{county}\" AND state = \"{state}\"")
    county_id = cursor.fetchone()[0]
    sql = f"INSERT INTO covid_record (county_id, recorded_on, total_num_cases, num_deaths) VALUES ({county_id}, \"{date}\", {cases}, {deaths})"
    print(sql)
    cursor.execute(sql)
  dbase.commit()
