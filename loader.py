import requests, json, urllib
import pandas as pd
from countryinfo import CountryInfo
from Countrydetails import country as country_details_country
import logging
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
#Logging file
logging.basicConfig(filename='Location_Sizes.log', encoding='utf-8', level=logging.DEBUG)

#MongoDB Database Credentials
m_usr = "experiencia_app"
m_pwd = "G6+-@{R6H?Nn<P<Bx`z?Hpe4IJ`f_VsTF7NGH6JdA+UzU82G%,!^_{=&~-@;Ed3H|RF[&ffJ\P}v]hr_e|^%SD?j:F-Cx:Jt)~qs&x"

#client = pymongo.MongoClient("mongodb+srv://"+m_usr+":"+urllib.parse.quote(m_pwd)+"@cluster0.z9ajl.mongodb.net/Cluster0?retryWrites=true&w=majority")
#covid_db = client.COVID_LOCATIONS


def checkConnToMongoDB():
    print(covid_db.COVID_DATA.find_one())

def searchGoogleForArea(name,skipGet = False):
    if(not skipGet):
        query = "http://google.com/search?q="+name.replace(" ","%20").replace(",","%2c")+"%20area"
        req = Request(query, headers={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36"})
        webpage = urlopen(req).read()
        f = open("temp.html","w+")
        f.write(webpage.decode('utf-8'))
        f.close()
    f = open("temp.html","r")
    html_doc = f.read()
    soup = BeautifulSoup(html_doc, 'html.parser')
    all_info_boxes = soup.find_all("div",class_="rVusze")
    for info_box in all_info_boxes:
        info_box_bs4 = BeautifulSoup(str(info_box), 'html.parser')
        fl = info_box_bs4.find("a",class_="fl")
        if("Area" == fl.text):
            area_span = info_box_bs4.find("span","z8gr9e").text
            area_span = area_span.split("\xa0")[0]
            area_span = area_span.replace(",","")
            return area_span
    return 0
    
def loadDb():
    new_data = []
    df = pd.read_csv("raw_covid_data.csv", index_col="Combined_Key")
    df.drop(columns=["FIPS","Admin2","Active","Recovered","Province_State","Last_Update"], inplace=True)
    df = df[df['Long_'].notna()]
    df = df[df['Lat'].notna()]
    for row in df.iterrows():
        row_info = row[1].to_frame().to_dict()
        key = [* row_info][0]
        row_info = row_info[key]
        datapoint = {
            "Combined_Key":key,
            "loc":{"lat":row_info["Lat"],"lon":row_info["Long_"]},
            "country": row_info["Country_Region"]
        }
        new_data.append(datapoint)
    f = open("covid_collection.json","w+")
    f.write(json.dumps(new_data))

def factor_location_sizes():
    covid_collection = pd.read_csv("raw_covid_data.csv")
    location_size_column = []
    covid_collection = prepare_raw_covid_data(covid_collection)
    for row in covid_collection.iterrows():
        row = row[1].to_frame().to_dict()
        key = [* row][0]
        row = row[key]
        combined_key = row["Combined_Key"]
        country = row["Country_Region"]
        logging_extra = ""
        if(len(covid_collection[covid_collection["Country_Region"] == country]) != 1):
            distance = searchGoogleForArea(combined_key)
        else:
            logging_extra = "Individual Country"
            try:
                country_obj = CountryInfo(country)
                distance = convert_to_sq_miles(country_obj.area())# CountryInfo returns area in km
            except:
                country_obj = country_details_country.country_details('India')
                distance = convert_to_sq_miles(country_obj.area())# CountryInfo returns area in km
        logging.info(combined_key+" | "+logging_extra+" | "+str(distance))
        location_size_column.append(distance)
    logging.info(str(location_size_column))
    new_df = covid_collection.assign(location_size = location_size_column)
    new_df.to_csv("advanced_covid_data.csv",index = False)

def prepare_raw_covid_data(df):
    df = df[df['Long_'].notna()]
    df = df[df['Lat'].notna()]
    return df

def convert_to_sq_miles(sq_km):
    try:
        conv_fac = 0.386102
        return sq_km * conv_fac 
    except:
        return 69696969

if __name__ == "__main__":
    #print(searchGoogleForArea("Northern Territory, Australia",True))
    factor_location_sizes()