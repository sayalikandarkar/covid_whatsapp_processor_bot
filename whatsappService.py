import re,pandas as pd,string,requests,spacy,json
from  spacy.lang.en.stop_words import STOP_WORDS

nlp = spacy.load('en_core_web_sm')

punctuations = string.punctuation
stopwords = list(STOP_WORDS)

class WhatsappService:
    def_dict = {'need': '','available':'','sender': 'N','resource_list':'','resource_contact': '','source': 'whatsapp','state': '','location': '','text':''}
    need_list = ['need','require','please','help','urgency','emergency','urgent','want','pls','urgentlyrequired','send',
                  'dm','get','year','old','male','female','saturation','any','request','requirement','seek','looking','look','lead']
    sane_list = ['bed','favipiravir','icu','need','oxygen','plasma','remdesivir','tocilizumab','ventilator']
    available_list = ['available','verify','unverified','notverified']
    towns = ['mumbai']
    states = ['delhi']
    # states =  ['uttar', 'pradesh', 'bengal', 'madhya', 'andhra', 'tamil', 'nadu', 'Himachal', 'Jammu', 'Kashmir', 'Arunachal', 'Dadra', 'Nagar', 'Haveli', 'Andaman', 'Nicobar']
    # states = [s.lower() for s in states]
    final_list = []

    def __init__(self):

        return None
        towns,states = self.getTownList()
        #!pip install -U spacy
        #!python -m spacy download en_core_web_sm

    def getTownList(self):
        jsonUrl = 'https://spreadsheets.google.com/feeds/cells/1LMqd74dNT-4Dc5oy44tavvhHu1xKuyTVlssxxa1eTUg/1/public/full?alt=json'
        response = requests.get(jsonUrl)
        data = response.json()
        data = data['feed']['entry']

        townList = []
        stateList = []
        i = 4
        while i < len(data):
            town = data[i]['content']['$t']
            state = data[i+1]['content']['$t']
            i = i + 4
            townList.append(town.lower())
            stateList.append(state.lower())

        return townList, list(set(stateList))

    def extract_phone_numbers(self, string):
        r = re.compile(r'(\d{6}[-\.\s]??\d{5}|\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})')
        # r = re.compile(r'')
        phone_numbers = r.findall(string)
        return [re.sub(r'\D', '', number) for number in phone_numbers]

    def dataCleaning(self,df):
      default_dict = {'need': '','available':'','sender': 'N','resource_list':'','resource_contact': '','source': 'whatsapp','state': '','location': '','text':''}
      towns = self.getDistricts()
      states = towns.values()
      sentence = df['Text']
      doc = nlp(sentence)
      tokens = []
      valid_items=[]
      contact_list = []

      for token in doc:
          if token.lemma_ != '-PRON-':
              temp = token.lemma_.lower().strip()
          else:
              temp = token.lower_
          tokens.append(temp)
      clean_tokens = []

      for token in tokens:
          if token not in punctuations and token not in stopwords:

            #print(token)
            if token in WhatsappService.available_list:
              default_dict['available'] = 'available'
              clean_tokens.append(token)
            elif token in WhatsappService.need_list:
              clean_tokens.append(token)
              default_dict['need'] = 'need'
              clean_tokens.append(token)
            elif token in towns:
              default_dict['location'] = token
              clean_tokens.append(token)
            elif token in states:
              default_dict['state'] = token
              clean_tokens.append(token)
            elif self.extract_phone_numbers(token):
              contact_list.append(token)
              clean_tokens.append(token)
            elif token in WhatsappService.sane_list:
              valid_items.append(token)
              clean_tokens.append(token) 
            else:
              pass
              
      if len(clean_tokens)>1:

        default_dict['resource_list'] = valid_items
        default_dict['resource_contact'] = contact_list
        default_dict['text']=sentence
        default_dict['sender']=df['Contact_no']
        WhatsappService.final_list.append(default_dict.copy())
        #print(default_dict)

    def read_file(self, file):
        file_output = open(file,'r', encoding = 'utf-8').read()
        return file_output

    def getDistricts(self):
        with open('/home/ec2-user/sayali_2/districts.json', 'r') as fp:
          values = json.load(fp)
          districts = dict()
        for k, v in enumerate(values):
          d, s = values[v]
          districts[d.lower()] = s.lower()

        return districts

    def processFile(self, data):
        data = re.sub(r'\boxy\b', 'oxygen', data)
        data = re.sub(r'\bcylinder\b', 'oxygen', data)

        chunks = re.split('\\d/\\d\\d/\\d\\d',data)
        df = pd.DataFrame(chunks)
        df.columns=['Chat']
        Message= df["Chat"].str.split("-", n = 1, expand = True)
        df['Time']=Message[0]
        df['Text']=Message[1]
        df['Text']=df['Text'].str.lower()
        Message1= df["Text"].str.split(":", n = 1, expand = True)
        df['Text']=Message1[1]
        df['Contact_no']=Message1[0]
        df=df.drop(columns=['Chat'])
        f=df['Text'].isnull() 
        df = df[~f]
        df['Text']=df['Text'].str.lower()
        df['Text'] = df['Text'].str.replace('<media omitted>','MediaShared')
        df['Text'] = df['Text'].str.replace('this message was deleted','DeletedMsg')
        df['Text'] = df['Text'].str.replace('\n',' ')  
        df['Time'] = df['Time'].str.replace(',','')

        return df

    def getResourceList(self, df):
      df.apply(self.dataCleaning, axis=1)

    def getData(self,location):
      data = self.read_file(location)
      processed_df = self.processFile(data)
      #final_df.to_csv('/content/sample_data/Covid_new.csv')
      self.getResourceList(processed_df)

def main():
    ws = WhatsappService()
    final_data = ws.getData('/home/ec2-user/sayali_2/Covid.txt')
    #print(final_data)
    #print(WhatsappService.final_list)
    result = pd.DataFrame(WhatsappService.final_list)
    result.to_csv('/home/ec2-user/sayali_2/Covid_new.csv')
    return WhatsappService.final_list

main()