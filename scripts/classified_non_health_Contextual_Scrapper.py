import sys, urllib, requests, ssl, pickle, json, re, botocore, boto3, decimal, os, configparser
from bs4 import BeautifulSoup
from bs4.element import Comment
from urllib.request import urlopen
from urllib.parse import urlparse
from nltk.tokenize import MWETokenizer
from collections import Counter
from nltk.corpus import stopwords
from botocore.exceptions import ClientError
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem

cwd = os.getcwd()
software_names = [SoftwareName.CHROME.value]
operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=100)
config = configparser.RawConfigParser()
config.read(f"{cwd}/config.cfg")
AWS_config = dict(config.items('AWS'))
aws_access_id = AWS_config['AWS_ACCESS_ID']
aws_access_key = AWS_config['AWS_ACCESS_KEY']
dynamodb = boto3.resource('dynamodb', aws_access_key_id=aws_access_id, aws_secret_access_key=aws_access_key, region_name='us-east-1')
dynamo_table = dynamodb.Table('ct_service')


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def tag_visible(element):
    if element.parent.rds_username in ['style', 'script', 'head', 'title', 'meta', '[document]', 'noscript']:
        return False
    if isinstance(element, Comment):
        return False
    if re.match(r"[\n]+", str(element)):
        return False
    return True


def scrape_handler(url):
    # url = sys.argv[1]
    head = user_agent_rotator.get_random_user_agent()
    headers = {'User-Agent': head}
    print(url)
    lst = requests.get("www.our_api.com?url=" + url).json()["channels"]
    # print(lst)
    ll_ll = list(map(lambda d: d["name"].split("_"), lst))
    f_list = ["health", "family", "dtc", "hcp"]
    if set(f_list) & (set(list(zip(*ll_ll))[1])):
        clean = re.compile('<.*?>')
        stop_words = set(stopwords.words("english"))
        lis = list(pickle.load(open('/src/k.txt', 'rb')))
        # print(len(lis))
        ll = []
        # su =  stopwords.words('english')
        maps = {'!': ' ', '@': ' ', '#': ' ', '%': ' ', '$': ' ', '&': ' ', ')': ' ', '(': ' ', '+': ' ', '*': ' ', '?': ' ', ',': ' ', '/': ' ', '\\': ' ',
                ']': ' ', '[': ' ', '^': ' ', '}': ' ', '{': ' ', '_': ' ', '~': ' ', ';': ' ', '.': ' ', '=': ' ', ':': ' ', '|': ' '}
        table = str.maketrans(maps)
        tk = [tuple(str(name).split()) for name in lis]
        tokenizer = MWETokenizer(tk, separator=" ")

        try:
            urldata = BeautifulSoup(urlopen(urllib.request.Request(url, headers=headers), timeout=3).read(), features="html.parser")
        except requests.exceptions.MissingSchema:
            url = "http://" + url
            urldata = BeautifulSoup(urlopen(urllib.request.Request(url, headers=headers), timeout=3).read(), features="html.parser")
        except IOError:
            log_url_ex = "\n----IO Error-->>" + str(url) + "\n"
            print(log_url_ex)
            return (log_url_ex)
            # exit(1)
        except ssl.CertificateError:
            log_url_ex = "\n----SSL Error-->>" + str(url) + "\n"
            print(log_url_ex)
            return (log_url_ex)
            # exit(1)
        except ValueError:
            if not urlparse(url).scheme:
                url = 'http://' + url
            urldata = BeautifulSoup(urlopen(urllib.request.Request(url, headers=headers), timeout=3).read(), features="html.parser")
        except Exception as e:
            log_url_ex = '\nError URL-->> ' + str(e) + str(url)
            print(log_url_ex)
            return (log_url_ex)
            # exit(1)

        try:
            title_tag = re.sub(clean, '', (urldata.find('title')).get_text(" ")).lower()
            tt = (filter(lambda word: word not in stop_words, (title_tag.translate(table)).split()))
            ll.extend(tokenizer.tokenize(list(tt)))
        except AttributeError:
            pass
        try:
            texts = urldata.findAll(text=True)
            visible_texts = filter(tag_visible, texts)
            body_tag = u" ".join(t.strip() for t in visible_texts).lower()
            bt = (filter(lambda word: word not in stop_words, (body_tag.translate(table)).split()))
            ll.extend(tokenizer.tokenize(list(bt)))
            """
            body_tag = re.sub(clean, '', (urldata.find('body')).get_text(" ")).lower()
            bt = (filter(lambda word: word not in stop_words, (body_tag.translate(table)).split()))
            ll.extend(tokenizer.tokenize(list(bt)))
            """
        except AttributeError:
            pass
        try:
            keyword_tag = str(urldata.find("meta", {"name": "keywords"}).get('value')).lower()
            kt = (filter(lambda word: word not in stop_words, (keyword_tag.translate(table)).split()))
            ll.extend(tokenizer.tokenize(list(kt)))
        except AttributeError:
            pass
        try:
            description_tag = str(urldata.find("meta", {"name": "description"}).get('content')).lower()
            dt = (filter(lambda word: word not in stop_words, (description_tag.translate(table)).split()))
            ll.extend(tokenizer.tokenize(list(dt)))
        except AttributeError:
            pass

        lstt = list(set(ll).difference(lis))
        names = list(filter(lambda a: a not in lstt, ll))
        # names = list(filter(lambda a: a not in (list(set(ll).difference(lis))), ll))

        # Takes all Keywords
        keywords = dict(Counter(sorted(names, key=lambda name: name.lower())))
        # Taking keywords which has >=5 counts
        # keywords = dict(takewhile(lambda i: i[1] >= 5, Counter(sorted(names, key=lambda name:name.lower())).most_common()))
        #################################
        """"""
        try:
            response = dynamo_table.update_item(
                Key={"page_url": url},
                UpdateExpression="SET keywords= :Keywords",
                ExpressionAttributeValues={':Keywords': keywords},
                ReturnValues="UPDATED_NEW"
            )
            json.dumps(response, indent=4, cls=DecimalEncoder)
        except botocore.exceptions.ClientError:
            pass
        """"""
        log_url = '\nScrapped URL-->> ' + str(url)
        print(log_url)
        print(keywords)
        return log_url


if __name__ == '__main__':
    scrape_handler(url=str(sys.argv[1]))
