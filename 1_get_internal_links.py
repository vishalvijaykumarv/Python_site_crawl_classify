import sys, os, glob, requests, colorama, re, boto3, time,datetime
from random_user_agent.params import SoftwareName, OperatingSystem
from random_user_agent.user_agent import UserAgent
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import pandas as pd
import bs4
import urllib.request

colorama.init()
GREEN = colorama.Fore.GREEN
GRAY = colorama.Fore.LIGHTBLACK_EX
RESET = colorama.Fore.RESET

software_names = [SoftwareName.CHROME.value]
operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
user_agents = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=100)

sys.setrecursionlimit(10 ** 6)  # max depth of recursion
# dates = str(sys.argv[1])  # str(datetime.datetime.now().date())
# main_url = str(sys.argv[2])
dates = str(datetime.datetime.now().date())
main_url = "https://druggenius.com/"
s3 = boto3.resource('s3')
s3_bucket = 'data-lake'
Key = "Classified_Url/" + dates + "/"
domain_name = urlparse(main_url).netloc
url_scheme = urlparse(main_url).scheme
result_file = domain_name.replace('.', '_') + ".csv.gz"
internal_urls = set()
external_urls = set()

try:
    if str(sys.argv[3]):
        skip_pp = set(str(sys.argv[3]).replace("'", "").split(','))
    else:
        skip_pp = set()
except IndexError as ind:
    skip_pp = set()
    pass

skip_urls = {'forums', 'newsletter'}.union(skip_pp)
print("SKIP URLS: " + str(skip_urls))


def cc(url):
    # print("executing:- cc")
    links = [url]
    try:
        while links:
            links = crawl(links)
    except Exception as e:
        print(e)


def crawl(url):  # , max_urls=1):
    # print("executing:- crawl")
    """
    Crawls a web page and extracts all links.
    You'll find all links in `external_urls` and `internal_urls` global set variables.
    params:
        max_urls (int): number of max urls to crawl, default is 30.
    """
    set_links = set()
    try:
        for link in url:
            time.sleep(1)
            links = get_all_website_links(link)
            set_links.update(links)
        return set_links
    except Exception as e:
        print(e)


def cloud_fare_sites(url):
    head = user_agents.get_random_user_agent()
    site = urllib.request.Request(url=f"{url}")
    site.add_header("User-Agent", f"{head}")
    with urllib.request.urlopen(site) as f:
        soup = bs4.BeautifulSoup(f, "html5lib")
        return soup


def get_all_website_links(url):
    # print("executing:- get_all_website_links ")
    """
    Returns all URLs that is found on `url` in which it belongs to the same website
    """
    # all URLs of `url`
    global url_s
    try:
        head = user_agents.get_random_user_agent()
        headers = {'User-Agent': head}
        url_s = set()
        time.sleep(2)
        response = requests.get(url, headers=headers)
        # print(f"Status Code for {url} is {response.status_code}")
        if response.status_code == 403:
            soup = cloud_fare_sites(url=url)
        else:
            soup = BeautifulSoup(requests.get(url, headers=headers).content.decode('utf-8', 'ignore'), "html.parser")

        for a_tag in soup.findAll("a"):
            href = a_tag.attrs.get("href")
            # print(href)

            if href == "" or href is None:
                # href empty tag
                continue
            # join the URL if it's relative (not absolute link)
            href = urljoin(url, href).strip("/")
            parsed_href = urlparse(href)
            # remove URL GET parameters, URL fragments, etc.
            href = (parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path).strip("/")
            # Skip_perticular url's
            if bool(re.search(r"\s", href)) or skip_urls.intersection(set(urlparse(href).path.split('/')[:-1])):
                continue
            if not is_valid(href):
                # not a valid URL
                continue
            if domain_name in parsed_href.netloc or parsed_href.netloc in domain_name:
                href = href.replace(urlparse(urljoin(url, href)).scheme, url_scheme).replace(urlparse(urljoin(url, href)).netloc, domain_name)
            else:
                # if domain_name not in parsed_href:
                # external link
                if href not in external_urls:
                    print(f"{GRAY}[!] External link: {href}{RESET}")
                    external_urls.add(href)
                continue
            if href in internal_urls:
                # already in the set
                continue
            if (requests.head(href).status_code not in [200, 301, 302,403]) or ('html' not in requests.head(href).headers['Content-Type']):
                print("Content-Type is Not *htm*: " + href)
                print(requests.head(href).headers['Content-Type'])
                print(requests.head(href).status_code)
                continue

            print(f"{GREEN}[*] Internal link: {href}{RESET}")
            url_s.add(href)
            internal_urls.add(href)
    except Exception as e:
        print(e)
    return url_s


def is_valid(url):
    # print("executing:- is_valid ")
    """
    Checks whether `url` is a valid URL.
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


def upload_to_s3():
    try:
        os.makedirs('/tmp/' + domain_name)
    except:
        pass
    for i, chunk in enumerate(pd.read_csv(result_file, header=None, compression='gzip', chunksize=2500)):
        chunk.to_csv('/tmp/' + domain_name + '/chunk{}.csv.gz'.format(i), header=['page_url'], compression='gzip', index=False)
    files = glob.glob('/tmp/' + domain_name + '/chunk*')
    print(files)
    for filename in files:
        s3.Bucket('data-lake').upload_file(Filename=filename, Key=Key + os.path.basename(filename), ExtraArgs={'ACL': 'bucket-owner-full-control'})
        print("Splitted File uploaded to " + Key + os.path.basename(filename) + "\n")


if __name__ == "__main__":
    # print(dates)
    print(main_url)

    cc(main_url)
    print("[+] Total External links:", len(external_urls))
    print("[+] Total Internal links:", len(internal_urls))
    print("[+] Total:", len(external_urls) + len(internal_urls))

    df = pd.DataFrame(list(internal_urls), columns=["colummn"])
    print(df)
    # df.to_csv(result_file, index=False, header=False, compression='gzip')
    df.to_csv("result_file.gz", index=False, header=False, compression='gzip')
    # print(result_file)

    upload_to_s3()
